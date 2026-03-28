#!/bin/bash

# --- 1. 数据库配置 (请根据实际情况修改密码) ---
DB_USER="hanyan"
DB_PASS="BAIXIUDA@@han138388"
DB_NAME="vos3000"
# 对应 SQL 要求的基础导出路径
BASE_EXPORT_PATH="/var/lib/mysql-files/005-DDDD-36.139.251.203-0秒-备用/"
BASE_100S_PATH="/var/lib/mysql-files/005-DDDD-36.139.251.203-100秒-备用/"


# --- 2. 核心导出与归档函数 ---
do_export() {
    local TARGET_DATE=$1
    local TABLE_NAME="e_cdr_${TARGET_DATE}"
    
    # 0秒路径逻辑
    local ROOT_PATH="${BASE_EXPORT_PATH}${TABLE_NAME}/"
    local ALL_PATH="${ROOT_PATH}all/"
    
    # 定义 0秒 统计日志文件路径
    local SUMMARY_LOG="${ROOT_PATH}export_summary.log"

    echo "=================================================="
    echo "正在启动任务: ${TARGET_DATE} (执行时间: $(date '+%Y-%m-%d %H:%M:%S'))"
    echo "=================================================="
    
    # 确保路径存在并修正权限
    mkdir -p "$ALL_PATH"
    chown -R mysql:mysql "$BASE_EXPORT_PATH"
    chown -R mysql:mysql "$BASE_100S_PATH"

    # 初始化日志文件记录开始时间
    echo "任务统计开始于: $(date '+%Y-%m-%d %H:%M:%S')" > "$SUMMARY_LOG"

    # A. 原始数据全导出 (0秒)
    echo " -> 步骤 A: 导出 0秒 原始话单到 all/ 目录..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportCallData('${TABLE_NAME}', '${ALL_PATH}', 'holdtime <= 0');"

    # 统计 A 步骤导出的总行数
    TOTAL_RAW=$(find "$ALL_PATH" -maxdepth 1 -name "*.csv" -exec cat {} + 2>/dev/null | wc -l)
    echo "Step A (0秒原始导出) 总行数: $TOTAL_RAW" >> "$SUMMARY_LOG"

    # B. 去重、分组、乱序导出 (0秒)
    echo " -> 步骤 B: 执行去重分组导出到根目录..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportDistinctGroupedCallData('${TABLE_NAME}', '${ROOT_PATH}', 'holdtime <= 0');"
    
    # 统计 B 步骤处理后的总行数
    TOTAL_DISTINCT=$(find "$ROOT_PATH" -maxdepth 1 -name "*.csv" -exec cat {} + 2>/dev/null | wc -l)
    echo "Step B (0秒去重处理后) 总行数: $TOTAL_DISTINCT" >> "$SUMMARY_LOG"
    
    # 计算过滤掉的差值
    if [ -n "$TOTAL_RAW" ] && [ -n "$TOTAL_DISTINCT" ]; then
        FILTERED_OUT=$((TOTAL_RAW - TOTAL_DISTINCT))
        echo "0秒业务共过滤/去重掉行数: $FILTERED_OUT" >> "$SUMMARY_LOG"
    fi

    # C. 归档整理 (0秒业务)
    echo " -> 步骤 C: 正在执行结果归档整理..."
    cd "${ROOT_PATH}" || return

    declare -A file_counts
    for file in *.csv; do
        [ -e "$file" ] || continue
        CORE_NAME=$(echo "$file" | cut -d'-' -f2)
        if [ -n "$CORE_NAME" ]; then
            ((file_counts["$CORE_NAME"]++))
        fi
    done

    for CORE_NAME in "${!file_counts[@]}"; do
        COUNT=${file_counts["$CORE_NAME"]}
        FOLDER_NAME="${CORE_NAME}-${COUNT}"
        echo "    [+] 归档分组: ${FOLDER_NAME} (含 ${COUNT} 个文件)"
        mkdir -p "${FOLDER_NAME}"
        mv *-"${CORE_NAME}"-*.csv "${FOLDER_NAME}/" 2>/dev/null
    done

    # D. [新增逻辑] 100秒通话时长归档 - 扁平化导出到根目录
    echo " -> 步骤 D: 导出 100秒 原始话单到 100秒 专用目录 (根目录直接存放)..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportCallData('${TABLE_NAME}', '${BASE_100S_PATH}', 'holdtime >= 100');"
    
    # 统计 100秒 导出的总行数并记录到 0秒 的日志中
    TOTAL_100S=$(find "$BASE_100S_PATH" -maxdepth 1 -name "${TABLE_NAME}_part*.csv" -exec cat {} + 2>/dev/null | wc -l)
    echo "Step D (100秒归档导出 - ${TABLE_NAME}) 总行数: $TOTAL_100S" >> "$SUMMARY_LOG"

    # 修正权限并返回
    chown -R mysql:mysql "${BASE_EXPORT_PATH}"
    chown -R mysql:mysql "${BASE_100S_PATH}"
    echo "√ 日期 ${TARGET_DATE} 处理全部完成！"
    echo ""
}

# --- 3. 参数处理与逻辑判断 (完整保留交互提示逻辑) ---
if [ "$#" -eq 0 ]; then
    YESTERDAY=$(date -d "yesterday" +%Y%m%d)
    echo "检测到无参数，自动执行昨日任务: ${YESTERDAY}"
    do_export "$YESTERDAY"

elif [ "$#" -eq 1 ]; then
    do_export "$1"

elif [ "$#" -eq 2 ]; then
    START_DATE=$1
    END_DATE=$2
    echo "检测到时间段模式: 从 ${START_DATE} 到 ${END_DATE}"
    CURRENT_DATE=$START_DATE
    while [ "$CURRENT_DATE" -le "$END_DATE" ]; do
        do_export "$CURRENT_DATE"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            CURRENT_DATE=$(date -j -v+1d -f "%Y%m%d" "$CURRENT_DATE" +%Y%m%d)
        else
            CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y%m%d)
        fi
    done
    echo "=== 所有日期段任务已处理完毕 ==="

else
    echo "使用错误！"
    echo "用法 1 (定时任务): sh $0"
    echo "用法 2 (单日补数): sh $0 20260321"
    echo "用法 3 (时间段):   sh $0 20260301 20260305"
    exit 1
fi