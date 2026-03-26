#!/bin/bash

# --- 1. 数据库配置 (请根据实际情况修改密码) ---
DB_USER="hanyan"
DB_PASS="BAIXIUDA@@han138388"
DB_NAME="vos3000"
# 对应 SQL 要求的基础导出路径
BASE_EXPORT_PATH="/var/lib/mysql-files/005-DDDD-36.139.251.203-0秒-备用/"

# --- 2. 核心导出与归档函数 ---
do_export() {
    local TARGET_DATE=$1
    local TABLE_NAME="e_cdr_${TARGET_DATE}"
    local ROOT_PATH="${BASE_EXPORT_PATH}${TABLE_NAME}/"
    local ALL_PATH="${ROOT_PATH}all/"
    
    # [新增] 定义统计日志文件路径
    local SUMMARY_LOG="${ROOT_PATH}export_summary.log"

    echo "=================================================="
    echo "正在启动任务: ${TARGET_DATE} (执行时间: $(date '+%Y-%m-%d %H:%M:%S'))"
    echo "=================================================="
    
    # 确保路径存在并修正权限
    mkdir -p "$ALL_PATH"
    chown -R mysql:mysql "$ROOT_PATH"

    # [新增] 初始化日志文件记录开始时间
    echo "任务统计开始于: $(date '+%Y-%m-%d %H:%M:%S')" > "$SUMMARY_LOG"

    # A. 原始数据全导出
    echo " -> 步骤 A: 导出原始话单到 all/ 目录..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportCallData('${TABLE_NAME}', '${ALL_PATH}', 'holdtime <= 0');"
    
    # [新增] 统计 A 步骤导出的总行数 (合并读取 all/ 下所有 csv)
    TOTAL_RAW=$(find "$ALL_PATH" -maxdepth 1 -name "*.csv" -exec cat {} + 2>/dev/null | wc -l)
    echo "Step A (ExportCallData 原始导出) 总行数: $TOTAL_RAW" >> "$SUMMARY_LOG"

    # B. 去重、分组、乱序导出
    echo " -> 步骤 B: 执行去重分组导出到根目录..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportDistinctGroupedCallData('${TABLE_NAME}', '${ROOT_PATH}', 'holdtime <= 0');"
    
    # [新增] 统计 B 步骤处理后的总行数 (只查 ROOT_PATH 第一层，防误算 all/ 目录)
    TOTAL_DISTINCT=$(find "$ROOT_PATH" -maxdepth 1 -name "*.csv" -exec cat {} + 2>/dev/null | wc -l)
    echo "Step B (ExportDistinctGroupedCallData 处理后) 总行数: $TOTAL_DISTINCT" >> "$SUMMARY_LOG"
    
    # [新增] 计算过滤掉的差值 (纯 Shell 底层运算)
    if [ -n "$TOTAL_RAW" ] && [ -n "$TOTAL_DISTINCT" ]; then
        FILTERED_OUT=$((TOTAL_RAW - TOTAL_DISTINCT))
        echo "共过滤/去重掉行数: $FILTERED_OUT" >> "$SUMMARY_LOG"
    fi

    # C. 归档整理 (实现标识-总数 结构)
    echo " -> 步骤 C: 正在执行结果归档整理..."
    cd "${ROOT_PATH}" || return

    # 声明关联数组用于统计
    declare -A file_counts
    
    # 第一次遍历：预统计
    for file in *.csv; do
        [ -e "$file" ] || continue
        CORE_NAME=$(echo "$file" | cut -d'-' -f2)
        if [ -n "$CORE_NAME" ]; then
            ((file_counts["$CORE_NAME"]++))
        fi
    done

    # 第二次遍历：移动归档
    for CORE_NAME in "${!file_counts[@]}"; do
        COUNT=${file_counts["$CORE_NAME"]}
        FOLDER_NAME="${CORE_NAME}-${COUNT}"
        
        echo "    [+] 归档分组: ${FOLDER_NAME} (含 ${COUNT} 个文件)"
        mkdir -p "${FOLDER_NAME}"
        
        # 匹配导出前缀并移动
        mv *-"${CORE_NAME}"-*.csv "${FOLDER_NAME}/" 2>/dev/null
    done

    # 修正权限并返回
    chown -R mysql:mysql "${ROOT_PATH}"
    echo "√ 日期 ${TARGET_DATE} 处理全部完成！"
    echo ""
}

# --- 3. 参数处理与逻辑判断 (修正报错的核心部分) ---
if [ "$#" -eq 0 ]; then
    # 模式 0: 不带参数 -> 默认昨天 (用于定时任务)
    YESTERDAY=$(date -d "yesterday" +%Y%m%d)
    echo "检测到无参数，自动执行昨日任务: ${YESTERDAY}"
    do_export "$YESTERDAY"

elif [ "$#" -eq 1 ]; then
    # 模式 1: 单日模式 (sh script.sh 20260321)
    do_export "$1"

elif [ "$#" -eq 2 ]; then
    # 模式 2: 时间段模式 (sh script.sh 20260301 20260305)
    START_DATE=$1
    END_DATE=$2
    
    echo "检测到时间段模式: 从 ${START_DATE} 到 ${END_DATE}"
    
    CURRENT_DATE=$START_DATE
    while [ "$CURRENT_DATE" -le "$END_DATE" ]; do
        # 执行导出
        do_export "$CURRENT_DATE"
        
        # 日期递增逻辑
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