#!/bin/bash

# --- 1. 数据库配置 ---
DB_USER="root"
DB_PASS="My@Passwd219x"
DB_NAME="vos3000"
# 基础导出路径 (根据服务器修改为 74/203/247)
BASE_EXPORT_PATH="/var/lib/mysql-files/247/"

# --- 2. 核心导出函数 ---
do_export() {
    local TARGET_DATE=$1
    local TABLE_NAME="e_cdr_${TARGET_DATE}"
    local ROOT_PATH="${BASE_EXPORT_PATH}${TABLE_NAME}/"
    local ALL_PATH="${ROOT_PATH}all/"

    echo "--------------------------------------------------"
    echo "正在处理日期: ${TARGET_DATE} (表名: ${TABLE_NAME})"
    
    # 确保物理路径存在并修正权限
    mkdir -p "$ALL_PATH"
    chown -R mysql:mysql "$ROOT_PATH"

    # 执行“导出.sql” -> 导出到 xxx/all/
    echo "步骤 A：原始数据分批导出..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportCallData('${TABLE_NAME}', '${ALL_PATH}', 'holdtime <= 0');"

    # 执行“导出_处理_分组.sql” -> 导出到 xxx/
    echo "步骤 B：去重分组乱序导出..."
    mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportDistinctGroupedCallData('${TABLE_NAME}', '${ROOT_PATH}', 'holdtime <= 0');"
}

# --- 3. 参数逻辑判断 ---
if [ "$#" -eq 1 ]; then
    # 模式 1: 只传某一天 (例如: sh script.sh 20260321)
    do_export "$1"
    
elif [ "$#" -eq 2 ]; then
    # 模式 2: 传起始和结束日期 (例如: sh script.sh 20260301 20260313)
    START_DATE=$1
    END_DATE=$2
    CURRENT_DATE="$START_DATE"
    while [ "$CURRENT_DATE" -le "$END_DATE" ]; do
        do_export "$CURRENT_DATE"
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y%m%d)
    done
    
elif [ "$#" -eq 0 ]; then
    # 模式 3: 不传参数，默认昨天 (用于凌晨 2 点定时任务)
    YESTERDAY=$(date -d "yesterday" +%Y%m%d)
    do_export "$YESTERDAY"
    
else
    echo "使用错误！示例："
    echo "  跑某一天: sh $0 20260321"
    echo "  跑时间段: sh $0 20260301 20260313"
    echo "  跑昨天:   sh $0"
    exit 1
fi

echo "--------------------------------------------------"
echo "任务全部执行完毕。"