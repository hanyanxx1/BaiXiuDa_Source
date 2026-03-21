#!/bin/bash

# --- 1. 数据库配置 (请根据实际情况修改密码) ---
DB_USER="hanyan"
DB_PASS="BAIXIUDA@@han138388"
DB_NAME="vos3000"
# 对应 SQL 要求的基础导出路径
BASE_EXPORT_PATH="/var/lib/mysql-files/39/"

# --- 2. 逻辑处理：确定目标表名 ---
if [ -n "$1" ]; then
    TABLE_NAME=$1
else
    # 计算昨天日期，格式为 YYYYMMDD
    YESTERDAY=$(date -d "yesterday" +%Y%m%d)
    TABLE_NAME="e_cdr_${YESTERDAY}"
fi

# --- 3. 构造不同的导出路径 ---
# 日期根路径 (用于 导出_处理_分组)
ROOT_PATH="${BASE_EXPORT_PATH}/${TABLE_NAME}/"
# all 子路径 (用于 导出.sql)
ALL_PATH="${ROOT_PATH}all/"

# 确保物理路径存在并修正权限
mkdir -p "$ALL_PATH"
chown -R mysql:mysql "$ROOT_PATH"

# --- 4. 调用 MySQL 存储过程 ---
echo "--------------------------------------------------"
echo "执行时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "目标表名: ${TABLE_NAME}"
echo "--------------------------------------------------"

# 步骤 A: 执行“导出.sql” -> 导出到 xxx/all/
echo "正在执行：原始数据分批导出 (至 ${ALL_PATH})..."
mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportCallData('${TABLE_NAME}', '${ALL_PATH}', 'holdtime <= 0');"

# 步骤 B: 执行“导出_处理_分组.sql” -> 导出到 xxx/
echo "正在执行：去重分组乱序处理导出 (至 ${ROOT_PATH})..."
mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} -e "CALL ExportDistinctGroupedCallData('${TABLE_NAME}', '${ROOT_PATH}', 'holdtime <= 0');"

echo "--------------------------------------------------"
echo "所有导出任务执行完毕。"