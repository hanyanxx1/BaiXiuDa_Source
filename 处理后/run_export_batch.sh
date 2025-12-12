#!/bin/bash

# ==========================================================
# 🛑 配置区：已根据用户要求更新
# ==========================================================
DB_USER="root"                 # 数据库用户名
DB_PASS="My@Passwd219x"        # 数据库密码 (已更新为您提供的值)
DB_NAME="vos3000"              # 数据库名称
BASE_EXPORT_DIR="/var/lib/mysql-files/" # 根导出目录
WHERE_CONDITION="holdtime <= 0"    # 传入存储过程的WHERE条件

# 日期范围：从 2025-11-01 到 2025-11-05
START_DATE="2025-11-01"
END_DATE="2025-11-05"
# ==========================================================
# 配置区结束
# ==========================================================

# 检查 MySQL 密码是否为空
if [ -z "$DB_PASS" ]; then
    echo "❌ 错误：DB_PASS 变量为空，请设置正确的数据库密码。"
    exit 1
fi

# 使用日期序列生成器 (seq) 循环遍历日期
current_date_ts=$(date -d "$START_DATE" +%s)
end_date_ts=$(date -d "$END_DATE" +%s)
# 每天的秒数
ONE_DAY=$((60 * 60 * 24))

echo "🚀 开始执行批量数据导出任务..."
echo "-------------------------------------"
echo "👤 数据库用户: ${DB_USER}"
echo "📅 任务范围: ${START_DATE} 到 ${END_DATE}"
echo "-------------------------------------"

while [ "$current_date_ts" -le "$end_date_ts" ]; do
    
    # 格式化当前的日期字符串 (例如：20251101)
    DATE_STR=$(date -d "@$current_date_ts" +%Y%m%d)
    
    # 构造表名 (例如：e_cdr_20251101)
    TABLE_NAME="e_cdr_${DATE_STR}"
    
    # 构造完整的导出路径 (例如：/var/lib/mysql-files/e_cdr_20251101/)
    EXPORT_PATH="${BASE_EXPORT_DIR}${TABLE_NAME}/"
    
    echo "✨ 正在处理表: **${TABLE_NAME}**"
    echo "   导出路径: ${EXPORT_PATH}"
    
    # 1. 检查并创建文件夹
    mkdir -p "${EXPORT_PATH}"
    
    # 检查 mkdir 命令的退出状态
    if [ $? -ne 0 ]; then
        echo "   ❌ 失败！无法创建目录：${EXPORT_PATH}"
        echo "   🛑 任务终止。"
        exit 1
    fi
    echo "   ✅ 目录创建或已存在。"

    # 2. 执行存储过程
    # 使用 heredoc 方式安全地传递 SQL 命令，并捕获错误
    # 注意：使用 -e 选项执行命令
    mysql -u"${DB_USER}" -p"${DB_PASS}" "${DB_NAME}" -e "
        CALL ExportDistinctGroupedCallData('${TABLE_NAME}', '${EXPORT_PATH}', '${WHERE_CONDITION}');
    "

    # 3. 检查 MySQL 命令的退出状态
    if [ $? -eq 0 ]; then
        echo "   ✅ **成功！** ${TABLE_NAME} 数据已处理并导出。"
    else
        # 打印详细错误信息
        echo "   ❌ **失败！** 执行 ${TABLE_NAME} 的存储过程时出错。退出码: $?"
        echo "   🛑 任务终止。请检查 MySQL 日志或命令输出获取具体错误。"
        exit 1
    fi
    
    echo "--- 完成 ${DATE_STR} ---"
    
    # 移动到下一天
    current_date_ts=$((current_date_ts + ONE_DAY))
done

echo ""
echo "🎉🎉🎉 **所有批量导出任务 (${START_DATE} 到 ${END_DATE}) 已成功完成！** 🎉🎉🎉"
echo "-------------------------------------------------------------------------"