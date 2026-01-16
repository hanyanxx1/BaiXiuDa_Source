#!/bin/bash

# ==========================================================
# 🛑 配置区：
# ==========================================================
BASE_EXPORT_DIR="/var/lib/mysql-files/" # 根导出目录
# ==========================================================
# 配置区结束
# ==========================================================

# 1. 接收和验证输入参数
if [ "$#" -ne 2 ]; then
    echo "❌ 错误：请提供 START_DATE 和 END_DATE 作为参数。"
    echo "用法: $0 YYYY-MM-DD YYYY-MM-DD"
    echo "示例: $0 2025-11-01 2025-11-05"
    exit 1
fi

START_DATE="$1"
END_DATE="$2"

# 2. 日期转换和验证
# 尝试将输入日期转换为 Unix 时间戳，如果失败则表示日期格式不正确
current_date_ts=$(date -d "$START_DATE" +%s 2>/dev/null)
end_date_ts=$(date -d "$END_DATE" +%s 2>/dev/null)

if [ -z "$current_date_ts" ] || [ -z "$end_date_ts" ]; then
    echo "❌ 错误：日期格式不正确或无效。请使用 YYYY-MM-DD 格式。"
    exit 1
fi

if [ "$current_date_ts" -gt "$end_date_ts" ]; then
    echo "❌ 错误：起始日期不能晚于结束日期。"
    exit 1
fi

# 每天的秒数
ONE_DAY=$((60 * 60 * 24))

echo "📂 🚀 开始批量创建导出目录..."
echo "-------------------------------------"
echo "📅 任务范围: ${START_DATE} 到 ${END_DATE}"
echo "   基础路径: ${BASE_EXPORT_DIR}"
echo "-------------------------------------"

# 3. 循环创建目录
while [ "$current_date_ts" -le "$end_date_ts" ]; do
    
    # 格式化当前的日期字符串 (例如：20251101)
    DATE_STR=$(date -d "@$current_date_ts" +%Y%m%d)
    
    # 构造表名 (例如：e_cdr_20251101)
    TABLE_NAME="e_cdr_${DATE_STR}"
    
    # 构造完整的导出路径 (例如：/var/lib/mysql-files/e_cdr_20251101/)
    EXPORT_PATH="${BASE_EXPORT_DIR}${TABLE_NAME}/all"
    
    echo "✨ 正在创建目录: **${EXPORT_PATH}**"
    
    # 1. 检查并创建文件夹
    mkdir -p "${EXPORT_PATH}"
    
    # 2. 检查 mkdir 命令的退出状态 ($?)
    if [ $? -eq 0 ]; then
        echo "   ✅ 成功：目录创建或已存在。"
    else
        echo "   ❌ **失败！** 无法创建目录：${EXPORT_PATH}"
        echo "   🛑 任务终止。请检查文件系统权限或路径是否存在问题。"
        exit 1
    fi
    
    echo "--- 完成 ${DATE_STR} ---"
    
    # 移动到下一天
    current_date_ts=$((current_date_ts + ONE_DAY))
done

echo ""
echo "🎉🎉🎉 **所有目录创建任务 (${START_DATE} 到 ${END_DATE}) 已成功完成！** 🎉🎉🎉"
echo "-------------------------------------------------------------------------"

--- 使用方式： ./create_export_dirs_param.sh 2026-01-08 2026-01-31

--- 给脚本授权：
--- chmod +x create_export_dirs_param.sh
--- 给mysql写入权限：
--- chown -R mysql:mysql e_cdr_2026*
