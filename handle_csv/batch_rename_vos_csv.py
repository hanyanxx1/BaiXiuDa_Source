import os
import re

def batch_rename_csv():
    print("=" * 50)
    print("   VOS CSV 历史文件批量重命名工具 (多路径增强版)")
    print("=" * 50)
    
    # 1. 交互式收集任意多个路径
    base_paths = []
    print("👉 请输入要处理的根目录路径 (支持多个，每次输入一个后按回车，直接按回车结束输入):")
    while True:
        path_input = input(f"   -> 第 {len(base_paths) + 1} 个路径: ").strip()
        
        # 如果用户直接按回车（输入为空），则结束路径收集
        if not path_input:
            if len(base_paths) == 0:
                print("\n❌ 您没有输入任何路径，已退出。")
                return
            break
            
        # 实时校验路径是否存在
        if os.path.exists(path_input):
            base_paths.append(path_input)
        else:
            print(f"   ⚠️ 警告：该路径不存在，请检查后重新输入！ '{path_input}'")

    # 2. 获取全局唯一服务标识
    server_name = input("\n👉 请输入当前服务标识 (例如 Vos36 或 36): ").strip()
    if not server_name:
        print("\n❌ 错误：服务标识不能为空！")
        return

    # 全局统计变量
    total_rename_count = 0
    total_skip_count = 0
    total_ignore_all_count = 0

    print("\n" + "=" * 50)
    print(f"🔧 目标服务名: {server_name}")
    print("=" * 50)

    # 3. 遍历所有收集到的路径
    for base_path in base_paths:
        print(f"\n🚀 正在处理路径: {base_path}")
        
        # 递归遍历当前目录
        for root, dirs, files in os.walk(base_path):
            # 排除并跳过名为 'all' 的文件夹
            if 'all' in dirs:
                dirs.remove('all')
                total_ignore_all_count += 1

            # 智能提取表名后6位
            # 利用正则匹配路径中的 e_cdr_YYYYMMDD，并提取后6位 (MMDD 或 YYMMDD)
            # 例如: 从 ".../e_cdr_20251126/" 中提取出 "251126"
            match = re.search(r'e_cdr_\d{2}(\d{6})', root)
            if not match:
                # 如果当前路径层级里没有 e_cdr_ 表名特征，就跳过当前层级文件的重命名
                continue
                
            table_suffix = match.group(1)
            target_replacement = f"-{server_name}-{table_suffix}-"

            # 遍历并修改文件
            for file in files:
                if file.endswith('.csv'):
                    # 严格匹配文件名中包含 '-0-' 的文件
                    if "-0-" in file:
                        old_path = os.path.join(root, file)
                        # 将第一次出现的 '-0-' 替换为 '-服务名-表名后6位-'
                        new_file_name = file.replace("-0-", target_replacement, 1)
                        new_path = os.path.join(root, new_file_name)

                        try:
                            os.rename(old_path, new_path)
                            print(f"✅ 成功: {file}  ->  {new_file_name}")
                            total_rename_count += 1
                        except Exception as e:
                            print(f"❌ 失败: {file}, 错误原因: {e}")
                    else:
                        total_skip_count += 1

    # 4. 打印全局最终报告
    print("\n" + "=" * 50)
    print("🎉 批量重命名任务全部完成！")
    print(f"📊 汇总统计数据：")
    print(f"   - 扫描目录总数: {len(base_paths)} 个")
    print(f"   - 成功重命名文件: {total_rename_count} 个")
    print(f"   - 跳过无需修改文件: {total_skip_count} 个 (没有包含-0-)")
    print(f"   - 成功避开 all 文件夹: {total_ignore_all_count} 个")
    print("=" * 50)

if __name__ == "__main__":
    batch_rename_csv()


# ==============================================================================
# 【调用示例与运行说明】
# ==============================================================================
#
# 1. 将上述代码保存到服务器上，例如放在 /root/scripts/ 目录下：
#    vi /root/scripts/batch_rename_vos_multi_path.py
#    (粘贴代码并保存)
#
# 2. 运行脚本：
#    python3 /root/scripts/batch_rename_vos_multi_path.py
#
# 3. 运行后的交互过程示例：
#    👉 请输入要处理的根目录路径 (支持多个，每次输入一个后按回车，直接按回车结束输入):
#       -> 第 1 个路径: /var/lib/mysql-files/001-AAAA-112.25.240.74-0秒/
#       -> 第 2 个路径: /var/lib/mysql-files/002-BBBB-112.25.240.75-0秒/
#       -> 第 3 个路径: [这里直接敲击回车，结束路径输入]
#    
#    👉 请输入当前服务标识 (例如 Vos36 或 36): Vos74
#
#    ==================================================
#    🔧 目标服务名: Vos74
#    ==================================================
#    
#    🚀 正在处理路径: /var/lib/mysql-files/001-AAAA-112.25.240.74-0秒/
#    ✅ 成功: 1-3791-0-12.12.csv  ->  1-3791-Vos74-251126-12.12.csv
#    ✅ 成功: 1-AAAA-0-12.12.csv  ->  1-AAAA-Vos74-251126-12.12.csv
#
#    🚀 正在处理路径: /var/lib/mysql-files/002-BBBB-112.25.240.75-0秒/
#    ✅ 成功: 1-BBBB-0-12.12.csv  ->  1-BBBB-Vos74-251126-12.12.csv
#    ...
#    🎉 批量重命名任务全部完成！
# ==============================================================================