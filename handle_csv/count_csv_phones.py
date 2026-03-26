# ==============================================================================
# 脚本名称: count_csv_phones.py
# 核心功能: 
#   1. 递归扫描指定路径下所有 CSV 文件的第二列（客户号码）。
#   2. 自动兼容多种编码 (utf-8-sig, gb18030, gbk, utf-8)。
#   3. 统计原始总行数、去重后的唯一号码数、重复项总数。
#   4. 任务结束后，询问用户是否需要重命名扫描的根目录文件夹。
# ==============================================================================

import os
import pandas as pd

def count_csv_phones():
    # 1. 交互式输入路径
    input_path = input("请输入要扫描的文件夹路径: ").strip().strip('"')
    
    if not os.path.exists(input_path):
        print(f"错误: 路径 '{input_path}' 不存在")
        return

    all_numbers_list = []
    total_files_found = 0
    
    print(f"\n开始递归扫描路径: {input_path}")
    print("-" * 60)
    
    # 2. 递归遍历并提取数据
    for root, dirs, files in os.walk(input_path):
        for file in files:
            if file.lower().endswith('.csv'):
                total_files_found += 1
                file_path = os.path.join(root, file)
                
                df = None
                # 尝试多编码读取
                for encoding in ['utf-8-sig', 'gb18030', 'gbk', 'utf-8']:
                    try:
                        # 读取第二列（索引为1），保持字符串类型以防号码变形
                        df = pd.read_csv(file_path, usecols=[1], dtype=str, encoding=encoding, on_bad_lines='skip')
                        break 
                    except Exception:
                        continue
                
                if df is not None:
                    col_data = df.iloc[:, 0].dropna().str.strip() # 去空并清理前后空格
                    all_numbers_list.append(col_data)
                    # 实时反馈进度
                    print(f"已处理: {file:<30} | 提取行数: {len(col_data)}")
                else:
                    print(f"跳过文件 {file}: 无法识别编码或列格式不正确")

    if not all_numbers_list:
        print(f"\n未在路径下提取到任何有效数据。共检查了 {total_files_found} 个 CSV 文件。")
        return

    # 3. 数据汇总与去重统计
    print("\n" + "="*60)
    print("                数据汇总统计结果")
    print("="*60)
    
    full_series = pd.concat(all_numbers_list, ignore_index=True)
    total_raw = len(full_series)
    
    # 去重
    unique_series = full_series.drop_duplicates()
    total_unique = len(unique_series)
    
    print(f"检索 CSV 文件总数: {total_files_found}")
    print(f"原始记录总行数: {total_raw}")
    print(f"去重后唯一号码数: {total_unique}")
    print(f"发现重复记录条数: {total_raw - total_unique}")
    if total_raw > 0:
        print(f"数据重复率: {((total_raw - total_unique) / total_raw * 100):.2f}%")
    print("="*60)

    # 4. 询问是否修改文件夹名称
    rename_opt = input("\n统计结束。是否需要重命名该扫描文件夹？(y/n): ").strip().lower()
    if rename_opt == 'y':
        suffix = input("请输入想添加的命名后缀 (例如: _已去重统计): ").strip()
        try:
            new_path = input_path.rstrip('\\/') + suffix
            os.rename(input_path, new_path)
            print(f"重命名成功！\n原路径: {input_path}\n新路径: {new_path}")
        except Exception as e:
            print(f"重命名失败: {e}")

if __name__ == "__main__":
    count_csv_phones()