# ==============================================================================
# 脚本名称: compare_multi_folders_v2.py
# 核心功能: 
#   1. 交互式询问输入任意数量的文件夹路径（输入 'q' 结束）。
#   2. 递归扫描每个路径下所有 CSV，自动跳过名为 "all" 的文件夹。
#   3. 提取 CSV 第二列（被叫/客户号码）并自动处理多种编码。
#   4. 进行全交叉两两比对，输出重复数及各自的占比百分比。
#   5. 统计所有路径汇总去重后的唯一号码总数。
# ==============================================================================

import os
import pandas as pd
from itertools import combinations

def get_unique_phones(search_path):
    """递归获取唯一号码集合，并过滤掉 /all 文件夹"""
    all_numbers = set()
    total_files = 0
    encodings = ['utf-8-sig', 'gb18030', 'gbk', 'utf-8']
    
    # 遍历目录
    for root, dirs, files in os.walk(search_path):
        # --- 核心修改：过滤掉路径中包含 'all' 的文件夹 ---
        if 'all' in dirs:
            dirs.remove('all') # 移除后，os.walk 不会再进入该子文件夹
            
        for file in files:
            if file.lower().endswith('.csv'):
                total_files += 1
                file_path = os.path.join(root, file)
                
                for enc in encodings:
                    try:
                        # 读取第二列 (index 1)
                        df = pd.read_csv(file_path, usecols=[1], dtype=str, encoding=enc, on_bad_lines='skip')
                        nums = df.iloc[:, 0].dropna().str.strip()
                        all_numbers.update(nums.tolist())
                        break
                    except Exception:
                        continue
    return all_numbers, total_files

def compare_multi_folders():
    paths_data = []
    
    print("请输入要对比的文件夹路径（支持任意数量，输入 'q' 结束）：")
    while True:
        idx = len(paths_data) + 1
        raw_input = input(f"请输入路径 {idx} (或输入 'q' 结束): ").strip().strip('"')
        
        if raw_input.lower() == 'q':
            if len(paths_data) < 2:
                print("[提示] 至少需要输入两个路径才能进行分析。")
                continue
            break
        
        if os.path.exists(raw_input):
            print(f"正在扫描并过滤 'all' 文件夹: {raw_input} ...")
            phones, file_count = get_unique_phones(raw_input)
            paths_data.append({
                'name': os.path.basename(raw_input) if os.path.basename(raw_input) else raw_input,
                'path': raw_input,
                'phones': phones,
                'file_count': file_count
            })
            print(f"加载成功: 已提取 {len(phones)} 个唯一号码 (忽略 'all' 后共计 {file_count} 个 CSV 文件)")
        else:
            print(f"[错误] 路径无效: {raw_input}")

    # 2. 生成比对表格
    if not paths_data: return

    print("\n" + "="*85)
    print(f"{'对比分析 (A vs B)':<45} | {'重复数':<10} | {'A中占比':<10} | {'B中占比':<10}")
    print("-" * 85)

    # 两两组合对比
    for a, b in combinations(paths_data, 2):
        intersection = a['phones'].intersection(b['phones'])
        count_inter = len(intersection)
        
        pct_a = (count_inter / len(a['phones']) * 100) if len(a['phones']) > 0 else 0
        pct_b = (count_inter / len(b['phones']) * 100) if len(b['phones']) > 0 else 0
        
        label = f"{a['name']} vs {b['name']}"
        # 截断过长的文件夹名以防表格错位
        print(f"{label[:45]:<45} | {count_inter:<10} | {pct_a:>8.2f}% | {pct_b:>8.2f}%")

    # 3. 汇总统计
    all_combined = set().union(*(p['phones'] for p in paths_data))
    print("-" * 85)
    print(f"【全局汇总】所有路径合并去重后的唯一号码总数: {len(all_combined)}")
    print("="*85)

if __name__ == "__main__":
    compare_multi_folders()