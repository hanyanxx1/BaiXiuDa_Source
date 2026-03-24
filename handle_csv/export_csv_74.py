import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

def check_header_and_encoding(filepath):
    """参考 36 脚本：动态探测编码及表头"""
    encodings = ['utf-8-sig', 'gbk', 'utf-8']
    keywords = ['主叫', '被叫', 'caller', 'callee', 'e164', '号码']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                first_line = f.readline().lower()
                has_header = any(k in first_line for k in keywords)
                return has_header, enc
        except UnicodeDecodeError:
            continue
    return False, 'utf-8-sig'

def process_vos_data(input_folder, output_folder):
    start_time = time.time()
    curr_date = datetime.now().strftime("%m.%d")
    
    input_folder = input_folder.strip().strip('"')
    output_folder = output_folder.strip().strip('"')

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    all_data_list = []
    
    # 递归获取所有CSV
    csv_files = []
    for root, dirs, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    if not csv_files:
        print(f"错误：在路径 [{input_folder}] 下未找到CSV文件。")
        return

    print(f"\n--- 阶段 1: 开始读取并清洗数据 (共 {len(csv_files)} 个文件) ---")
    
    for filepath in csv_files:
        has_header, encoding = check_header_and_encoding(filepath)
        try:
            if has_header:
                df = pd.read_csv(filepath, encoding=encoding, dtype=str)
                # 清洗列名
                df.columns = [str(col).strip().lower() for col in df.columns]
                # 匹配列
                c_col = next((c for c in ['callere164', 'caller e164', '主叫', '主叫号码', 'caller'] if c in df.columns), None)
                e_col = next((c for c in ['calleee164', 'callee e164', '被叫', '被叫号码', 'callee'] if c in df.columns), None)
                
                if c_col and e_col:
                    temp_df = df[[c_col, e_col]].copy()
                else:
                    # 有表头但没对上，强制取1、2列
                    temp_df = df.iloc[:, [0, 1]].copy()
            else:
                # 无表头，强制取前两列
                df = pd.read_csv(filepath, encoding=encoding, dtype=str, header=None)
                temp_df = df.iloc[:, [0, 1]].copy()
            
            temp_df.columns = ['callere164', 'calleee164']
            temp_df = temp_df.dropna(subset=['calleee164'])
            # 被叫后11位清洗
            temp_df['calleee164'] = temp_df['calleee164'].str.strip().str[-11:]
            
            all_data_list.append(temp_df)
            print(f" [+] 已加载: {os.path.basename(filepath)} ({'有表头' if has_header else '无表头'})")
            
        except Exception as e:
            print(f" [!] 无法读取 {os.path.basename(filepath)}: {e}")

    if not all_data_list:
        print("\n[!!!] 任务终止：未提取到有效数据。")
        return

    all_df = pd.concat(all_data_list, ignore_index=True)

    print("\n--- 阶段 2: 执行全局去重 ---")
    before_len = len(all_df)
    all_df.drop_duplicates(subset=['calleee164'], inplace=True)
    print(f" 去重完成: {before_len} -> {len(all_df)} (剔除重复: {before_len - len(all_df)})")

    # 分组统计
    counts = all_df['callere164'].value_counts()
    large_groups = counts[counts > 50000].index.tolist()
    small_groups = counts[counts <= 50000].index.tolist()

    def export_batches(df_group, core_identifier):
        """核心导出函数"""
        # 乱序
        df_group = df_group.sample(frac=1).reset_index(drop=True)
        
        # 构造标准表头
        export_df = pd.DataFrame({
            '客户姓名': ["" ] * len(df_group),
            '客户号码': df_group['calleee164'],
            '地址': ["" ] * len(df_group),
            '购买套数': ["" ] * len(df_group),
            '签收电话': ["" ] * len(df_group),
            '备注': ["" ] * len(df_group)
        })

        batch_size = 50000
        num_batches = (len(export_df) + batch_size - 1) // batch_size

        # --- 优化点：参考 move_files 的文件夹命名逻辑 (核心标识-文件数) ---
        folder_name = f"{core_identifier}-{num_batches}"
        target_dir = os.path.join(output_folder, folder_name)
        
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        for i in range(num_batches):
            start = i * batch_size
            end = min((i + 1) * batch_size, len(export_df))
            batch_data = export_df.iloc[start:end]
            
            # 命名规则
            file_name = f"{i + 1}-{core_identifier}-0-{curr_date}.csv"
            save_path = os.path.join(target_dir, file_name)
            batch_data.to_csv(save_path, index=False, encoding='utf-8-sig', lineterminator='\r\n')
        
        print(f" √ 成功创建文件夹: {folder_name} (内含 {num_batches} 个文件，共 {len(export_df)} 条)")

    print("\n--- 阶段 3: 开始分组导出 ---")
    
    # 1. 大组独立导出
    for caller in large_groups:
        export_batches(all_df[all_df['callere164'] == caller], str(caller))

    # 2. 小组汇总为 AAAA 导出
    if small_groups:
        aaaa_df = all_df[all_df['callere164'].isin(small_groups)]
        if not aaaa_df.empty:
            export_batches(aaaa_df, 'AAAA')

    end_time = time.time()
    print(f"\n--- 任务全部完成! 总耗时: {end_time - start_time:.2f}秒 ---")
    input("\n按回车键退出程序...")

if __name__ == "__main__":
    print("=== 74 号码清洗分组工具 (参考 move_files 优化版) ===")
    in_dir = input("请输入【原始CSV文件夹】路径: ").strip()
    out_dir = input("请输入【结果存放文件夹】路径: ").strip()
    
    if in_dir and out_dir:
        process_vos_data(in_dir, out_dir)
    else:
        print("路径不能为空。")