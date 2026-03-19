# 该脚本用于批量处理指定目录下的VOS数据文件（CSV格式），
# 提取主叫、被叫、通话时长、起始时间、终止时间这5个字段，
# 并将处理后的结果保存到新的CSV文件中。
# 每个文件处理完成后，会在控制台输出处理时间和处理的记录数。
import os
import glob
import time
from datetime import datetime
import pandas as pd
import numpy as np

def check_header_and_encoding(filepath):
    """动态探测文件的编码格式以及是否包含表头"""
    encodings = ['utf-8-sig', 'gbk', 'utf-8']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                first_line = f.readline()
                # 只要第一行包含这些关键字，就认定有表头
                has_header = any(k in first_line for k in ['主叫', '被叫', 'callere164', 'calleee164'])
                return has_header, enc
        except UnicodeDecodeError:
            continue
    return False, 'utf-8-sig'

def process_vos_data(input_folder, output_folder, chunk_size=1000000):
    start_time = time.time()
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    # 获取所有CSV并排序，确保 part1排在最前面被首先读取
    all_files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not all_files:
        print(f"指定路径 {input_folder} 下未找到CSV文件。")
        return

    print(f"开始火力全开处理，共发现 {len(all_files)} 个CSV文件...")

    # --- 1. 获取标准表头 (以第一个文件为准) ---
    first_file = all_files[0]
    _, first_enc = check_header_and_encoding(first_file)
    sample_df = pd.read_csv(first_file, nrows=0, encoding=first_enc)
    actual_cols = sample_df.columns.tolist()
    
    target_fields = {
        'callere164': ['主叫号码', 'callere164', '主叫'],
        'calleee164': ['被叫号码', 'calleee164', '被叫'],
        'holdtime': ['通话时长', 'holdtime', '时长'],
        'starttime': ['起始时间', 'starttime', '开始时间'],
        'stoptime': ['终止时间', 'stoptime', '结束时间']
    }
    
    current_usecols = []
    rename_map = {}
    
    for std_col, possible_names in target_fields.items():
        for name in possible_names:
            if name in actual_cols:
                current_usecols.append(name)
                rename_map[name] = std_col
                break

    # --- 2. 核心读取与清洗 (无缝兼容无表头文件) ---
    valid_data_chunks = []
    
    for filename in all_files:
        print(f"  正在极速读取文件: {os.path.basename(filename)}")
        has_header, file_enc = check_header_and_encoding(filename)
        
        # 核心逻辑：如果没表头，就强行把第一份文件的表头套给它
        if has_header:
            chunks = pd.read_csv(filename, usecols=current_usecols, encoding=file_enc, chunksize=chunk_size)
        else:
            chunks = pd.read_csv(filename, header=None, names=actual_cols, usecols=current_usecols, encoding=file_enc, chunksize=chunk_size)

        for chunk in chunks:
            chunk = chunk.rename(columns=rename_map)
            
            # 时间推导补全逻辑
            if 'holdtime' not in chunk.columns and 'starttime' in chunk.columns and 'stoptime' in chunk.columns:
                chunk['holdtime'] = np.where(chunk['starttime'] == chunk['stoptime'], 0, 1)
            if 'holdtime' not in chunk.columns:
                 chunk['holdtime'] = 0

            # 逻辑 1: whereCondition 过滤
            mask = (
                (chunk['holdtime'] <= 0) & 
                (~chunk['calleee164'].astype(str).str.contains("QIANHAO|WuRaoHaoMa|DONGTAIDIFANG|/|\\?|,|#|\\\\|\\*|-", na=False, regex=True))
            )
            df_filtered = chunk[mask].copy()
            
            if df_filtered.empty:
                continue

            # 逻辑 2: 号码截取处理
            callee_str = df_filtered['calleee164'].astype(str)
            prefix_pass = callee_str.str.startswith('PASS')
            prefix_hmd = callee_str.str.startswith('HMD')
            
            df_filtered['processed_calleee164'] = np.where(
                prefix_pass | prefix_hmd, 
                callee_str.str[8:], 
                callee_str.str[4:]
            )
            
            df_filtered['processed_calleee164'] = pd.to_numeric(df_filtered['processed_calleee164'], errors='coerce')
            df_filtered = df_filtered.dropna(subset=['processed_calleee164'])
            
            # 逻辑 4: 10-11位长度过滤
            str_lens = df_filtered['processed_calleee164'].astype(np.int64).astype(str).str.len()
            df_filtered = df_filtered[(str_lens >= 10) & (str_lens <= 11)]
            
            valid_data_chunks.append(df_filtered[['callere164', 'processed_calleee164']])

    if not valid_data_chunks:
        print("处理完毕：没有符合要求的数据。")
        return

    # --- 3. 合并与全局去重 ---
    print("正在合并全量数据并进行全局去重...")
    all_valid_df = pd.concat(valid_data_chunks, ignore_index=True)
    all_valid_df = all_valid_df.drop_duplicates(subset=['processed_calleee164'])
    
    total_records = len(all_valid_df)
    print(f"-> 去重后总有效被叫号码记录数: {total_records}")

    # --- 4. 分组与文件夹结构化导出 ---
    curr_date = datetime.now().strftime('%m.%d')
    group_counts = all_valid_df['callere164'].value_counts()
    
    large_groups = group_counts[group_counts >= 50000].index.tolist()
    small_groups = group_counts[group_counts < 50000].index.tolist()

    def export_batches(df_to_export, file_prefix):
        """执行乱序并分批导出，自动创建专属文件夹"""
        df_to_export = df_to_export.sample(frac=1).reset_index(drop=True)
        
        export_df = pd.DataFrame({
            '客户姓名': '',
            '客户号码': df_to_export['processed_calleee164'].astype(np.int64).astype(str),
            '地址': '',
            '购买套数': '',
            '签收电话': '',
            '备注': ''
        })
        
        batch_size = 50000
        total_len = len(export_df)
        num_batches = int(np.ceil(total_len / batch_size))
        if num_batches == 0: num_batches = 1
        
        # --- 核心改进：创建跟左图一模一样的文件夹 (例如: naifen-16) ---
        folder_name = f"{file_prefix}-{num_batches}"
        target_dir = os.path.join(output_folder, folder_name)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_len)
            batch_df = export_df.iloc[start_idx:end_idx]
            
            file_name = f"{i + 1}-{file_prefix}-0-{curr_date}.csv"
            export_file_path = os.path.join(target_dir, file_name)
            
            batch_df.to_csv(export_file_path, index=False, encoding='utf-8-sig', lineterminator='\r\n')
            
        print(f"    √ 生成文件夹 [{folder_name}] 成功，内含 {num_batches} 个文件，共 {total_len} 条。")

    print("\n--- 开始按主叫分组导出 ---")
    for caller in large_groups:
        group_data = all_valid_df[all_valid_df['callere164'] == caller]
        export_batches(group_data, caller)
        
    if small_groups:
        small_group_data = all_valid_df[all_valid_df['callere164'].isin(small_groups)]
        if not small_group_data.empty:
             export_batches(small_group_data, 'BBBB') 

    end_time = time.time()
    print("=" * 50)
    print(f"🎉 全部处理完成！耗时: {end_time - start_time:.2f} 秒")
    print(f"最终有效总记录数: {total_records}")
    print("=" * 50)

if __name__ == '__main__':
    input_dir = input("请输入CSV源文件所在的文件夹路径 (例如 .../all): ").strip().strip('\'"')
    output_dir = input("请输入期望输出的主文件夹路径 (例如 .../e_cdr_20260301): ").strip().strip('\'"')
    
    if input_dir and output_dir:
        process_vos_data(input_dir, output_dir)