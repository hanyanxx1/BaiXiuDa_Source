# ==============================================================================
# 脚本名称: export_csv_36_multi_final.py
# 核心逻辑 (36 业务专用):
#   1. 支持输入多个 all 文件夹路径进行跨日期汇总处理。
#   2. 自动识别路径中的日期并创建如 "e_cdr_20260303-20260305" 的目录。
#   3. 兼容无表头文件：若无表头，自动按第1列(主叫)、第2列(被叫)提取。
#   4. 保留 36 原始逻辑：PASS/HMD 截取、holdtime 过滤及特殊字符清洗。
#   5. 分批优化：最后一个分包若小于 20,000 条，则合并至前一个文件。
#   6. 自动刷新：导出完成后调用 WPS/Excel 接口执行静默保存刷新。
# ==============================================================================

import os
import time
import pandas as pd
import numpy as np
import win32com.client as win32
import re
from datetime import datetime

def refresh_csv_via_software(folder_path):
    """通过软件接口自动保存刷新文件格式"""
    print("\n" + "=" * 50)
    print("阶段 4: 开始执行文件格式刷新 (WPS/Excel 自动保存)")
    print("=" * 50)
    
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    if not csv_files: return

    try:
        try:
            app = win32.gencache.EnsureDispatch('Et.Application')
        except:
            app = win32.gencache.EnsureDispatch('Excel.Application')
        
        app.Visible = False 
        app.DisplayAlerts = False 

        for index, file_path in enumerate(csv_files, 1):
            try:
                abs_path = os.path.abspath(file_path)
                wb = app.Workbooks.Open(abs_path)
                wb.Save()
                wb.Close()
                print(f" [{index}/{len(csv_files)}] 格式已刷新: {os.path.basename(file_path)}")
            except Exception as e:
                print(f" [{index}] 刷新失败: {e}")
        app.Quit()
    except Exception as e:
        print(f"无法启动刷新程序: {e}")

def check_header_and_encoding(filepath):
    """探测文件的编码格式以及是否包含表头"""
    encodings = ['utf-8-sig', 'gbk', 'utf-8']
    keywords = ['主叫', '被叫', 'callere164', 'calleee164', 'holdtime']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                first_line = f.readline().lower()
                has_header = any(k in first_line for k in keywords)
                return has_header, enc
        except Exception: continue
    return False, 'utf-8-sig'

def export_batches(df_to_export, file_prefix, output_folder, curr_date):
    """分批次导出，逻辑优化：末尾不足 20,000 则合并"""
    df_to_export = df_to_export.sample(frac=1).reset_index(drop=True)
    
    export_df = pd.DataFrame({
        '客户姓名': '',
        '客户号码': df_to_export['processed_calleee164'].astype(np.int64).astype(str),
        '地址': '',
        '购买套数': '',
        '签收电话': '',
        '备注': ''
    })
    
    total_len = len(export_df)
    batch_size = 50000
    min_last_batch = 20000 
    
    initial_batches = total_len // batch_size
    remainder = total_len % batch_size
    batch_ranges = []
    
    if initial_batches == 0:
        batch_ranges.append((0, total_len))
    elif remainder < min_last_batch and remainder > 0:
        # 最后一包太小，合并到倒数第一包
        for i in range(initial_batches - 1):
            batch_ranges.append((i * batch_size, (i + 1) * batch_size))
        batch_ranges.append(((initial_batches - 1) * batch_size, total_len))
    else:
        for i in range(initial_batches):
            batch_ranges.append((i * batch_size, (i + 1) * batch_size))
        if remainder > 0:
            batch_ranges.append((initial_batches * batch_size, total_len))

    num_files = len(batch_ranges)
    folder_name = f"{file_prefix}-{num_files}"
    target_dir = os.path.join(output_folder, folder_name)
    if not os.path.exists(target_dir): os.makedirs(target_dir)
    
    for i, (start, end) in enumerate(batch_ranges):
        file_name = f"{i + 1}-{file_prefix}-0-{curr_date}.csv"
        export_file_path = os.path.join(target_dir, file_name)
        export_df.iloc[start:end].to_csv(export_file_path, index=False, encoding='utf-8-sig', lineterminator='\r\n')
            
    print(f"    √ 生成文件夹 [{folder_name}] 成功，包含 {num_files} 个文件。")

def process_multi_paths_36():
    input_paths = []
    print("=== 36 脚本汇总处理工具 (最终完整版) ===")
    print("请输入 all 文件夹的完整路径 (输入 'q' 结束输入):")
    
    while True:
        p = input(f"请输入路径 {len(input_paths)+1}: ").strip().strip('"')
        if p.lower() == 'q': break
        if os.path.exists(p): input_paths.append(p)
        else: print(" [!] 路径不存在")

    if not input_paths: return

    # 1. 自动计算输出文件夹日期范围
    dates = re.findall(r'e_cdr_(20\d{6})', " ".join(input_paths))
    if dates:
        dates.sort()
        folder_name = f"e_cdr_{dates[0]}-{dates[-1]}"
    else:
        folder_name = f"汇总_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    parent_dir = os.path.dirname(os.path.dirname(os.path.normpath(input_paths[0])))
    output_folder = os.path.join(parent_dir, folder_name)
    if not os.path.exists(output_folder): os.makedirs(output_folder)

    target_fields = {
        'callere164': ['主叫号码', 'callere164', '主叫'],
        'calleee164': ['被叫号码', 'calleee164', '被叫'],
        'holdtime': ['通话时长', 'holdtime', '时长'],
        'starttime': ['起始时间', 'starttime', '开始时间'],
        'stoptime': ['终止时间', 'stoptime', '结束时间']
    }

    valid_data_chunks = []
    print(f"\n--- 阶段 1: 汇总读取并执行 36 逻辑处理 ---")

    for path in input_paths:
        files = [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith('.csv')]
        for filename in files:
            has_header, file_enc = check_header_and_encoding(filename)
            try:
                if has_header:
                    df_sample = pd.read_csv(filename, nrows=0, encoding=file_enc)
                    raw_cols = df_sample.columns.tolist()
                    norm_cols = [str(c).strip().lower() for c in raw_cols]
                    current_usecols, rename_map = [], {}
                    for std_col, possible_names in target_fields.items():
                        for name in possible_names:
                            if name.lower() in norm_cols:
                                idx = norm_cols.index(name.lower())
                                actual_name = raw_cols[idx]
                                current_usecols.append(actual_name)
                                rename_map[actual_name] = std_col
                                break
                    df = pd.read_csv(filename, encoding=file_enc, usecols=current_usecols, on_bad_lines='skip')
                    chunk = df.rename(columns=rename_map)
                else:
                    # 无表头降级逻辑：第0列主叫，第1列被叫
                    df = pd.read_csv(filename, encoding=file_enc, header=None, on_bad_lines='skip')
                    chunk = df.iloc[:, [0, 1]].copy()
                    chunk.columns = ['callere164', 'calleee164']
                    chunk['holdtime'] = 0 

                # 36 脚本核心 holdtime 推导与清洗
                if 'holdtime' not in chunk.columns and 'starttime' in chunk.columns and 'stoptime' in chunk.columns:
                    chunk['holdtime'] = np.where(chunk['starttime'] == chunk['stoptime'], 0, 1)
                if 'holdtime' not in chunk.columns: chunk['holdtime'] = 0

                mask = (
                    (chunk['holdtime'].astype(float) <= 0) & 
                    (~chunk['calleee164'].astype(str).str.contains("QIANHAO|WuRaoHaoMa|DONGTAIDIFANG|/|\\?|,|#|\\\\|\\*|-", na=False, regex=True))
                )
                df_f = chunk[mask].copy()
                if df_f.empty: continue

                # 36 脚本核心 PASS/HMD 截取逻辑
                callee_str = df_f['calleee164'].astype(str)
                df_f['processed_calleee164'] = np.where(
                    callee_str.str.startswith('PASS') | callee_str.str.startswith('HMD'), 
                    callee_str.str[8:], callee_str.str[4:]
                )
                
                df_f['processed_calleee164'] = pd.to_numeric(df_f['processed_calleee164'], errors='coerce')
                df_f = df_f.dropna(subset=['processed_calleee164'])
                str_lens = df_f['processed_calleee164'].astype(np.int64).astype(str).str.len()
                df_f = df_f[(str_lens >= 10) & (str_lens <= 11)]
                
                valid_data_chunks.append(df_f[['callere164', 'processed_calleee164']])
                print(f"  √ 已处理: {os.path.basename(filename)}")
            except Exception as e: print(f"  × 失败 {os.path.basename(filename)}: {e}")

    if not valid_data_chunks: return

    # 阶段 2: 全局排序去重
    print("\n--- 阶段 2: 开始执行全局排序去重 (对齐 SQL MIN 逻辑) ---")
    all_df = pd.concat(valid_data_chunks, ignore_index=True)
    before_len = len(all_df)
    all_df.sort_values(by=['processed_calleee164', 'callere164'], ascending=[True, True], inplace=True)
    all_df.drop_duplicates(subset=['processed_calleee164'], keep='first', inplace=True)
    print(f" [+] 去重前: {before_len} | 去重后: {len(all_df)} | 剔除: {before_len - len(all_df)}")

    # 阶段 3: 分组导出
    print("\n--- 阶段 3: 执行分组导出 ---")
    curr_date = datetime.now().strftime('%m.%d')
    group_counts = all_df['callere164'].value_counts()
    
    for caller in group_counts[group_counts >= 50000].index:
        export_batches(all_df[all_df['callere164'] == caller], str(caller), output_folder, curr_date)
        
    small_group_data = all_df[all_df['callere164'].isin(group_counts[group_counts < 50000].index)]
    if not small_group_data.empty:
        export_batches(small_group_data, 'BBBB', output_folder, curr_date)

    # 阶段 4: 刷新格式
    refresh_csv_via_software(output_folder)
    print(f"\n🎉 全部流程完成！结果路径: {output_folder}")

if __name__ == "__main__":
    process_multi_paths_36()