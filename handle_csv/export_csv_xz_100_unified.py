# ==============================================================================
# 脚本名称: export_csv_xz_120_unified.py
# 核心功能: 
#   1. 支持多路径汇总与自动日期命名
#   2. 严格 11 位数字清洗与确定性全局去重
#   3. 统一导出命名：序号-新120接通高(话费较高)-月份.日期.csv
#   4. 尾部合并逻辑：若最后一包 < 20,000 条，则合并至前一个文件
#   5. 自动刷新 WPS/Excel 格式
# ==============================================================================

import os
import time
import pandas as pd
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
    """探测编码及表头"""
    encodings = ['utf-8-sig', 'gb18030', 'gbk', 'utf-8']
    keywords = ['主叫', '被叫', 'caller', 'callee', 'e164', '号码']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                first_line = f.readline().lower()
                has_header = any(k in first_line for k in keywords)
                return has_header, enc
        except Exception: continue
    return False, 'utf-8-sig'

def export_unified_batches(all_df, output_folder, curr_date):
    """统一分批次导出逻辑：不再按主叫区分文件夹，统一命名"""
    # 全局乱序处理
    all_df = all_df.sample(frac=1).reset_index(drop=True)
    
    export_df = pd.DataFrame({
        '客户姓名': [""] * len(all_df),
        '客户号码': all_df['calleee164'],
        '地址': [""] * len(all_df),
        '购买套数': [""] * len(all_df),
        '签收电话': [""] * len(all_df),
        '备注': [""] * len(all_df)
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
        for i in range(initial_batches - 1):
            batch_ranges.append((i * batch_size, (i + 1) * batch_size))
        batch_ranges.append(((initial_batches - 1) * batch_size, total_len))
    else:
        for i in range(initial_batches):
            batch_ranges.append((i * batch_size, (i + 1) * batch_size))
        if remainder > 0:
            batch_ranges.append((initial_batches * batch_size, total_len))

    # 直接在输出根目录下生成文件，不再创建主叫子文件夹
    if not os.path.exists(output_folder): os.makedirs(output_folder)

    file_identifier = "新120接通高(话费较高)"

    for i, (start, end) in enumerate(batch_ranges):
        # 命名格式参考：序号-新120接通高(话费较高)-月份.日期.csv
        file_name = f"{i + 1}-{file_identifier}-{curr_date}.csv"
        save_path = os.path.join(output_folder, file_name)
        export_df.iloc[start:end].to_csv(
            save_path, 
            index=False, 
            encoding='utf-8-sig', 
            lineterminator='\r\n'
        )
    print(f" [+] 成功导出: 共分出 {len(batch_ranges)} 个文件，总数 {total_len}")

def process_multi_paths():
    input_paths = []
    print("=== 跨日期多路径汇总处理工具 (120接通统一命名版) ===")
    print("请输入 all 文件夹的完整路径 (输入 'q' 结束输入):")
    
    while True:
        p = input(f"请输入路径 {len(input_paths)+1}: ").strip().strip('"')
        if p.lower() == 'q': break
        if os.path.exists(p): input_paths.append(p)
        else: print(" [!] 路径不存在，请重新输入。")

    if not input_paths: return

    # 正则识别日期
    all_paths_str = " ".join(input_paths)
    dates = re.findall(r'e_cdr_(20\d{6})', all_paths_str)
    
    if dates:
        dates.sort()
        folder_name = f"e_cdr_{dates[0]}-{dates[-1]}_120接通高"
    else:
        folder_name = f"汇总_120接通高_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 路径解析
    parent_dir = os.path.dirname(os.path.dirname(os.path.normpath(input_paths[0])))
    output_folder = os.path.join(parent_dir, folder_name)
    
    print(f"\n--- 阶段 1: 汇总读取 (输出至: {folder_name}) ---")
    all_data_list = []
    for path in input_paths:
        for file in os.listdir(path):
            if file.lower().endswith('.csv'):
                filepath = os.path.join(path, file)
                has_header, enc = check_header_and_encoding(filepath)
                try:
                    df = pd.read_csv(filepath, encoding=enc, dtype=str, header=0 if has_header else None, on_bad_lines='skip')
                    if not has_header: df.columns = ['col0', 'col1'] + list(df.columns[2:])
                    else: df.columns = [str(c).strip().lower() for c in df.columns]
                    
                    c_col = next((c for c in ['callere164', '主叫'] if c in df.columns), df.columns[0])
                    e_col = next((c for c in ['calleee164', '被叫'] if c in df.columns), df.columns[1])
                    
                    temp = df[[c_col, e_col]].copy()
                    temp.columns = ['callere164', 'calleee164']
                    temp['calleee164'] = temp['calleee164'].astype(str).str.strip().str[-11:]
                    mask = temp['calleee164'].str.match(r'^\d{11}$', na=False)
                    all_data_list.append(temp[mask])
                    print(f"  √ 已加载: {file}")
                except Exception as e: print(f"  × 跳过 {file}: {e}")

    if not all_data_list: 
        print("未提取到任何有效数据。")
        return
    
    # 2. 全局去重逻辑 (对齐 SQL MIN 逻辑)
    all_df = pd.concat(all_data_list, ignore_index=True)
    all_df.sort_values(by=['calleee164', 'callere164'], ascending=[True, True], inplace=True)
    all_df.drop_duplicates(subset=['calleee164'], keep='first', inplace=True)

    # 3. 统一导出处理
    curr_date = datetime.now().strftime("%m.%d")
    export_unified_batches(all_df, output_folder, curr_date)

    # 4. 刷新格式
    refresh_csv_via_software(output_folder)
    print(f"\n任务完成! 结果保存在: {output_folder}")

if __name__ == "__main__":
    process_multi_paths()