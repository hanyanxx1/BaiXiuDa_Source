# ==============================================================================
# 脚本名称: export_csv_xz_120_unified.py (扁平化目录匹配版)
# 核心功能: 
#   1. 内置路径数组，仅需输入日期即可自动扫描根目录下的对应日期文件
#   2. 严格 11 位数字清洗与确定性全局去重
#   3. 统一导出命名：序号-新120接通高(话费较高)-月份.日期.csv
#   4. 强力随机：导出前执行 3 次随机洗牌
#   5. 打印绝对路径日志，自动刷新 WPS/Excel 格式
# ==============================================================================

import os
import pandas as pd
import win32com.client as win32
import re
from datetime import datetime

# ==========================================
# 【配置区】请在此处手动维护你的所有服务器基础路径
# ==========================================
BASE_PATHS = [
    r"F:\120秒话单\001-AAAA-112.25.240.74-120秒",
    r"F:\120秒话单\002-QQQQ-211.103.25.247-120秒",
    r"F:\120秒话单\003-BBBB-61.158.166.36-120秒",
    r"F:\120秒话单\004-EEEE-36.139.87.39-120秒",
    r"F:\120秒话单\005-DDDD-36.139.251.203-120秒",
    r"F:\120秒话单\006-CCCC-211.103.25.241-120秒",
    r"F:\120秒话单\007-FFFF-211.103.25.243-120秒",
    r"F:\120秒话单\008-GGGG-112.25.240.67-120秒",
    r"F:\120秒话单\009-HHHH-211.103.25.240-120秒"
    # 在这里可以随时添加更多路径...
]

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
            # 使用动态调用，防止 gen_py 缓存损坏报错
            app = win32.Dispatch('Et.Application')
        except:
            app = win32.Dispatch('Excel.Application')
        
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
    
    # 核心修改点：执行 3 次强力洗牌
    for _ in range(3):
        all_df = all_df.sample(frac=1)
    all_df = all_df.reset_index(drop=True)
    
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
    print("=== 跨日期多路径汇总处理工具 (扁平目录版) ===")
    
    target_date = input("👉 请输入要处理的文件日期 (例如: 20260401): ").strip()
    if not target_date:
        print("❌ 错误：日期不能为空！")
        return
        
    matched_files = []
    print("\n--- 阶段 0: 扫描直接匹配文件 ---")
    for base in BASE_PATHS:
        if os.path.exists(base):
            try:
                # 遍历基础路径下的所有文件，寻找名字中带有目标日期的 csv 文件
                for file_name in os.listdir(base):
                    if target_date in file_name and file_name.lower().endswith(".csv"):
                        filepath = os.path.join(base, file_name)
                        matched_files.append(filepath)
                        print(f" [命中] -> {filepath}")
            except Exception as e:
                print(f" [错误] 无法读取目录 {base}: {e}")
        else:
            print(f" [跳过] 基础路径不存在: {base}")

    if not matched_files: 
        print(f"\n❌ 在所有配置的路径中，均未找到包含日期 '{target_date}' 的 CSV 文件。")
        return

    folder_name = f"e_cdr_{target_date}_120接通高"

    # 将输出目录放在第一个成功命中文件的路径的父级 (即 F:\00秒话单\ 目录下)
    parent_dir = os.path.dirname(os.path.normpath(os.path.dirname(matched_files[0])))
    output_folder = os.path.join(parent_dir, folder_name)
    
    print(f"\n--- 阶段 1: 汇总读取 (输出至: {output_folder}) ---")
    all_data_list = []
    
    # 核心修改点：直接遍历命中文件的绝对路径
    for filepath in matched_files:
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
            
            print(f"  √ 已加载: {filepath}")
        except Exception as e: 
            print(f"  × 跳过 {filepath}: {e}")

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