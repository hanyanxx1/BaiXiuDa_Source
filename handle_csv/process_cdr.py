# 该脚本用于处理指定目录下的秒话单文件，根据主叫号码进行分类合并、去重，并输出到新文件夹。
# 处理完成后，自动调用 WPS/Excel 接口打开并重新保存生成的文件，确保文件格式刷新。

import os
import pandas as pd
from datetime import datetime
import win32com.client as win32

def refresh_csv_via_wps(folder_path):
    """
    遍历指定目录及其所有子目录，寻找 CSV 文件并通过 WPS/Excel 重新保存刷新
    """
    print("\n" + "=" * 40)
    print("开始执行文件刷新(WPS/Excel自动保存)逻辑")
    print("=" * 40)
    
    csv_files = []
    # 使用 os.walk 递归遍历，确保能读到诸如 "汇总/AAAA-2/" 子文件夹中的csv
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    if not csv_files:
        print(f"未在 {folder_path} 及其子目录下找到CSV文件。")
        return

    # 尝试连接 WPS (ET 是 WPS 表格的名称)，如果没有 WPS 则尝试 Excel
    try:
        app = win32.gencache.EnsureDispatch('Et.Application') # WPS表格
    except:
        try:
            app = win32.gencache.EnsureDispatch('Excel.Application') # Excel
        except Exception as e:
            print(f"错误：未发现安装 WPS 或 Excel! 无法执行刷新操作。详细信息: {e}")
            return

    # 设置软件不可见，禁用弹窗
    app.Visible = False 
    app.DisplayAlerts = False 

    print(f"正在通过软件接口处理 {len(csv_files)} 个文件...")

    for index, file_path in enumerate(csv_files, 1):
        try:
            # 1. 打开文件 (绝对路径)
            abs_path = os.path.abspath(file_path)
            wb = app.Workbooks.Open(abs_path)
            
            # 2. 保存 (等同于 Ctrl + S)
            wb.Save()
            
            # 3. 关闭
            wb.Close()
            
            print(f"[{index}/{len(csv_files)}] 软件已刷新: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"[{index}] 失败: {os.path.basename(file_path)} | 原因: {e}")

    # 退出软件进程
    app.Quit()
    print("所有文件已真实保存并刷新。")


def process_cdr_files(folder_param):
    # 1. 拼接三个源文件夹路径
    path_1 = rf"F:\00秒话单\005-AAAA-112.25.240.74-0秒\{folder_param}"
    base_paths = [
        path_1,
        rf"F:\00秒话单\006-EEEE-36.139.87.39-0秒\{folder_param}",
        rf"F:\00秒话单\007-DDDD-36.139.251.203-0秒\{folder_param}"
    ]

    # 将输出基准路径设定为 path_1 下的“汇总”文件夹
    out_base_dir = os.path.join(path_1, "汇总")
    os.makedirs(out_base_dir, exist_ok=True)

    # 2. 遍历路径，按“主叫”归类文件
    caller_files_map = {}

    for path in base_paths:
        if not os.path.exists(path):
            print(f"提示：路径不存在，跳过 -> {path}")
            continue

        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            # 增加 isfile 判断，防止读取到生成的子文件夹
            if os.path.isfile(file_path) and file_name.endswith('.csv'):
                # 按照 '-' 分割文件名，第2段（索引1）即为主叫
                parts = file_name.split('-')
                if len(parts) >= 2:
                    caller = parts[1]
                    if caller not in caller_files_map:
                        caller_files_map[caller] = []
                    caller_files_map[caller].append(file_path)

    if not caller_files_map:
        print("未找到任何符合条件的 CSV 文件，请检查参数和路径。")
        return

    # 获取当前日期，用于输出文件名
    current_date_str = datetime.now().strftime('%m.%d')
    chunk_size = 50000

    # 3 & 4. 按主叫合并、去重并分割输出
    for caller, file_paths in caller_files_map.items():
        print(f"\n正在处理主叫: {caller}，共找到 {len(file_paths)} 个源文件...")
        df_list = []
        
        for f in file_paths:
            try:
                # 使用 dtype 防止科学计数法精度丢失
                df = pd.read_csv(f, dtype={'客户号码': str, '签收电话': str}, encoding='gbk')
            except UnicodeDecodeError:
                # 容错：如果 GBK 读取失败，尝试 UTF-8
                df = pd.read_csv(f, dtype={'客户号码': str, '签收电话': str}, encoding='utf-8')
            df_list.append(df)

        if not df_list:
            continue

        # 汇总该主叫下的所有数据
        merged_df = pd.concat(df_list, ignore_index=True)
        initial_count = len(merged_df)
        
        # 针对“客户号码”进行去重，保留第一条
        if '客户号码' in merged_df.columns:
            dedup_df = merged_df.drop_duplicates(subset=['客户号码'], keep='first')
        else:
            print(f"警告：主叫 {caller} 的数据中未找到'客户号码'列，跳过去重。")
            dedup_df = merged_df
            
        final_count = len(dedup_df)
        print(f"-> 汇总 {initial_count} 条，去重后剩余 {final_count} 条 (排重 {initial_count - final_count} 条)")

        # 计算所需的分割文件数
        total_rows = len(dedup_df)
        num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size > 0 else 0)

        if num_chunks == 0:
            print(f"-> 主叫 {caller} 数据为空，跳过输出。")
            continue

        # 在“汇总”目录下创建专属文件夹
        caller_out_dir = os.path.join(out_base_dir, f"{caller}-{num_chunks}")
        os.makedirs(caller_out_dir, exist_ok=True)

        # 5 & 6. 按每 50000 条分割并导出到专属文件夹
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            chunk_df = dedup_df.iloc[start_idx:end_idx]

            # 序号从 1 开始
            seq_num = i + 1
            out_filename = f"{seq_num}-{caller}-0-{current_date_str}.csv"
            out_filepath = os.path.join(caller_out_dir, out_filename)

            # 导出 CSV
            chunk_df.to_csv(out_filepath, index=False, encoding='utf-8-sig')
            print(f"   [保存] -> 汇总\\{caller}-{num_chunks}\\{out_filename} (包含 {len(chunk_df)} 条)")

    # ================= 新增逻辑 =================
    # 所有文件生成完毕后，针对 out_base_dir（汇总目录）调用软件刷新逻辑
    refresh_csv_via_wps(out_base_dir)
    # ============================================

if __name__ == "__main__":
    # 1. 交互式输入参数（即子文件夹名称）
    print("=" * 40)
    print("批量话单处理工具 (汇总 -> 去重 -> 归类分割 -> WPS自动刷新)")
    print("=" * 40)
    param = input("请输入参数 (例如 e_cdr_20260222): ").strip()
    
    if param:
        process_cdr_files(param)
        print(f"\n全部流程处理完成！请前往 F:\\00秒话单\\005-AAAA-112.25.240.74-0秒\\{param}\\汇总 目录下查看结果。")
    else:
        print("参数不能为空！")