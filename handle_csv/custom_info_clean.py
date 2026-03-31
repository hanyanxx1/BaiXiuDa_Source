import sys
import json
import random
import os
import datetime
try:
    import win32com.client
except ImportError:
    print("❌ 缺少依赖库！请先在终端运行: pip install pywin32")
    sys.exit(1)

def refresh_csv_with_excel(file_paths):
    """调用本地 Excel 引擎，在后台静默刷新并保存 CSV 文件"""
    print(f"\n[4/4] 正在唤醒本地 Excel 引擎，准备对 {len(file_paths)} 个文件进行静默刷新...")
    
    excel = win32com.client.DispatchEx("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False
    
    success_count = 0
    try:
        for idx, file_path in enumerate(file_paths, 1):
            abs_path = os.path.abspath(file_path)
            print(f"   -> 正在刷新 ({idx}/{len(file_paths)}): {os.path.basename(abs_path)}")
            try:
                # 用 Excel 打开并强制原样保存
                wb = excel.Workbooks.Open(abs_path)
                wb.Save()
                wb.Close(SaveChanges=True)
                success_count += 1
            except Exception as e:
                print(f"      [警告] 文件 {os.path.basename(abs_path)} 刷新失败: {e}")
    finally:
        excel.Quit()
        
    print(f"  -> Excel 刷新完毕！成功处理 {success_count} 个文件。")

def main():
    print("="*55)
    print(" 🚀 PSCC 数据清洗、重组与 Excel 刷新工具 (终极版)")
    print("="*55)

    input_file = input("1. 请输入原始 TSV 数据文件路径: ").strip()
    input_file = input_file.strip('"').strip("'") 

    output_dir = input("2. 请输入处理后文件的保存目录: ").strip()
    output_dir = output_dir.strip('"').strip("'")

    chunk_input = input("3. 请输入每个切割文件的行数 (直接回车默认为 50000): ").strip()
    chunk_size = 50000 if not chunk_input else int(chunk_input)

    default_date = datetime.datetime.now().strftime("%m.%d")
    date_input = input(f"4. 请输入文件名中的日期后缀 (直接回车默认为今天 {default_date}): ").strip()
    date_str = default_date if not date_input else date_input

    print("\n" + "="*55)
    print(" 📋 终极执行清单：")
    print(f"   - 原始文件: {input_file}")
    print(f"   - 输出目录: {output_dir}")
    print(f"   - 动作: 极速去重 -> 乱序洗牌 -> {chunk_size}行切分 -> Excel 静默刷新")
    print("="*55)

    while True:
        confirm = input("\n⚠️ 参数确认无误，请按 'q' 开始执行 (按 'c' 取消退出): ").strip().lower()
        if confirm == 'q':
            break
        elif confirm == 'c':
            print("已取消操作，安全退出。")
            return
        else:
            print("输入无效，请重新输入。")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    seen_phones = set()
    unique_records = []
    generated_files = [] # 记录所有生成的文件路径，留给最后一步 Excel 刷新

    print("\n[1/4] 正在读取原始数据，进行内存级去重与 JSON 解析 (遇到脏字符将自动忽略)...")
    try:
        # 加上 errors='ignore'，彻底免疫任何乱码报错
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                parts = line.strip('\n').split('\t')
                if len(parts) < 1: continue
                phone = parts[0].strip()
                
                if not phone or phone.upper() == 'NULL': continue
                
                if phone not in seen_phones:
                    seen_phones.add(phone)
                    name_db = parts[1].strip() if len(parts) > 1 and parts[1].strip() != 'NULL' else ''
                    fields_str = parts[2].strip() if len(parts) > 2 and parts[2].strip() != 'NULL' else '{}'
                    
                    try:
                        fields = json.loads(fields_str)
                    except:
                        fields = {}
                    
                    customer_name = fields.get('客户姓名', '')
                    if not customer_name: customer_name = name_db
                    
                    address = fields.get('地址', '')
                    purchase_qty = fields.get('购买套数', '')
                    sign_phone = fields.get('签收电话', '')
                    remark = fields.get('备注', '')
                    
                    unique_records.append([customer_name, phone, address, purchase_qty, sign_phone, remark])
    except Exception as e:
        print(f"\n❌ 读取文件失败: {e}")
        return

    print(f"  -> 去重完毕！共清洗出 {len(unique_records)} 条有效唯一客户数据。")

    print("[2/4] 正在进行全局随机乱序洗牌 (保障系统外呼呼出率)...")
    random.shuffle(unique_records)

    print("[3/4] 开始切割文件并生成 PSCC 标准格式...")
    total_chunks = (len(unique_records) + chunk_size - 1) // chunk_size

    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = start_idx + chunk_size
        chunk_data = unique_records[start_idx:end_idx]
        
        batch_num = (chunk_idx // 99) + 1
        seq_num = (chunk_idx % 99) + 1
        folder_name = f"{batch_num}-99"
        
        if batch_num % 2 == 1:
            seq_str = str(seq_num)
        else:
            seq_str = f"A{seq_num}"
            
        file_name = f"{seq_str}-HB1-0-{date_str}.csv"
        
        target_dir = os.path.join(output_dir, folder_name)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            
        file_path = os.path.join(target_dir, file_name)
        generated_files.append(file_path)
        
        with open(file_path, 'w', encoding='utf-8-sig') as out_f:
            out_f.write("客户姓名,客户号码,地址,购买套数,签收电话,备注\n")
            for row in chunk_data:
                safe_row = [str(x).replace(',', '，').replace('\n', ' ') for x in row]
                out_f.write(",".join(safe_row) + "\n")

    # 执行最后一步：Excel 刷新
    if generated_files:
        refresh_csv_with_excel(generated_files)

    print(f"\n✅ [大功告成] 全部处理完成！成品文件已就绪。")

    print("\n" + "="*55)
    print(" 📊 最终数据对账报告")
    print("="*55)
    print(f"   ▶ 处理完毕的唯一号码总数: {len(unique_records)}")
    print("   ▶ 共生成标准上传文件: {len(generated_files)} 个")
    print("="*55 + "\n")

if __name__ == "__main__":
    main()