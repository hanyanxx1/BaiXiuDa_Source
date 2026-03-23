# 合并客户号码汇总_去重版.xlsx 中的客户号码列
import os
import sys
import pandas as pd

def merge_csv_phones(input_path):
    output_excel = "客户号码汇总_去重版.xlsx"
    all_numbers = []
    
    # 检查路径是否存在
    if not os.path.exists(input_path):
        print(f"错误: 路径 '{input_path}' 不存在")
        return

    print(f"开始递归扫描路径: {input_path}")
    
    # 1. 递归遍历
    for root, dirs, files in os.walk(input_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                
                # 2. 尝试不同编码读取文件
                df = None
                for encoding in ['utf-8-sig', 'gbk', 'utf-8']:
                    try:
                        # usecols=[1] 读取第二列；dtype=str 保持原始文本（防止手机号变科学计数法）
                        df = pd.read_csv(file_path, usecols=[1], dtype=str, encoding=encoding)
                        break # 读取成功则退出尝试
                    except Exception:
                        continue
                
                if df is not None:
                    # 提取列并重命名，确保一致性
                    col_data = df.iloc[:, 0].dropna() # 去除空值
                    all_numbers.append(col_data)
                    print(f"已处理: {file} (行数: {len(col_data)})")
                else:
                    print(f"跳过文件 {file}: 无法识别编码或格式不正确")

    if not all_numbers:
        print("未提取到任何有效数据，请检查 CSV 格式。")
        return

    # 3. 汇总数据
    print("\n正在合并数据并进行去重...")
    full_df = pd.concat(all_numbers, ignore_index=True).to_frame(name='客户号码')

    # 4. 去重
    total_raw = len(full_df)
    full_df.drop_duplicates(subset=['客户号码'], inplace=True)
    total_unique = len(full_df)
    
    print("-" * 30)
    print(f"汇总结果统计：")
    print(f"原始总行数: {total_raw}")
    print(f"去重后行数: {total_unique}")
    print(f"发现重复项: {total_raw - total_unique}")
    print("-" * 30)

    # 5. 分 Sheet 写入 Excel (每 Sheet 限制 100 万行)
    rows_per_sheet = 1000000
    num_sheets = (len(full_df) // rows_per_sheet) + 1

    print(f"正在保存至 Excel (共 {num_sheets} 个工作表)...")
    try:
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            for i in range(num_sheets):
                start_row = i * rows_per_sheet
                end_row = (i + 1) * rows_per_sheet
                chunk = full_df.iloc[start_row:end_row]
                
                if not chunk.empty:
                    sheet_name = f"号码汇总_第{i+1}页"
                    chunk.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"保存成功！文件名为: {output_excel}")
    except Exception as e:
        print(f"保存失败: {e}")

if __name__ == "__main__":
    # 从命令行获取路径参数
    if len(sys.argv) < 2:
        print("使用方法: py .\merge_csv_phones.py <你的文件夹路径>")
    else:
        target_path = sys.argv[1]
        merge_csv_phones(target_path)