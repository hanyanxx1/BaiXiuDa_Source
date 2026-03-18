# 该脚本用于批量提取指定目录下所有 CSV 文件的客户号码（假设客户号码在第二列），
# 并将所有号码合并到一个新的 CSV 文件中。
# 每个号码占一行，列标题为 "客户号码"。

import pandas as pd
import os

def main():
    print("=== 客户号码批量提取工具 (参谋增强版) ===")
    
    # 1. 确定目标目录
    target_dir = input("请输入文件夹路径 (直接回车表示当前目录): ").strip()
    if not target_dir:
        target_dir = os.getcwd() # 获取当前脚本所在位置
    
    if not os.path.exists(target_dir):
        print(f"[错误] 路径不存在: {target_dir}")
        input("按回车退出..."); return

    # 2. 确定输出文件名
    output_name = input("请输入保存的文件名 (如 result.csv): ").strip()
    if not output_name.lower().endswith('.csv'):
        output_name += '.csv'
    
    # 完整输出路径
    output_path = os.path.join(target_dir, output_name)

    # 3. 扫描文件
    files = [f for f in os.listdir(target_dir) if f.endswith('.csv') and f != output_name]

    if not files:
        print(f"\n[错误] 在路径 {target_dir} 下未找到 CSV 文件！")
        print("提示：请检查文件夹路径是否正确，或文件后缀是否为小写 .csv")
        input("按回车退出..."); return

    all_data = []
    print(f"\n找到 {len(files)} 个文件，准备开始提取...")

    for file in files:
        file_path = os.path.join(target_dir, file)
        try:
            # 物理锚点：锁定第二列，强制字符串
            df = pd.read_csv(file_path, usecols=[1], dtype=str)
            numbers = df.iloc[:, 0].dropna().tolist()
            all_data.extend(numbers)
            print(f"  [OK] {file} -> 提取 {len(numbers)} 条")
        except Exception as e:
            print(f"  [跳过] {file} 错误: {e}")

    # 4. 落地保存
    if all_data:
        result_df = pd.DataFrame(all_data, columns=['客户号码'])
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print("\n" + "="*30)
        print(f"提取成功！总计: {len(all_data)} 条号码")
        print(f"文件位置: {output_path}")
        print("="*30)
    else:
        print("\n[警告] 未能提取到任何号码数据。")

    input("\n任务完成，按回车键退出...")

if __name__ == "__main__":
    main()