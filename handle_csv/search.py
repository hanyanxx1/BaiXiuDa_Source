import os
import pandas as pd

def find_phone_records():
    # 1. 交互式输入
    target_number = input("请输入要排查的故障号码: ").strip()
    # 建议输入最外层的根路径，例如 F:\00秒话单\
    search_path = input("请输入要扫描的根路径: ").strip().strip('"')
    
    if not target_number or not search_path:
        print("错误：号码或路径不能为空！")
        return

    all_matches = [] 
    total_files_scanned = 0
    total_folders_scanned = 0
    
    print(f"\n开始深度全局扫描...")
    print(f"目标被叫号码: {target_number}")
    print("-" * 80)

    encodings = ['utf-8-sig', 'utf-8', 'gb18030', 'ansi']

    # 深度递归遍历所有子文件夹
    for root, dirs, files in os.walk(search_path):
        total_folders_scanned += 1
        # 实时提示当前正在扫描的文件夹，让你确认路径是否覆盖全了
        print(f"正在进入目录: {root}", end='\r') 
        
        for file in files:
            if file.lower().endswith('.csv'):
                total_files_scanned += 1
                file_path = os.path.join(root, file)
                
                for enc in encodings:
                    try:
                        df = pd.read_csv(file_path, dtype=str, encoding=enc, on_bad_lines='skip')
                        
                        if '被叫号码' in df.columns and '主叫号码' in df.columns:
                            # 模糊匹配
                            mask = df['被叫号码'].str.contains(target_number, na=False)
                            matches = df[mask][['主叫号码', '被叫号码']].copy()
                            
                            if not matches.empty:
                                # 记录来源文件的【相对路径】，方便你定位到底在哪个子文件夹
                                matches['文件位置'] = os.path.relpath(file_path, search_path)
                                all_matches.append(matches)
                        break 
                    except Exception:
                        continue 

    print("\n" + "-" * 80) # 换行清除结尾的 \r 打印内容
    
    # 2. 结果汇总输出
    if all_matches:
        final_df = pd.concat(all_matches, ignore_index=True)
        
        # 调整表头，让文件位置显示更完整
        print(f"{'主叫号码':<15} | {'被叫号码':<25} | {'文件相对路径'}")
        print("-" * 80)
        
        for _, row in final_df.iterrows():
            print(f"{row['主叫号码']:<15} | {row['被叫号码']:<25} | {row['文件位置']}")
            
        print("-" * 80)
        print(f"完成！共扫描了 {total_folders_scanned} 个文件夹，{total_files_scanned} 个 CSV 文件。")
        print(f"最终在上述文件中找到了 {len(final_df)} 条匹配记录。")
    else:
        print(f"未找到相关记录。已检索目录数: {total_folders_scanned}，文件数: {total_files_scanned}。")

if __name__ == "__main__":
    find_phone_records()