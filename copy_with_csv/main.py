import os
import shutil
import pandas as pd

def copy_files_fuzzy():
    print("=== 文件名匹配工具 (自动忽略 .csv 后缀) ===")
    
    # 1. 输入路径
    file_path = input("1. 请输入任务清单路径 (CSV/XLSX): ").strip().strip('"')
    if not os.path.exists(file_path):
        print("错误: 找不到清单文件")
        return

    search_dir = input("2. 请输入要查找的源目录: ").strip().strip('"')
    target_dir = input("3. 请输入保存文件的目标路径: ").strip().strip('"')

    if not os.path.isdir(search_dir):
        print("错误: 源目录不存在")
        return
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # 2. 读取并统一清洗清单 (剥离 .csv 后缀)
    raw_names = []
    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            try:
                df = pd.read_csv(file_path, usecols=[0], header=None, encoding='utf-8-sig')
            except:
                df = pd.read_csv(file_path, usecols=[0], header=None, encoding='gbk')
            raw_names = df[0].astype(str).tolist()
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path, usecols=[0], header=None)
            raw_names = df.iloc[:, 0].astype(str).tolist()
    except Exception as e:
        print(f"读取清单出错: {e}")
        return

    # 核心步骤：将清单中的名字统一去掉 .csv (不区分大小写)
    clean_targets = []
    for name in raw_names:
        name = name.strip()
        if not name or name.lower() == 'nan':
            continue
        # 如果以 .csv 结尾则去掉，否则保持原样
        if name.lower().endswith('.csv'):
            base_name = name[:-4]
        else:
            base_name = name
        clean_targets.append(base_name)
    
    # 清理后的任务去重
    clean_targets = list(set(clean_targets))
    total_tasks = len(clean_targets)
    print(f"\n清单处理完成，需匹配的纯文件名共计: {total_tasks} 个")

    # 3. 递归查找逻辑
    copy_count = 0
    skip_count = 0
    not_found_list = []

    for i, target_base in enumerate(clean_targets, 1):
        print(f"进度: [{i}/{total_tasks}] 正在匹配: {target_base}", end="\r")
        
        found = False
        # 在源目录中递归
        for root, dirs, files in os.walk(search_dir):
            for f in files:
                # 获取磁盘文件的纯文件名 (不含后缀)
                disk_file_base, disk_file_ext = os.path.splitext(f)
                
                # 匹配逻辑：纯文件名相同
                if disk_file_base == target_base:
                    source_full_path = os.path.join(root, f)
                    target_full_path = os.path.join(target_dir, f)
                    
                    # 检查目标位置是否已存在
                    if os.path.exists(target_full_path):
                        skip_count += 1
                        found = True
                        break
                    
                    try:
                        shutil.copy2(source_full_path, target_full_path)
                        copy_count += 1
                        found = True
                    except Exception as e:
                        print(f"\n[错误] 拷贝失败 {f}: {e}")
                    break # 找到该任务的一个文件就跳出当前 os.walk 内部循环
            
            if found: break # 找到后跳出 root 遍历

        if not found:
            not_found_list.append(target_base)

    # 4. 统计输出
    print("\n" + "="*30)
    print(f"处理完成！统计如下：")
    print(f"  √ 成功拷贝: {copy_count}")
    print(f"  → 跳过(已存在): {skip_count}")
    print(f"  × 未找到: {len(not_found_list)}")
    
    if not_found_list:
        log_path = os.path.join(target_dir, "未找到文件清单.txt")
        with open(log_path, "w", encoding="utf-8") as log_f:
            log_f.write("\n".join(not_found_list))
        print(f"\n缺失详情已保存至: {log_path}")
    
    input("\n任务结束，按回车退出...")

if __name__ == "__main__":
    copy_files_fuzzy()