# 该脚本用于将指定目录下的所有 .csv 文件根据核心标识符进行分类并移动到新创建的文件夹中。
# 每个文件夹的名称格式为：核心标识符-文件数量（例如：core1-3）。
# 如果核心标识符相同，会将所有文件移动到一个文件夹中。

import os
import shutil
from collections import defaultdict

def organize_files(target_dir):
    # 确保路径是绝对路径
    target_dir = os.path.abspath(target_dir)
    
    # 检查路径是否存在
    if not os.path.isdir(target_dir):
        print(f"错误: 路径 '{target_dir}' 不存在或不是一个目录。")
        return

    # 获取目标目录下所有的 .csv 文件
    files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f)) and f.endswith('.csv')]
    
    if not files:
        print(f"在 '{target_dir}' 中没有找到 .csv 文件。")
        return

    # 1. 统计每个核心标识符出现次数
    file_groups = defaultdict(list)
    for file_name in files:
        parts = file_name.split('-')
        if len(parts) > 1:
            core_name = parts[1]
            file_groups[core_name].append(file_name)

    # 2. 创建文件夹并移动文件
    for core_name, file_list in file_groups.items():
        count = len(file_list)
        folder_name = f"{core_name}-{count}"
        folder_path = os.path.join(target_dir, folder_name)
        
        # 如果文件夹不存在则创建
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"创建文件夹: {folder_name}")

        # 移动文件
        for file_name in file_list:
            src_path = os.path.join(target_dir, file_name)
            dst_path = os.path.join(folder_path, file_name)
            
            try:
                shutil.move(src_path, dst_path)
                print(f"已移动: {file_name} -> {folder_name}/")
            except Exception as e:
                print(f"移动文件 {file_name} 出错: {e}")

if __name__ == "__main__":
    while True:
        path_input = input("\n请拖入文件夹并按回车 (输入 exit 退出): ")
        if path_input.lower() == 'exit':
            break
        path_input = path_input.strip().replace('"', '')
        organize_files(path_input)
        print("\n任务完成！")
