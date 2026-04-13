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
        print(f"   ❌ 错误: 路径 '{target_dir}' 不存在或不是一个目录。")
        return

    # 获取目标目录下所有的 .csv 文件
    files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f)) and f.endswith('.csv')]
    
    if not files:
        print(f"   ⚠️ 在 '{target_dir}' 中没有找到 .csv 文件，已跳过。")
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
            print(f"   📁 创建文件夹: {folder_name}")

        # 移动文件
        for file_name in file_list:
            src_path = os.path.join(target_dir, file_name)
            dst_path = os.path.join(folder_path, file_name)
            
            try:
                shutil.move(src_path, dst_path)
                print(f"   ✅ 已移动: {file_name} -> {folder_name}/")
            except Exception as e:
                print(f"   ❌ 移动文件 {file_name} 出错: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("   VOS CSV 归档整理工具 (多路径增强版)")
    print("=" * 50)

    # 1. 交互式收集任意多个路径
    base_paths = []
    print("👉 请输入/拖入要处理的目录路径 (支持多个，每次输入一个后按回车，直接按回车结束输入):")
    
    while True:
        path_input = input(f"   -> 第 {len(base_paths) + 1} 个路径: ").strip()

        # 去除拖拽文件夹时可能自动生成的引号
        path_input = path_input.replace('"', '').replace("'", "")

        # 如果用户直接按回车（输入为空），则结束路径收集
        if not path_input:
            if len(base_paths) == 0:
                print("\n❌ 您没有输入任何路径，已退出。")
                exit(0)
            break

        # 实时校验路径是否存在且为目录
        if os.path.isdir(path_input):
            base_paths.append(path_input)
        else:
            print(f"   ⚠️ 警告：该路径不存在或不是文件夹，请检查后重新输入！ '{path_input}'")

    print("\n" + "=" * 50)
    print("🚀 开始执行批量归档任务")
    print("=" * 50)

    # 2. 遍历所有收集到的路径并执行归档
    for idx, base_path in enumerate(base_paths, 1):
        print(f"\n▶️ 正在处理第 {idx}/{len(base_paths)} 个路径: {base_path}")
        organize_files(base_path)

    print("\n" + "=" * 50)
    print("🎉 所有路径归档整理任务全部完成！")
    print("=" * 50)


# ==============================================================================
# 【调用示例与运行说明】
# ==============================================================================
#
# 1. 运行方式：在终端或命令行中执行
#    python3 move_files.py
#
# 2. 交互过程示例：
#    👉 请输入/拖入要处理的目录路径 (支持多个，每次输入一个后按回车，直接按回车结束输入):
#       -> 第 1 个路径: /var/lib/mysql-files/001-AAAA-112.25.240.74-0秒/
#       -> 第 2 个路径: "D:\VosData\002-BBBB-112.25.240.75-0秒"  (脚本会自动剥离双引号)
#       -> 第 3 个路径: [直接按回车键结束输入]
#
#    ==================================================
#    🚀 开始执行批量归档任务
#    ==================================================
#
#    ▶️ 正在处理第 1/2 个路径: /var/lib/mysql-files/001-AAAA-112.25.240.74-0秒/
#       📁 创建文件夹: AAAA-1
#       ✅ 已移动: 1-AAAA-Vos74-251126-12.12.csv -> AAAA-1/
#       📁 创建文件夹: BBBB-2
#       ✅ 已移动: 1-BBBB-Vos74-251126-12.12.csv -> BBBB-2/
#       ✅ 已移动: 2-BBBB-Vos74-251126-12.12.csv -> BBBB-2/
#
#    ▶️ 正在处理第 2/2 个路径: D:\VosData\002-BBBB-112.25.240.75-0秒
#       ⚠️ 在 'D:\VosData\002-BBBB-112.25.240.75-0秒' 中没有找到 .csv 文件，已跳过。
#
#    ==================================================
#    🎉 所有路径归档整理任务全部完成！
#    ==================================================
# ==============================================================================