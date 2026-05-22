import os
import pandas as pd
import re

def check_header_and_encoding(filepath):
    """探测文件的编码格式"""
    encodings = ['utf-8-sig', 'gb18030', 'gbk', 'utf-8']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                f.readline()
                return enc
        except Exception:
            continue
    return 'utf-8-sig'

def validate_mobile_numbers():
    print("=" * 50)
    print("   VOS CSV 手机号码合法性抽检工具")
    print("=" * 50)
    
    # 1. 交互式收集任意多个路径
    base_paths = []
    print("👉 请输入/拖入要检查的根目录路径 (支持多个，每次输入一个后按回车，直接按回车结束输入):")
    
    while True:
        path_input = input(f"   -> 第 {len(base_paths) + 1} 个路径: ").strip()
        path_input = path_input.replace('"', '').replace("'", "")
        
        if not path_input:
            if len(base_paths) == 0:
                print("\n❌ 您没有输入任何路径，已退出。")
                return
            break
            
        if os.path.isdir(path_input):
            base_paths.append(path_input)
        else:
            print(f"   ⚠️ 警告：该路径不存在或不是文件夹，请检查后重新输入！ '{path_input}'")

    print("\n" + "=" * 50)
    print("🚀 开始执行批量检查任务")
    print("=" * 50)

    # 定义中国大陆合法手机号正则：11位数字，1开头，第二位3-9
    mobile_pattern = re.compile(r'^1[3-9]\d{9}$')

    global_total = 0
    global_valid = 0

    # 2. 遍历所有收集到的路径
    for idx, base_path in enumerate(base_paths, 1):
        print(f"\n▶️ 正在扫描第 {idx}/{len(base_paths)} 个路径: {base_path}")
        
        # 3. 递归读取目录
        for root, dirs, files in os.walk(base_path):
            # 遇到 'all' 文件夹直接跳过，不深入遍历
            if 'all' in dirs:
                dirs.remove('all')

            for file in files:
                if file.lower().endswith('.csv'):
                    filepath = os.path.join(root, file)
                    enc = check_header_and_encoding(filepath)

                    try:
                        # 读取 CSV 文件
                        df = pd.read_csv(filepath, encoding=enc, dtype=str, on_bad_lines='skip')
                        
                        # 4. 寻找“客户号码”列
                        target_col = None
                        for col in df.columns:
                            if '客户号码' in str(col):
                                target_col = col
                                break
                                
                        if not target_col:
                            print(f"  ⚠️ 跳过 {file}: 未找到“客户号码”表头列")
                            continue

                        # 清洗并提取数据（去除空值、去除前后空格）
                        numbers = df[target_col].dropna().astype(str).str.strip()
                        file_total = len(numbers)
                        
                        if file_total == 0:
                            print(f"  ⚠️ 跳过 {file}: 该文件中的客户号码列数据为空")
                            continue

                        # 执行正则匹配
                        valid_mask = numbers.str.match(mobile_pattern)
                        file_valid = valid_mask.sum()
                        file_invalid = file_total - file_valid

                        # 累加到全局变量
                        global_total += file_total
                        global_valid += file_valid

                        print(f"  √ 检查完成: {file} (共 {file_total} 条 | 合格 {file_valid} | 不合格 {file_invalid})")

                    except Exception as e:
                        print(f"  × 读取失败 {file}: {e}")

    # 5. 输出最终检查结果与占比
    print("\n" + "=" * 50)
    print("🎉 批量检查任务全部完成！")
    
    if global_total > 0:
        valid_ratio = (global_valid / global_total) * 100
        invalid_ratio = 100 - valid_ratio
        
        print(f"📊 最终质检汇总报告：")
        print(f"   - 扫描抽检总记录数: {global_total} 条")
        print(f"   - ✅ 格式合格号码数: {global_valid} 条  (占比 {valid_ratio:.2f}%)")
        print(f"   - ❌ 格式异常号码数: {global_total - global_valid} 条  (占比 {invalid_ratio:.2f}%)")
        
        if invalid_ratio > 5:
            print("\n   [!] 提示：不合格率较高，可能混入了大量固话、座机或截取错误的数据，建议复查源数据！")
    else:
        print("📊 汇总统计：未扫描到任何有效的号码数据。")
    print("=" * 50)


if __name__ == "__main__":
    validate_mobile_numbers()


# ==============================================================================
# 【调用示例与运行说明】
# ==============================================================================
#
# 1. 运行方式：
#    python3 check_csv_phones.py
#
# 2. 交互过程示例：
#    👉 请输入/拖入要检查的根目录路径 (支持多个，每次输入一个后按回车，直接按回车结束输入):
#       -> 第 1 个路径: F:\00秒话单\001-AAAA-112.25.240.74-0秒\e_cdr_20260401
#       -> 第 2 个路径: [直接按回车键结束输入]
#
#    ==================================================
#    🚀 开始执行批量检查任务
#    ==================================================
#
#    ▶️ 正在扫描第 1/1 个路径: F:\00秒话单\001-AAAA-112.25.240.74-0秒\e_cdr_20260401
#      √ 检查完成: 1-AAAA-0-04.08.csv (共 50000 条 | 合格 49998 | 不合格 2)
#      √ 检查完成: 2-AAAA-0-04.08.csv (共 50000 条 | 合格 50000 | 不合格 0)
#
#    ==================================================
#    🎉 批量检查任务全部完成！
#    📊 最终质检汇总报告：
#       - 扫描抽检总记录数: 100000 条
#       - ✅ 格式合格号码数: 99998 条  (占比 99.99%)
#       - ❌ 格式异常号码数: 2 条  (占比 0.01%)
#    ==================================================