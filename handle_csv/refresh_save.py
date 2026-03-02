# 该脚本用于通过 WPS 或 Excel 刷新指定目录下的所有 CSV 文件。
# 它会自动打开每个文件，保存并关闭，确保文件内容是最新的。

import os
import glob
import win32com.client as win32

def refresh_csv_via_wps(folder_path):
    # 处理路径
    folder_path = folder_path.strip().replace('"', '')
    search_path = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(search_path)

    if not csv_files:
        print(f"未找到CSV文件: {folder_path}")
        return

    # 尝试连接 WPS (ET 是 WPS 表格的名称)，如果没有 WPS 则尝试 Excel
    try:
        app = win32.gencache.EnsureDispatch('Et.Application') # WPS表格
    except:
        try:
            app = win32.gencache.EnsureDispatch('Excel.Application') # Excel
        except Exception as e:
            print(f"错误：未发现安装 WPS 或 Excel! {e}")
            return

    # 设置软件不可见 (设为 True 可以看它自动操作的过程，但会慢一点)
    app.Visible = False 
    app.DisplayAlerts = False # 禁用所有弹窗（确定保存、格式警告等）

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
    print("\n所有文件已真实保存并刷新。")

if __name__ == "__main__":
    while True:
        path_input = input("\n请拖入文件夹并按回车 (输入 exit 退出): ")
        if path_input.lower() == 'exit':
            break
        refresh_csv_via_wps(path_input)