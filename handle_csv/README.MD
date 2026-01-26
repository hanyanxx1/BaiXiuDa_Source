既然你已经打开了 VS Code 并且建立了文件夹，接下来我们要做的就是**配置环境、安装工具、放入代码、执行脚本**。

看你目前的进度，请按照以下步骤操作：

---

### 第一步：创建虚拟环境并安装 Pandas

VS Code 本身只是个编辑器，我们需要安装处理 CSV 的工具库 `pandas`。

1. 在 VS Code 底部找到 **Terminal（终端）** 窗口。如果没有看到，按快捷键 `Ctrl + ` ` (数字1左边的键)。
2. 在终端中输入以下命令并回车（这会创建一个独立环境）：
```bash
python -m venv .venv

```


3. **激活环境**：
* **Windows**: 输入 `.venv\Scripts\activate`
* **Mac**: 输入 `source .venv/bin/activate`
* *看到命令行开头多了 `(.venv)` 字样说明成功了。*
