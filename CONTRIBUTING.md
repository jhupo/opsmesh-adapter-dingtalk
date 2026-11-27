# 贡献指南（Contributing）

感谢关注并参与 `opsmesh-adapter-dingtalk` 的开发。

## 开发环境准备

要求 **Python >= 3.11**，需先安装 `OpsMesh` 核心包与开发工具链：

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e "../OpsMesh[httpx,websockets]" -e ".[dev]"
```

## 代码规范与本地检查

提交前请确保以下检查全部通过：

```bash
ruff check src tests examples
ruff format --check src tests examples
bandit -r src/dingtalk -ll -ii
pytest -v
```

## 设计约束

1. **零同步阻塞 I/O**：严禁引入 `requests` 或 `dingtalk-stream` 同步调用，所有 HTTP / 流式下载 / WebSocket 通信必须通过 OpsMesh `Adapter.request()`、`Adapter.stream_request()` 与 `Adapter.websocket()` 完成。
2. **大文件恒定内存**：入站文件下载默认流式落盘（`.part` 临时文件 + 原子重命名），禁止默认将未知大小的附件拼入内存 `bytes`。
3. **单向分层模块依赖**：严格遵守 `L0 (typing/exception/config/utils) -> L1 (message/ · event/) -> L2 (stream/ · api/) -> L3 (gateway · adapter)` 依赖方向，禁止跨层反向导入，禁止绕过 OpsMesh 基类另建平行事件/消息/异常体系。
