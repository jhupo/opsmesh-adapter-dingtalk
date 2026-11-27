# 04. 互动卡片回调与 AI 流式打字机卡片设计

本文档定义钉钉 **互动卡片按钮交互回调（`/v1.0/card/instances/callback`）** 与 **AI 流式打字机卡片（`createAndDeliver` + `/v1.0/card/streaming`）** 的完整协同设计。

---

## 1. 核心痛点与架构对策

1. **痛点 1：忘记声明 `callbackType="STREAM"` 导致点按钮无响应**
   - 钉钉投放卡片接口默认 `callbackType="HTTP"`。适配器在 `create_and_deliver_card()` 中**默认强制写入 `"callbackType": "STREAM"`**，并在握手时默认订阅 `/v1.0/card/instances/callback`。
2. **痛点 2：前端按钮约 3 秒等待 ACK 超时报错 vs 后端长耗时业务冲突**
   - 用户点击卡片按钮后，钉钉客户端转圈等待 WebSocket `AckMessage` 仅约 **3 秒**：
     - 短业务（如“批准 / 拒绝”）希望直接随 `AckMessage` 带回新 `cardParamMap`，省去一次额外 HTTP 调用；
     - 长业务（如“执行生产部署”，耗时 30 秒）若等跑完再回 ACK，客户端在第 3 秒就会弹出“请求超时”错误提示。
   - **双模式抢占方案**：适配器为每个 `CardCallbackEvent` 建立 `anyio.Event` 信号槽并启动 **2.0 秒超时守护（`dingtalk_card_ack_timeout`）**：
     - 若插件在 2.0 秒内调用 `event.reply_card(...)` 或处理完毕 → **快路径**：直接把更新参数装入 WebSocket `AckMessage` 同步带回钉钉；
     - 若插件调用 `await event.ack_card(...)` 提前释放，或处理耗时满 2.0 秒仍未结束 → **慢路径**：自动先把过渡态（或空响应）`AckMessage` 发回钉钉收起前端转圈，后台业务协程继续无损运行，完成后调用 `await gateway.update_card(...)`（`PUT /v1.0/card/instances`）完成最终状态更新。
3. **痛点 3：大模型 Token 逐字输出触发钉钉流式接口高频限流与异常卡死**
   - LLM 流式接口每秒可能产生数十个 chunk；若逐个调用 `PUT /v1.0/card/streaming` 极易触发钉钉接口限流，且一旦业务抛异常未发 `isFinalize=true`，卡片会永久停留在“正在输入...”加载态。
   - **`ai_card_stream` 上下文管理器方案**：内置最小刷新间隔节流缓冲（默认 `150ms` 合并一次增量文本），并在 `__aexit__` 中保证：**正常退出必发 `isFinalize=true`，异常退出必发 `isError=true` 收尾**。

---

## 2. 时序图 6：互动卡片按钮回调快慢双路径时序

```mermaid
sequenceDiagram
    autonumber
    participant USER as 钉钉客户端用户
    participant WSS as 钉钉 Stream WSS
    participant ADP as dingtalk.Adapter
    participant EVT as CardCallbackEvent
    participant PLG as 业务插件 Matcher
    participant API as 钉钉 OpenAPI

    USER->>WSS: 点击卡片交互按钮 (如 "批准" 或 "执行发布")
    WSS->>ADP: CALLBACK (/v1.0/card/instances/callback, messageId="cb_01", outTrackId="card_88")
    ADP->>EVT: 解析 out_track_id / user_id / action_ids / cardPrivateData.params
    ADP->>PLG: 并发启动 handle_event(gateway, event) 并开启 2.0s ACK 等待窗

    alt 场景 A：秒级短业务（2.0s 内完成或调用 reply_card）
        PLG->>EVT: event.reply_card(card_param_map={"status": "已批准"}, user_private_map={"tip": "操作成功"})
        EVT-->>ADP: 唤醒 ACK 信号槽
        ADP->>WSS: AckMessage(200, messageId="cb_01", response={cardData, userPrivateData})
        WSS-->>USER: 按钮瞬间停止转圈并局部刷新为“已批准”（0 额外 OpenAPI 请求）

    else 场景 B：长耗时业务（显式 ack_card 或超过 2.0s 自动触发）
        opt 插件主动返回过渡态
            PLG->>EVT: await event.ack_card(card_param_map={"status": "发布执行中...", "btn_disabled": "true"})
        end
        Note over ADP,WSS: 主动唤醒或满 2.0s 窗口到期，立即回传 ACK 释放客户端转圈
        ADP->>WSS: AckMessage(200, messageId="cb_01", response={过渡态或空响应})
        WSS-->>USER: 前端按钮正常收起转圈（不触发 3s 超时红字报错）
        Note over PLG: 后台协程继续执行 30s 部署任务...
        PLG->>API: gateway.update_card(out_track_id="card_88", card_param_map={"status": "发布成功"}) (PUT /v1.0/card/instances)
        API-->>USER: 卡片异步刷新为“发布成功”
    end
```

---

## 3. 卡片回调 2.0 秒抢占协调逻辑（`adapter.py` & `event.py`）

```mermaid
flowchart TD
    FRAME["收到 /v1.0/card/instances/callback 帧"] --> DEDUP{"messageId 幂等检查"}
    DEDUP -- "重复帧" --> ACK_EMPTY["直接回空 AckMessage(200) 并结束"]
    DEDUP -- "首次帧" --> SPAWN["构造 CardCallbackEvent (_ack_ready = anyio.Event)"]
    SPAWN --> BG["task_group.start_soon(_run_and_signal, gateway, event)"]
    BG --> WAIT["with anyio.move_on_after(2.0): await event._ack_ready.wait()"]
    WAIT --> SEND_ACK["读取 event._ack_payload 构造 AckMessage(200) 发回 WSS (加 _ack_sent 标记)"]
    SEND_ACK --> CONT["后台 handle_event 若尚未结束则继续在 task_group 中运行至完成"]
```

---

## 4. 时序图 7：AI 流式打字机卡片全生命周期（`gateway.ai_card_stream`）

```mermaid
sequenceDiagram
    autonumber
    participant PLG as 业务插件 (LLM Agent)
    participant CTX as gateway.ai_card_stream
    participant API as 钉钉 OpenAPI (/v1.0/card/*)
    participant USER as 钉钉客户端卡片视图

    PLG->>CTX: async with gateway.ai_card_stream(event, template_id, key="content", initial_text="思考中...") as stream
    CTX->>CTX: 生成唯一 out_track_id (UUIDv4) 并识别目标单聊/群聊 openSpaceId
    CTX->>API: POST /v1.0/card/instances/createAndDeliver (callbackType="STREAM", cardParamMap={"content": "思考中..."})
    API-->>USER: 会话窗投放出初始 AI 卡片

    loop 大模型流式输出 Token
        PLG->>CTX: await stream.append(token_chunk)
        CTX->>CTX: 累积全文 buffer，检查距上次推送是否 >= throttle_interval (150ms)
        opt 达到节流间隔
            CTX->>API: PUT /v1.0/card/streaming {outTrackId, guid, key: "content", content: buffer, isFull: true, isFinalize: false}
            API-->>USER: 卡片打字机平滑刷新内容
        end
    end

    alt 正常退出 async with 代码块
        CTX->>API: PUT /v1.0/card/streaming {outTrackId, guid, key: "content", content: final_buffer, isFull: true, isFinalize: true}
        API-->>USER: 结束流式加载动画，固化最终 Markdown 内容
    else 代码块内抛出未捕获异常
        CTX->>API: PUT /v1.0/card/streaming {outTrackId, guid, key: "content", content: error_fallback, isFull: true, isFinalize: true, isError: true}
        API-->>USER: 卡片切换为错误终态（绝不永久卡在“思考中...”）
        CTX-->>PLG: 重新向上抛出原始异常
    end
```

---

## 5. 卡片接口空间参数（`openSpaceId`）自动推导规则

钉钉新版卡片投放接口 `POST /v1.0/card/instances/createAndDeliver` 使用统一的 `openSpaceId` 语法，适配器在 `gateway.py` 中自动根据 `event` 或入参拼接，免去业务侧记忆复杂格式：

| 投放目标场景 | `openSpaceId` 自动构造格式 | 附带投放场域字段 |
| :--- | :--- | :--- |
| **群聊会话**（`GroupMessageEvent` 或传 `open_conversation_id`） | `dtv1.card//IM_GROUP.{open_conversation_id}` | `imGroupOpenSpaceModel` + `imGroupOpenDeliverModel: {"robotCode": ...}` |
| **单聊会话**（`PrivateMessageEvent` 或传 `user_id`） | `dtv1.card//IM_ROBOT.{user_id}` | `imRobotOpenSpaceModel` + `imRobotOpenDeliverModel: {"spaceType": "IM_ROBOT"}` |
