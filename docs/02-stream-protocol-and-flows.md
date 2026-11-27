# 02. Stream 协议状态机与消息收发全链路时序设计

本文档详细定义钉钉 Stream 长连接生命周期、帧级状态流转、入站消息归一化、幂等去重、出站智能双通道选路与用户信息鉴权流程。

---

## 1. Stream 协议握手与帧结构规范

### 1.1 连接开票请求
- **Endpoint**：`POST https://api.dingtalk.com/v1.0/gateway/connections/open`
- **Request JSON**：
  ```json
  {
    "clientId": "dingxxxxxxxx",
    "clientSecret": "<secret>",
    "subscriptions": [
      {"type": "CALLBACK", "topic": "/v1.0/im/bot/messages/get"},
      {"type": "CALLBACK", "topic": "/v1.0/card/instances/callback"}
    ],
    "ua": "opsmesh-adapter-dingtalk/0.1.0"
  }
  ```
- **安全校验**：返回的 `endpoint` 必须满足 `scheme == "wss"`、存在合法主机名且不含明文用户信息，随后拼接 `?ticket={ticket}` 通过 `Adapter.websocket()` 建立连接。

### 1.2 下行帧与上行 `AckMessage` 结构

| 帧 `type` | `headers.topic` | 含义与适配器响应策略 |
| :--- | :--- | :--- |
| `SYSTEM` | `ping` | 应用层探活帧：原样将 `data` 放入 `AckMessage(code=200)` 立即回传 |
| `SYSTEM` | `disconnect` | 钉钉网关灰度发布/负载均衡摘流预告：关闭当前 socket 并 **0 延迟立即重新开票重连** |
| `CALLBACK` | `/v1.0/im/bot/messages/get` | 机器人单聊/群聊消息：**先立即回传 `AckMessage(200)`**，经 `msgId` 幂等校验后投入后台 `TaskGroup` 执行 `handle_event` |
| `CALLBACK` | `/v1.0/card/instances/callback` | 互动卡片点击回调：进入 **2 秒同步抢占窗口**（支持随 ACK 返回 `cardData` 局部刷新，超时先回过渡 ACK 再异步调 OpenAPI） |
| `EVENT` | 具体事件名（订阅 `*` 时） | 企业组织/OA 事件：**先立即回传 `AckMessage(200)`**，经 `eventId` 幂等校验后派发 `PlatformNoticeEvent` |

---

## 2. 连接生命周期状态机

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> OpeningTicket: driver.on_startup
    OpeningTicket --> ConnectingWS: 获取 wss endpoint + ticket
    OpeningTicket --> BackoffWait: HTTP/凭证/网络异常

    ConnectingWS --> Connected: WSS 握手成功 · 调用 gateway.connect()
    ConnectingWS --> BackoffWait: WSS 握手失败

    state Connected {
        [*] --> ReceivingFrame
        ReceivingFrame --> ReceivingFrame: SYSTEM/ping -> 回传 pong ACK
        ReceivingFrame --> ReceivingFrame: CALLBACK/EVENT -> 回传 ACK + 异步分发事件
    }

    Connected --> FastReconnect: 收到 SYSTEM/disconnect 摘流指令 · gateway.disconnect()
    FastReconnect --> OpeningTicket: 0s 延迟立即换新票重连

    Connected --> BackoffWait: Socket 异常断开/超时 · gateway.disconnect()
    BackoffWait --> OpeningTicket: sleep(min(2^n, 30s) + jitter) 且重置成功计数

    OpeningTicket --> Stopped: driver.on_shutdown (CancelScope)
    ConnectingWS --> Stopped: driver.on_shutdown (CancelScope)
    Connected --> Stopped: driver.on_shutdown · 关闭 WS · gateway.disconnect()
    BackoffWait --> Stopped: driver.on_shutdown (CancelScope)
    Stopped --> [*]
```

---

## 3. 时序图 1：连接启动、心跳、灰度摘流无缝重连与优雅关停

```mermaid
sequenceDiagram
    autonumber
    participant DRV as OpsMesh Driver
    participant ADP as dingtalk.Adapter
    participant GW as dingtalk.Gateway
    participant API as 钉钉 OpenAPI
    participant WSS as 钉钉 Stream WSS

    DRV->>ADP: on_startup() 启动各机器人保活任务
    ADP->>API: POST /v1.0/gateway/connections/open (clientId, secret, subscriptions)
    API-->>ADP: 200 OK {endpoint, ticket}
    ADP->>WSS: WebSocket Connect wss://endpoint?ticket=...
    WSS-->>ADP: 101 Switching Protocols
    ADP->>GW: gateway.connect() (触发 @Gateway.on_connect)

    loop 长连接帧接收循环
        alt 应用层心跳 SYSTEM/ping
            WSS->>ADP: {type: "SYSTEM", headers: {topic: "ping", messageId: "m1"}, data: "..."}
            ADP->>WSS: {code: 200, headers: {messageId: "m1"}, message: "OK", data: "..."}
        else 钉钉服务端灰度摘流 SYSTEM/disconnect
            WSS->>ADP: {type: "SYSTEM", headers: {topic: "disconnect"}}
            ADP->>GW: gateway.disconnect() (触发 @Gateway.on_disconnect)
            ADP->>WSS: Close WebSocket (1000)
            Note over ADP,API: 0 延迟立即重新开票建立新连接
            ADP->>API: POST /v1.0/gateway/connections/open
            API-->>ADP: 200 OK {new_endpoint, new_ticket}
            ADP->>WSS: WebSocket Connect (new_ticket)
            ADP->>GW: gateway.connect()
        end
    end

    DRV->>ADP: on_shutdown() 取消保活 CancelScope
    ADP->>GW: gateway.disconnect()
    ADP->>WSS: Close WebSocket (1000)
```

---

## 4. 时序图 2：入站机器人消息异步处理与幂等去重

钉钉 Stream 若在数秒内未收到 WebSocket `AckMessage` 会触发重投。为防止耗时业务（如执行自动化脚本、调用大模型）阻塞读取协程或导致重复执行，适配器严格采用 **“先立即 ACK + 内存环形去重 + 后台并发执行”** 管线：

```mermaid
sequenceDiagram
    autonumber
    participant WSS as 钉钉 Stream WSS
    participant ADP as dingtalk.Adapter
    participant DED as DedupeRing (LRU+TTL)
    participant PAR as Event/Message Parser
    participant CORE as OpsMesh handle_event()
    participant PLG as 业务插件 Matcher

    WSS->>ADP: CALLBACK (/v1.0/im/bot/messages/get, messageId="f_01", data={msgId: "msg_99", ...})
    ADP->>WSS: 立即回传 AckMessage(code=200, messageId="f_01", data='{"response":null}')

    ADP->>DED: seen_or_record("f_01" / "msg_99")
    alt 已在 TTL (600s) 窗口内处理过
        DED-->>ADP: True (重复投递帧)
        Note over ADP: 记录 debug 日志并直接丢弃，防止重复触发命令
    else 首次到达
        DED-->>ADP: False (登记时间戳)
        ADP->>PAR: parse_chatbot_callback(data)
        Note over PAR: 识别单聊与群聊 · 拆解多模态段 · 剥离群 @ 前导空格 · 附件仅存 downloadCode 元信息
        PAR-->>ADP: PrivateMessageEvent / GroupMessageEvent
        ADP->>CORE: driver.task_group.start_soon(handle_event, gateway, event)
        CORE->>PLG: Permission -> Rule (to_me / command) -> Depends -> Handler
    end
```

---

## 5. 入站消息归一化流程（`message.py` & `event.py`）

```mermaid
flowchart TD
    RAW["收到 Chatbot JSON data"] --> CONV{"conversationType == '1' ?"}
    CONV -- "是 (单聊)" --> PRIV["构造 PrivateMessageEvent · is_tome() = True"]
    CONV -- "否 (群聊 '2')" --> GRP["构造 GroupMessageEvent · is_tome() = isInAtList or bot in atUsers"]

    PRIV --> TYPE{"判断 msgtype"}
    GRP --> TYPE

    TYPE -- "text" --> T_SEG["提取 text.content · 群聊 lstrip 前导空白 -> MessageSegment.text"]
    TYPE -- "picture" --> P_SEG["提取 downloadCode 或 pictureDownloadCode -> MessageSegment.image"]
    TYPE -- "richText" --> R_SEG["按序遍历 content.richText 数组 -> 依次生成 text 与 image 段 (首文本段群聊 lstrip)"]
    TYPE -- "file" --> F_SEG["提取 downloadCode + fileName + fileSize -> MessageSegment.file"]
    TYPE -- "audio" --> A_SEG["提取 downloadCode + duration + recognition -> MessageSegment.audio"]
    TYPE -- "video" --> V_SEG["提取 downloadCode + duration + videoType -> MessageSegment.video"]
    TYPE -- "其他未知类型" --> U_SEG["保底生成 MessageSegment('unknown', raw) 确保 message[0] 永不越界"]

    T_SEG --> UID["确定 get_user_id(): senderStaffId 优先，缺失时回退加密 senderId"]
    P_SEG --> UID
    R_SEG --> UID
    F_SEG --> UID
    A_SEG --> UID
    V_SEG --> UID
    U_SEG --> UID
```

---

## 6. 出站消息智能双通道选路流程（`Gateway.send`）

```mermaid
flowchart TD
    IN["Gateway.send(event, message, at_sender=False)"] --> NORM["标准化为 dingtalk.Message (段序列)"]
    NORM --> CAN_WH{"满足 sessionWebhook 快速通道? (MessageEvent · 距过期 > 5s · 仅含文本/Markdown/按钮卡片)"}

    CAN_WH -- "是" --> PACK_WH["组装 Webhook JSON (合并 text 与 at / 渲染 markdown 或 actionCard)"]
    PACK_WH --> POST_WH["POST event.session_webhook"]
    POST_WH --> WH_OK{"HTTP 200 且 errcode == 0 ?"}
    WH_OK -- "是" --> DONE["返回发送回执"]
    WH_OK -- "否 (会话失效或限流)" --> FALLBACK["自动降级切换至 OpenAPI 通道"]

    CAN_WH -- "否" --> FALLBACK
    FALLBACK --> SPLIT["按段类型拆分出站批次 (连续 text 合并为单条 sampleText，每个媒体/卡片独立一条)"]
    SPLIT --> LOOP["按序遍历每条待发单元"]
    LOOP --> NEED_UP{"是否为本地 path / bytes 媒体且无 media_id ?"}
    NEED_UP -- "是 (≤20MB 素材 / ≤2MB 语音)" --> UPLOAD["获取 access_token -> POST oapi /media/upload 换取 media_id"]
    NEED_UP -- "是 (>20MB 文件)" --> BIG{"已启用钉盘且有 union_id ?"}
    BIG -- "是" --> DINGPAN["走 send_dingpan_file (OSS 流式直传投递群文件)"]
    BIG -- "否" --> ERR["抛出 MediaLimitExceeded"]
    NEED_UP -- "否 (纯文本/Markdown/已有 media_id 或 URL)" --> SEND_API

    UPLOAD --> SEND_API{"判断目标会话类型"}
    SEND_API -- "群聊 (openConversationId)" --> GRP_API["POST /v1.0/robot/groupMessages/send"]
    SEND_API -- "单聊 (senderStaffId)" --> OTO_API["POST /v1.0/robot/oToMessages/batchSend"]
    GRP_API --> DONE
    OTO_API --> DONE
    DINGPAN --> DONE
```

---

## 7. 时序图 3：`access_token` 双检锁刷新与员工通讯录 RBAC 鉴权

```mermaid
sequenceDiagram
    autonumber
    participant PLG as 业务插件 Handler / Depends
    participant GW as dingtalk.Gateway
    participant LOCK as anyio.Lock (_token_lock)
    participant U_CACHE as UserProfile TTL Cache
    participant API as 钉钉 OpenAPI / OAPI

    PLG->>GW: get_user_profile(staff_id="014728")
    GW->>U_CACHE: 查询缓存 ("014728")
    alt 命中 5 分钟 TTL 缓存
        U_CACHE-->>GW: UserProfile (name, job_number, dept_id_list, roles, unionid)
        GW-->>PLG: 返回缓存的 UserProfile (0 网络请求)
    else 缓存未命中或已过期
        GW->>GW: get_access_token()
        alt Token 有效 (monotonic() < expires_at - 300s)
            Note over GW: 无锁快路径直接返回内存 Token
        else Token 即将过期或为空
            GW->>LOCK: async with _token_lock
            Note over GW,LOCK: 二次检查防止并发协程重复刷新
            GW->>API: POST /v1.0/oauth2/accessToken {appKey, appSecret}
            API-->>GW: {accessToken, expireIn: 7200}
            GW->>GW: 更新 _access_token 与 _token_expires_at
        end
        GW->>API: POST /topapi/v2/user/get?access_token=... {userid: "014728"}
        API-->>GW: {errcode: 0, result: {name, unionid, job_number, dept_id_list, role_list, ...}}
        GW->>U_CACHE: 写入缓存 (TTL = 300s)
        GW-->>PLG: 返回解析后的 UserProfile
    end
```
