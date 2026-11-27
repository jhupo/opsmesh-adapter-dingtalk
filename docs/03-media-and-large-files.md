# 03. 多模态富媒体与 GB 级大文件收发专项设计

本文档定义图片、普通文件、语音、视频的入站流式拉取与出站上传投递方案，重点解决 **钉钉单聊最高 4GB 大文件恒定内存流式下载、`Range` 断点续传、OSS 签名链接安全校验** 以及 **出站 20MB 分界线双通道投递**。

---

## 1. 入站与出站能力边界对照

钉钉开放平台对媒体收发采用不对称架构，适配器必须分别针对性设计：

| 方向 | 钉钉底层机制 | 平台体积上限 | 适配器策略 |
| :--- | :--- | :--- | :--- |
| **入站下载** | `POST /v1.0/robot/messageFiles/download` 将 `downloadCode` 换为**阿里云 OSS 临时签名直链** | 随钉钉客户端上限（最高 **4 GB**） | 默认不设体积封顶（`max_download_bytes=None`），**流式直写 `.part` 临时文件 + 读空闲超时 + `Range` 续传**，内存占用恒定 < 1 MB |
| **出站常规媒体** | `POST https://oapi.dingtalk.com/media/upload` 换取 `media_id` 后发 `sample*` 模板消息 | 图片/视频/文件 **≤ 20 MB**，语音 **≤ 2 MB** | 本地预检文件大小与格式头，自动上传换 `media_id` 并按单聊/群聊投递原生气泡 |
| **出站超大文件** | 群钉盘空间 `convFile` + 存储 `uploadInfos` 获取 OSS 凭证直传 `PUT` | 突破 20 MB（GB 级） | 提供 `send_dingpan_file()` 流式 `PUT` 直传群钉盘并投递原生群文件卡片；无钉盘权限时抛 `MediaLimitExceeded` 或引导发送预签名下载卡片 |

---

## 2. 入站三层媒体读取接口设计

为防止业务插件在处理几百 MB 日志包或视频时误将文件读入内存导致 OOM，`Gateway` 明确划分三个层次的读取接口：

| 接口签名 | 返回值 | 内存特征 | 适用场景 |
| :--- | :--- | :--- | :--- |
| `await gateway.download_media(target, dest_dir=None, filename=None, max_bytes=None)` | `DownloadedMedia(path, name, size, mime)` | **恒定 O(1)**（仅驻留单块 256 KiB 缓冲） | **默认推荐**：落盘处理文件、归档、解压、离线分析 |
| `gateway.iter_media(target, chunk_size=262144)` | `AsyncIterator[bytes]` | **恒定 O(1)** | 在线流式计算哈希（SHA256）、流式管道转传至自有 S3/OSS |
| `await gateway.read_media_bytes(target, max_bytes=33554432)` | `bytes` | **O(N)**（默认强制 32 MB 内存保护上限） | 仅用于小图片/小配置文件直传多模态大模型 API |

> `target` 参数既支持直接传入入站 `MessageSegment`（`image` / `file` / `audio` / `video`），也支持直接传入裸 `download_code: str`。

---

## 3. 时序图 4：GB 级大文件流式下载与 `Range` 断点续传全流程

```mermaid
sequenceDiagram
    autonumber
    participant PLG as 业务插件
    participant GW as dingtalk.Gateway
    participant MED as dingtalk.media
    participant API as 钉钉 OpenAPI
    participant OSS as 阿里云 OSS 签名节点
    participant DISK as 本地磁盘 (.part -> 正式文件)

    PLG->>GW: download_media(file_segment, dest_dir="/data/inbox")
    GW->>GW: get_access_token()
    GW->>MED: exchange_and_stream_to_disk(token, robot_code, seg)

    MED->>API: POST /v1.0/robot/messageFiles/download {downloadCode, robotCode}
    API-->>MED: 200 OK {downloadUrl: "http://xxx.oss-cn-hangzhou.aliyuncs.com/..."}

    Note over MED: 无损将开头 http 升级为 https (不重编码 path/query 保签名) · Path.name 净化文件名 · 创建 .part 临时文件

    loop 最多 3 跳安全重定向校验 (不携带钉钉 access_token)
        MED->>MED: 校验 https + 端口 443 + 域名后缀属于白名单
        MED->>OSS: stream_request(GET, Timeout(total=None, connect=10s, read=60s), auto_redirects=False)
        alt 301/302/307 重定向
            OSS-->>MED: Redirect Location
        else 200 OK 开始流式传输
            loop 每次读取 256 KiB chunk (60s 读空闲保活，无总时长封顶)
                OSS-->>MED: chunk bytes
                MED->>DISK: async write chunk & offset += len(chunk)
            end
        end
    end

    opt 下载中途偶发网络闪断 (offset > 0 且重试次数 <= 2)
        MED->>OSS: stream_request(GET, headers={"Range": "bytes={offset}-"})
        alt OSS 返回 206 Partial Content
            OSS-->>MED: 剩余字节流
            MED->>DISK: 追加写入 .part 直至完成
        else OSS 返回 200 OK (不支持分段)
            MED->>DISK: 截断归零重新写入 .part
        end
    end

    MED->>MED: 若文件名无后缀或为 .file，按段元信息/Content-Type 补全扩展名
    MED->>DISK: 原子重命名 xxx.zip.part -> xxx.zip
    MED-->>GW: DownloadedMedia(path, name, size, mime)
    GW-->>PLG: 返回 DownloadedMedia
```

---

## 4. 入站下载安全防护决策流

```mermaid
flowchart TD
    URL_IN["拿到 downloadUrl"] --> UPGRADE["若匹配 http://*.oss-*.aliyuncs.com 则无损升级为 https://"]
    UPGRADE --> HOP{"校验当前跳 URL: https · 无账密 · 端口 443 · 域名命中白名单后缀"}
    HOP -- "任一不满足" --> REJECT["立即阻断并抛出 MediaError (防 SSRF)"]
    HOP -- "通过" --> REQ["发起不带凭证、禁自动跳转的 stream_request"]
    REQ --> STATUS{"响应状态码"}
    STATUS -- "301/302/303/307/308" --> HOP_CNT{"跳转次数 < 3 且含 Location ?"}
    HOP_CNT -- "是" --> NEXT_URL["解析下一跳绝对 URL"] --> HOP
    HOP_CNT -- "否" --> REJECT
    STATUS -- "200 / 206" --> WRITE["循环写入受控目录下 .part 文件"]
    WRITE --> LIMIT{"设置过 max_bytes 且 written > max_bytes ?"}
    LIMIT -- "是" --> CLEAN_LIMIT["关闭流 + 删除 .part 残片 + 抛 MediaLimitExceeded"]
    LIMIT -- "否" --> FINISH["流结束 -> 补全后缀 -> 原子 replace -> 返回 DownloadedMedia"]
```

---

## 5. 时序图 5：出站文件双通道发送（≤ 20MB 标准通道 vs > 20MB 钉盘直传）

```mermaid
sequenceDiagram
    autonumber
    participant PLG as 业务插件
    participant GW as dingtalk.Gateway
    participant MED as dingtalk.media
    participant OAPI as 钉钉 OAPI (media/upload)
    participant API as 钉钉 OpenAPI (robot / convFile / storage)
    participant OSS as 钉钉云盘 OSS

    PLG->>GW: send(event, MessageSegment.file(path="archive.zip"))
    GW->>MED: 检查本地文件大小 stat().st_size 与扩展名

    alt 文件大小 <= 20 MB (标准机器人媒体通道，单聊/群聊通用)
        GW->>GW: get_access_token()
        MED->>OAPI: POST /media/upload?access_token=... (multipart type=file)
        OAPI-->>MED: {errcode: 0, media_id: "@lAzP..."}
        alt 群聊会话
            GW->>API: POST /v1.0/robot/groupMessages/send (msgKey="sampleFile", msgParam='{"mediaId":...,"fileName":...,"fileType":"zip"}')
        else 单聊会话
            GW->>API: POST /v1.0/robot/oToMessages/batchSend (userIds=[staff_id], msgKey="sampleFile", ...)
        end
        API-->>PLG: 发送成功（原生文件气泡）

    else 文件大小 > 20 MB 且目标为群聊并具备操作人 union_id (钉盘大文件直传通道)
        GW->>API: POST /v1.0/convFile/conversations/spaces/query?unionId=... {openConversationId}
        API-->>GW: {space: {spaceId: "sp_01"}}
        GW->>API: POST /v2.0/storage/spaces/sp_01/files/0/uploadInfos/query?unionId=... {protocol: "HEADER_SIGNATURE", ...}
        API-->>GW: {uploadKey, headerSignatureInfo: {resourceUrls: [...], headers: {...}}}
        MED->>OSS: 流式 PUT 本地大文件字节流至 resourceUrls[0] (恒定低内存)
        OSS-->>MED: 200 OK
        GW->>API: POST /v2.0/storage/spaces/sp_01/files/0/commit?unionId=... {uploadKey, name}
        API-->>GW: {dentry: {id: "den_99", spaceId: "sp_01"}}
        GW->>API: POST /v1.0/convFile/conversations/files/send?unionId=... {spaceId: "sp_01", dentryId: "den_99", openConversationId}
        API-->>PLG: 发送成功（群内原生钉盘大文件气泡）

    else 文件大小 > 20 MB 且未开启钉盘通道
        MED-->>PLG: 本地立即抛出 MediaLimitExceeded (零无效上传流量)
    end
```

---

## 6. 出站 OpenAPI 消息模板（`msgKey` / `msgParam`）精确映射表

注意钉钉机器人 OpenAPI 要求顶层字段 **`msgParam` 必须是序列化后的 JSON 字符串**：

| 段类型 | `msgKey` | `msgParam` 内层 JSON 结构 | 本地预检约束 |
| :--- | :--- | :--- | :--- |
| `text` | `sampleText` | `{"content": "..."}` | 非空文本 |
| `markdown` | `sampleMarkdown` | `{"title": "...", "text": "..."}` | `title` 供会话列表首屏预览 |
| `image` | `sampleImageMsg` | `{"photoURL": "<media_id 或公网 https URL>"}` | 本地上传 ≤ 20MB，支持 PNG / JPEG / GIF / BMP |
| `file` | `sampleFile` | `{"mediaId": "<media_id>", "fileName": "a.pdf", "fileType": "pdf"}` | 本地上传 ≤ 20MB，必须具备非空 ASCII 扩展名 `fileType` |
| `audio` | `sampleAudio` | `{"mediaId": "<media_id>", "duration": "4000"}` | 本地上传 ≤ 2MB，必须提供毫秒数字符串 `duration` |
| `video` | `sampleVideo` | `{"videoMediaId": "<vid>", "videoType": "mp4", "duration": "12000", "picMediaId": "<pic_id>"}` | 视频 ≤ 20MB MP4，需同时上传封面图换取 `picMediaId` |
| `action_card` | `sampleActionCard` | `{"title": "...", "text": "...","singleTitle": "...", "singleURL": "..."}` | 支持单按钮跳转（复杂多按钮交互统一走互动卡片接口） |
