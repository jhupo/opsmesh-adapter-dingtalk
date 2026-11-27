# 安全策略（Security Policy）

## 支持版本

| 版本 | 支持状态 |
| :--- | :--- |
| `0.1.x` | 支持中 |

## 安全设计基线

- **凭证保护**：所有 `client_secret` 使用 `pydantic.SecretStr` 存储，禁止在日志、异常或 `repr` 中输出明文。
- **附件下载防 SSRF**：换取的 `downloadUrl` 逐跳强制校验 `https` 协议及受控域名白名单（`dingtalk.com`、`alicdn.com`、`aliyuncs.com`、`ddurl.cn`），跨域跳转不转发钉钉 `access_token`。
- **路径穿越防护**：落地文件名统一经 `Path(name).name` 净化并写入指定目录内的独立 `.part` 临时文件。

## 漏洞上报

请勿在公开 Issue 中披露未修复的安全漏洞，请通过 GitHub Security Advisories 私密上报。
