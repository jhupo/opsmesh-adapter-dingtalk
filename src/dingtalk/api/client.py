"""Async HTTP wrapper over Adapter.request() for api.dingtalk.com and oapi."""

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from opsmesh.drivers import Request, Response

from dingtalk.config import Config
from dingtalk.exception import DingTalkActionFailed, DingTalkNetworkError
from dingtalk.typing import T_JsonDict

if TYPE_CHECKING:
    from opsmesh.adapters import Adapter as BaseAdapter


def _decode_json_body(response: Response) -> T_JsonDict:
    if not response.content:
        return {}
    text = (
        response.content.decode("utf-8")
        if isinstance(response.content, bytes)
        else response.content
    )
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}
    return parsed if isinstance(parsed, dict) else {"result": parsed}


def _check_oapi_errcode(body: T_JsonDict, status_code: int, *, label: str) -> None:
    errcode = body.get("errcode")
    if errcode is not None and errcode != 0:
        raise DingTalkActionFailed(
            str(body.get("errmsg") or f"{label} error {errcode}"),
            status_code=status_code,
            code=errcode,
            request_id=body.get("request_id"),
        )


class ApiClient:
    """Thin async HTTP client delegating all network I/O to OpsMesh Adapter."""

    def __init__(self, adapter: "BaseAdapter", config: Config) -> None:
        self._adapter = adapter
        self._config = config

    @property
    def adapter(self) -> "BaseAdapter":
        return self._adapter

    @property
    def config(self) -> Config:
        return self._config

    async def _send_json(
        self,
        request: Request,
        *,
        label: str,
        msg_field: str = "errmsg",
        code_field: str = "errcode",
    ) -> tuple[int, T_JsonDict]:
        try:
            response = await self._adapter.request(request)
        except Exception as exc:
            raise DingTalkNetworkError(f"{label} request failed: {exc}") from exc

        body = _decode_json_body(response)
        if not (200 <= response.status_code < 300):
            raise DingTalkActionFailed(
                str(body.get(msg_field) or f"{label} HTTP {response.status_code}"),
                status_code=response.status_code,
                code=body.get(code_field),
                request_id=body.get("requestid") or body.get("requestId"),
            )
        return response.status_code, body

    async def request_openapi(
        self,
        method: str,
        path: str,
        *,
        access_token: str | None = None,
        params: dict[str, Any] | None = None,
        json_body: T_JsonDict | None = None,
    ) -> T_JsonDict:
        """Call new-generation DingTalk OpenAPI (https://api.dingtalk.com)."""
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if access_token:
            headers["x-acs-dingtalk-access-token"] = access_token

        url = f"{self._config.dingtalk_api_base.rstrip('/')}{path}"
        request = Request(
            method,
            url,
            params=params,
            headers=headers,
            json=json_body,
            timeout=self._config.dingtalk_request_timeout,
        )
        _, body = await self._send_json(
            request, label="OpenAPI", msg_field="message", code_field="code"
        )
        return body

    async def request_oapi(
        self,
        method: str,
        path: str,
        *,
        access_token: str | None = None,
        params: dict[str, Any] | None = None,
        json_body: T_JsonDict | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | list[tuple[str, Any]] | None = None,
    ) -> T_JsonDict:
        """Call legacy DingTalk OAPI (https://oapi.dingtalk.com)."""
        query = dict(params or {})
        if access_token:
            query["access_token"] = access_token

        url = f"{self._config.dingtalk_oapi_base.rstrip('/')}{path}"
        request = Request(
            method,
            url,
            params=query or None,
            json=json_body,
            data=data,
            files=files,
            timeout=self._config.dingtalk_request_timeout,
        )
        status, body = await self._send_json(request, label="OAPI")
        _check_oapi_errcode(body, status, label="OAPI")
        return body

    async def post_webhook(self, webhook_url: str, payload: T_JsonDict) -> T_JsonDict:
        """POST a reply to a DingTalk sessionWebhook URL after host validation."""
        parsed = urlsplit(webhook_url)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "https" or not (
            host == "dingtalk.com" or host.endswith(".dingtalk.com")
        ):
            raise DingTalkActionFailed(
                f"Refused non-DingTalk sessionWebhook URL: {webhook_url!r}"
            )

        request = Request(
            "POST",
            webhook_url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=self._config.dingtalk_request_timeout,
        )
        status, body = await self._send_json(request, label="Webhook")
        _check_oapi_errcode(body, status, label="Webhook")
        return body


__all__ = ["ApiClient"]
