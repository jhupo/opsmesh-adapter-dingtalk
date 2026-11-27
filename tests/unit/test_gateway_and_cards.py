"""Tests for TokenManager lock, Gateway smart send, Contact cache, and Cards."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import json
import time
from typing import Any

import anyio
from opsmesh import on_type
from opsmesh.config import Config as OpsMeshConfig
from opsmesh.config import Env
from opsmesh.drivers import (
    Driver,
    HTTPClientMixin,
    HTTPClientSession,
    Request,
    Response,
    WebSocket,
    WebSocketClientMixin,
)
import pytest

from dingtalk import (
    Adapter,
    BotConfig,
    CardCallbackEvent,
    Config,
    Message,
    MessageSegment,
)
from dingtalk.event import parse_chatbot_event
from dingtalk.stream import parse_stream_frame


class _CaptureWS(WebSocket):
    def __init__(self) -> None:
        super().__init__(request=Request("GET", "wss://wss.dingtalk.com/connect"))
        self.sent: list[str] = []

    @property
    def closed(self) -> bool:
        return False

    async def accept(self) -> None:
        return None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None

    async def receive(self) -> str | bytes:
        return ""

    async def receive_text(self) -> str:
        return ""

    async def receive_bytes(self) -> bytes:
        return b""

    async def send_text(self, data: str) -> None:
        self.sent.append(data)

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data.decode())


class _ApiMockDriver(Driver, HTTPClientMixin, WebSocketClientMixin):
    def __init__(self) -> None:
        super().__init__(Env(), OpsMeshConfig())
        self.requests: list[Request] = []
        self.token_calls = 0
        self.user_calls = 0

    @property
    def type(self) -> str:
        return "api-mock"

    @property
    def logger(self) -> Any:
        return None

    def run(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def request(self, setup: Request) -> Response:
        self.requests.append(setup)
        url = str(setup.url)
        if url.endswith("/v1.0/oauth2/accessToken"):
            self.token_calls += 1
            await anyio.sleep(0.02)
            return Response(
                200,
                content=json.dumps(
                    {"accessToken": "tok_shared", "expireIn": 7200}
                ).encode(),
            )
        if "sendBySession" in url:
            return Response(200, content=json.dumps({"errcode": 0}).encode())
        if "/media/upload" in url:
            return Response(
                200,
                content=json.dumps({"errcode": 0, "media_id": "@m_file"}).encode(),
            )
        if "/robot/groupMessages/send" in url or "/robot/oToMessages/batchSend" in url:
            return Response(200, content=json.dumps({"processQueryKey": "q1"}).encode())
        if "/topapi/v2/user/get" in url:
            self.user_calls += 1
            return Response(
                200,
                content=json.dumps(
                    {
                        "errcode": 0,
                        "result": {
                            "userid": "staff_001",
                            "name": "张三",
                            "unionid": "union_001",
                            "job_number": "E1001",
                            "dept_id_list": [10, 20],
                            "role_list": [{"id": 1, "name": "SRE"}],
                        },
                    }
                ).encode(),
            )
        if "/v1.0/card/instances/createAndDeliver" in url:
            return Response(200, content=json.dumps({"result": {"ok": True}}).encode())
        if "/v1.0/card/instances" in url or "/v1.0/card/streaming" in url:
            return Response(200, content=json.dumps({"success": True}).encode())
        return Response(200, content=b"{}")

    async def stream_request(
        self, setup: Request, *, chunk_size: int = 1024
    ) -> AsyncGenerator[Response, None]:
        yield Response(200, content=b"")

    def get_session(self, *args: Any, **kwargs: Any) -> HTTPClientSession:
        raise NotImplementedError

    @asynccontextmanager
    async def websocket(self, setup: Request) -> AsyncGenerator[WebSocket, None]:
        raise NotImplementedError
        yield  # pragma: no cover


@pytest.mark.anyio
async def test_token_double_checked_lock_and_contact_ttl_cache() -> None:
    driver = _ApiMockDriver()
    cfg = Config(dingtalk_bots=[BotConfig(client_id="ding_c", client_secret="sec_c")])
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_c"]

    tokens: list[str] = []

    async def _fetch() -> None:
        tokens.append(await gw.get_access_token())

    async with anyio.create_task_group() as tg:
        for _ in range(5):
            tg.start_soon(_fetch)

    assert tokens == ["tok_shared"] * 5
    assert driver.token_calls == 1

    # UserProfile cached across repeated calls
    p1 = await gw.get_user_profile("staff_001")
    p2 = await gw.get_user_profile("staff_001")
    assert p1.name == "张三"
    assert p1.roles[0].name == "SRE"
    assert p2.job_number == "E1001"
    assert driver.user_calls == 1


@pytest.mark.anyio
async def test_gateway_send_webhook_vs_openapi_routing() -> None:
    driver = _ApiMockDriver()
    cfg = Config(dingtalk_bots=[BotConfig(client_id="ding_s", client_secret="sec_s")])
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_s"]

    future_ms = int((time.time() + 3600) * 1000)
    ev = parse_chatbot_event(
        {
            "conversationType": "2",
            "conversationId": "cid_ops",
            "senderStaffId": "staff_001",
            "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=abc",
            "sessionWebhookExpiredTime": future_ms,
            "msgtype": "text",
            "text": {"content": "hi"},
        }
    )

    # Pure text uses sessionWebhook fast path
    await gw.send(ev, "ok", at_sender=True)
    assert "sendBySession" in str(driver.requests[-1].url)
    assert driver.requests[-1].json["at"]["atUserIds"] == ["staff_001"]

    # Mixed text + file automatically uploads and dispatches 2 OpenAPI group messages
    driver.requests.clear()
    await gw.send(
        ev,
        Message("请查收") + MessageSegment.file(content=b"abc", name="out.pdf"),
    )
    urls = [str(r.url) for r in driver.requests]
    assert any("/media/upload" in u for u in urls)
    group_calls = [
        r for r in driver.requests if "/robot/groupMessages/send" in str(r.url)
    ]
    assert len(group_calls) == 2
    assert group_calls[0].json["msgKey"] == "sampleText"
    assert group_calls[1].json["msgKey"] == "sampleFile"


@pytest.mark.anyio
async def test_card_callback_sync_reply_and_ai_card_stream() -> None:
    driver = _ApiMockDriver()
    cfg = Config(
        dingtalk_bots=[BotConfig(client_id="ding_k", client_secret="sec_k")],
        dingtalk_card_ack_timeout=0.5,
    )
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_k"]
    ws = _CaptureWS()

    cb_matcher = on_type(CardCallbackEvent)

    @cb_matcher.handle()
    async def _on_card(event: CardCallbackEvent) -> None:
        if event.params.get("action") == "approve":
            event.reply_card(
                card_param_map={"status": "approved"},
                user_private_map={"tip": "done"},
            )

    try:
        async with driver._lifespan:
            frame = parse_stream_frame(
                json.dumps(
                    {
                        "type": "CALLBACK",
                        "headers": {
                            "topic": "/v1.0/card/instances/callback",
                            "messageId": "cb_msg_1",
                        },
                        "data": json.dumps(
                            {
                                "outTrackId": "track_88",
                                "userId": "staff_001",
                                "content": (
                                    '{"cardPrivateData":{"actionIds":["b1"],'
                                    '"params":{"action":"approve"}}}'
                                ),
                            }
                        ),
                    }
                )
            )
            await adapter.handle_stream_frame(gw, frame, ws)
            assert len(ws.sent) == 1
            ack_outer = json.loads(ws.sent[0])
            ack_inner = json.loads(ack_outer["data"])
            assert ack_inner["cardData"]["cardParamMap"]["status"] == "approved"
            assert (
                ack_inner["userPrivateDataMap"]["staff_001"]["cardParamMap"]["tip"]
                == "done"
            )
    finally:
        cb_matcher.destroy()

    # AI Card streaming: normal completion sends isFinalize=True
    driver.requests.clear()
    async with gw.ai_card_stream(
        card_template_id="tpl.schema",
        open_conversation_id="cid_ops",
        initial_text="thinking...",
        throttle_interval=0.0,
    ) as stream:
        await stream.append("hello ")
        await stream.append("world")

    deliver_req = next(
        r for r in driver.requests if "/card/instances/createAndDeliver" in str(r.url)
    )
    assert deliver_req.json["callbackType"] == "STREAM"
    assert deliver_req.json["openSpaceId"] == "dtv1.card//IM_GROUP.cid_ops"

    stream_reqs = [r for r in driver.requests if "/card/streaming" in str(r.url)]
    assert stream_reqs[-1].json["isFinalize"] is True
    assert stream_reqs[-1].json["isError"] is False
    assert stream_reqs[-1].json["content"] == "hello world"

    # AI Card streaming: exception guarantees isError=True
    driver.requests.clear()
    with pytest.raises(RuntimeError, match="llm crash"):
        async with gw.ai_card_stream(
            card_template_id="tpl.schema",
            user_id="staff_001",
        ):
            raise RuntimeError("llm crash")

    err_req = [r for r in driver.requests if "/card/streaming" in str(r.url)][-1]
    assert err_req.json["isFinalize"] is True
    assert err_req.json["isError"] is True
