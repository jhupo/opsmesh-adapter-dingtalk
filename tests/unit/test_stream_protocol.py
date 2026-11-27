"""Tests for Stream wire protocol, ping/pong, 0s disconnect reconnect, and dedupe."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import json
from typing import Any

import anyio
import opsmesh
from opsmesh import on_command
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
from opsmesh.exception import WebSocketClosed
from opsmesh.rule import TrieRule
import pytest

from dingtalk import Adapter, BotConfig, Config, MessageEvent
from dingtalk.stream import (
    build_ack_frame,
    build_open_connection_payload,
    build_wss_url,
    parse_stream_frame,
)


class _FakeWebSocket(WebSocket):
    def __init__(self, request: Request, incoming: list[str]) -> None:
        super().__init__(request=request)
        self._incoming = list(incoming)
        self.sent: list[str] = []
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def accept(self) -> None:
        return None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self._closed = True

    async def receive(self) -> str | bytes:
        if self._incoming:
            await anyio.lowlevel.checkpoint()
            return self._incoming.pop(0)
        self._closed = True
        raise WebSocketClosed(1000, "EOF")

    async def receive_text(self) -> str:
        data = await self.receive()
        return data.decode() if isinstance(data, bytes) else data

    async def receive_bytes(self) -> bytes:
        data = await self.receive()
        return data if isinstance(data, bytes) else data.encode()

    async def send_text(self, data: str) -> None:
        self.sent.append(data)

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data.decode("utf-8"))


class _FakeClientDriver(Driver, HTTPClientMixin, WebSocketClientMixin):
    def __init__(self, ws_batches: list[list[str]]) -> None:
        super().__init__(Env(), OpsMeshConfig())
        self._ws_batches = list(ws_batches)
        self.http_requests: list[Request] = []
        self.opened_ws: list[_FakeWebSocket] = []
        self.reconnected = anyio.Event()

    @property
    def type(self) -> str:
        return "fake"

    @property
    def logger(self) -> Any:
        return None

    def run(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def request(self, setup: Request) -> Response:
        self.http_requests.append(setup)
        if str(setup.url).endswith("/v1.0/gateway/connections/open"):
            return Response(
                200,
                content=json.dumps(
                    {
                        "endpoint": "wss://wss-open-connection.dingtalk.com/connect",
                        "ticket": f"ticket_{len(self.http_requests)}",
                    }
                ).encode(),
            )
        return Response(200, content=b"{}")

    async def stream_request(
        self, setup: Request, *, chunk_size: int = 1024
    ) -> AsyncGenerator[Response, None]:
        yield Response(200, content=b"")

    def get_session(self, *args: Any, **kwargs: Any) -> HTTPClientSession:
        raise NotImplementedError

    @asynccontextmanager
    async def websocket(self, setup: Request) -> AsyncGenerator[WebSocket, None]:
        batch = self._ws_batches.pop(0) if self._ws_batches else []
        ws = _FakeWebSocket(setup, batch)
        self.opened_ws.append(ws)
        if len(self.opened_ws) >= 2:
            self.reconnected.set()
        yield ws


def test_stream_protocol_helpers() -> None:
    bot = BotConfig(
        client_id="ding_1",
        client_secret="sec_1",
        subscribe_card_callbacks=True,
        subscribe_events=True,
    )
    payload = build_open_connection_payload(bot)
    assert payload["clientId"] == "ding_1"
    assert len(payload["subscriptions"]) == 3

    wss = build_wss_url("wss://wss.dingtalk.com/connect", "tk_99")
    assert str(wss) == "wss://wss.dingtalk.com/connect?ticket=tk_99"

    with pytest.raises(Exception, match="Unsafe or invalid"):
        build_wss_url("http://insecure.example.com/ws", "tk")

    frame = parse_stream_frame(
        json.dumps(
            {
                "type": "CALLBACK",
                "headers": {
                    "topic": "/v1.0/im/bot/messages/get",
                    "messageId": "m_01",
                },
                "data": json.dumps({"msgtype": "text", "text": {"content": "hi"}}),
            }
        )
    )
    assert frame.message_id == "m_01"
    assert frame.data["msgtype"] == "text"
    ack = json.loads(build_ack_frame("m_01"))
    assert ack["code"] == 200
    assert ack["headers"]["messageId"] == "m_01"


@pytest.mark.anyio
async def test_stream_session_ping_dedupe_and_fast_disconnect_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    msg_frame = json.dumps(
        {
            "type": "CALLBACK",
            "headers": {
                "topic": "/v1.0/im/bot/messages/get",
                "messageId": "f_msg_1",
            },
            "data": json.dumps(
                {
                    "conversationType": "1",
                    "msgId": "dup_msg_id",
                    "msgtype": "text",
                    "senderStaffId": "staff_1",
                    "text": {"content": "/ping"},
                }
            ),
        }
    )
    dup_frame = json.dumps(
        {
            "type": "CALLBACK",
            "headers": {
                "topic": "/v1.0/im/bot/messages/get",
                "messageId": "f_msg_2",
            },
            "data": json.dumps(
                {
                    "conversationType": "1",
                    "msgId": "dup_msg_id",
                    "msgtype": "text",
                    "senderStaffId": "staff_1",
                    "text": {"content": "/ping"},
                }
            ),
        }
    )
    ping_frame = json.dumps(
        {
            "type": "SYSTEM",
            "headers": {"topic": "ping", "messageId": "f_ping"},
            "data": '{"opaque":"123"}',
        }
    )
    disconnect_frame = json.dumps(
        {
            "type": "SYSTEM",
            "headers": {"topic": "disconnect", "messageId": "f_disc"},
            "data": "{}",
        }
    )

    driver = _FakeClientDriver(
        ws_batches=[
            [ping_frame, msg_frame, dup_frame, disconnect_frame],
            [],
        ]
    )
    monkeypatch.setattr(opsmesh, "_driver", driver)
    cfg = Config(
        dingtalk_bots=[BotConfig(client_id="ding_test", client_secret="sec_test")],
        dingtalk_reconnect_max_delay=0.05,
    )
    adapter = Adapter(driver, dingtalk_config=cfg)

    handled: list[str] = []
    ping_done = anyio.Event()
    ping_cmd = on_command("ping")

    @ping_cmd.handle()
    async def _on_ping(event: MessageEvent) -> None:
        handled.append(event.msg_id)
        ping_done.set()

    try:
        async with driver._lifespan:
            with anyio.fail_after(2.0):
                await driver.reconnected.wait()
                await ping_done.wait()
            assert handled == ["dup_msg_id"]
            first_ws = driver.opened_ws[0]
            acks = [json.loads(s) for s in first_ws.sent]
            assert any(
                a["headers"]["messageId"] == "f_ping"
                and a["data"] == '{"opaque":"123"}'
                for a in acks
            )
            assert any(a["headers"]["messageId"] == "f_msg_1" for a in acks)
            assert any(a["headers"]["messageId"] == "f_msg_2" for a in acks)
    finally:
        ping_cmd.destroy()
        TrieRule.prefix.pop("/ping", None)

    assert adapter.gateways == {}
