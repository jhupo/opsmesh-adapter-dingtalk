"""Edge-case tests: slow card ACK, FinishedException, webhook fallback, and hooks."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import json
from pathlib import Path
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
from opsmesh.exception import ActionNotAvailable, FinishedException, MockInvokeResult
import pytest

from dingtalk import (
    Adapter,
    BotConfig,
    CardCallbackEvent,
    Config,
    Gateway,
    MessageSegment,
)
from dingtalk.event import parse_chatbot_event
from dingtalk.stream import parse_stream_frame


class _EdgeMockDriver(Driver, HTTPClientMixin, WebSocketClientMixin):
    def __init__(self) -> None:
        super().__init__(Env(), OpsMeshConfig())
        self.requests: list[Request] = []
        self.webhook_should_fail = False

    @property
    def type(self) -> str:
        return "edge-mock"

    @property
    def logger(self) -> Any:
        return None

    def run(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def request(self, setup: Request) -> Response:
        self.requests.append(setup)
        url = str(setup.url)
        if url.endswith("/v1.0/oauth2/accessToken"):
            return Response(
                200,
                content=json.dumps(
                    {"accessToken": "tok_edge", "expireIn": 7200}
                ).encode(),
            )
        if "sendBySession" in url:
            if self.webhook_should_fail:
                return Response(
                    200,
                    content=json.dumps(
                        {"errcode": 310000, "errmsg": "session expired"}
                    ).encode(),
                )
            return Response(200, content=json.dumps({"errcode": 0}).encode())
        if "/robot/groupMessages/send" in url:
            return Response(200, content=json.dumps({"processQueryKey": "g1"}).encode())
        if "/convFile/conversations/spaces/query" in url:
            return Response(
                200, content=json.dumps({"space": {"spaceId": "sp_big"}}).encode()
            )
        if "/uploadInfos/query" in url:
            return Response(
                200,
                content=json.dumps(
                    {
                        "uploadKey": "uk_big",
                        "headerSignatureInfo": {
                            "resourceUrls": [
                                "https://dt-bucket.oss-cn-hangzhou.aliyuncs.com/big.bin"
                            ],
                            "headers": {"Authorization": "OSS sig"},
                        },
                    }
                ).encode(),
            )
        if setup.method == "PUT" and "aliyuncs.com" in url:
            return Response(200, content=b"")
        if "/commit" in url:
            return Response(
                200,
                content=json.dumps(
                    {"dentry": {"id": "den_big", "spaceId": "sp_big"}}
                ).encode(),
            )
        if "/convFile/conversations/files/send" in url:
            return Response(200, content=json.dumps({"dingpanSent": True}).encode())
        if "/v1.0/card/" in url:
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
async def test_card_callback_slow_ack_and_async_update() -> None:
    driver = _EdgeMockDriver()
    cfg = Config(
        dingtalk_bots=[BotConfig(client_id="ding_slow", client_secret="sec_slow")],
        dingtalk_card_ack_timeout=1.0,
    )
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_slow"]

    acks: list[str] = []
    finished = anyio.Event()

    async def _send_ack(data: str) -> None:
        acks.append(data)

    cb = on_type(CardCallbackEvent)

    @cb.handle()
    async def _slow_handler(gateway: Gateway, event: CardCallbackEvent) -> None:
        await event.ack_card(card_param_map={"status": "running"})
        await anyio.sleep(0.05)
        await gateway.update_card(
            out_track_id=event.out_track_id,
            card_param_map={"status": "completed"},
        )
        finished.set()

    try:
        async with driver._lifespan:
            frame = parse_stream_frame(
                json.dumps(
                    {
                        "type": "CALLBACK",
                        "headers": {
                            "topic": "/v1.0/card/instances/callback",
                            "messageId": "cb_slow_1",
                        },
                        "data": json.dumps(
                            {
                                "outTrackId": "track_slow",
                                "userId": "staff_1",
                                "content": (
                                    '{"cardPrivateData":'
                                    '{"actionIds":["b1"],"params":{}}}'
                                ),
                            }
                        ),
                    }
                )
            )
            await adapter.handle_stream_frame(gw, frame, _send_ack)
            assert len(acks) == 1
            ack_data = json.loads(json.loads(acks[0])["data"])
            assert ack_data["cardData"]["cardParamMap"]["status"] == "running"

            with anyio.fail_after(2.0):
                await finished.wait()

            update_reqs = [
                r
                for r in driver.requests
                if r.method == "PUT" and str(r.url).endswith("/v1.0/card/instances")
            ]
            assert len(update_reqs) == 1
            assert (
                update_reqs[0].json["cardData"]["cardParamMap"]["status"] == "completed"
            )
    finally:
        cb.destroy()


@pytest.mark.anyio
async def test_webhook_fallback_and_large_file_dingpan_upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = _EdgeMockDriver()
    driver.webhook_should_fail = True
    cfg = Config(
        dingtalk_bots=[
            BotConfig(
                client_id="ding_fb",
                client_secret="sec_fb",
                operator_union_id="union_ops",
            )
        ]
    )
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_fb"]

    future_ms = int((time.time() + 3600) * 1000)
    ev = parse_chatbot_event(
        {
            "conversationType": "2",
            "conversationId": "cid_fb",
            "senderStaffId": "staff_1",
            "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=old",
            "sessionWebhookExpiredTime": future_ms,
            "msgtype": "text",
            "text": {"content": "hi"},
        }
    )

    # Failed sessionWebhook falls back cleanly to group OpenAPI
    res = await gw.send(ev, "fallback-reply")
    assert res == {"processQueryKey": "g1"}
    assert any("sendBySession" in str(r.url) for r in driver.requests)
    assert any("/robot/groupMessages/send" in str(r.url) for r in driver.requests)

    # File exceeding media/upload limit auto-upgrades to DingPan group file
    monkeypatch.setitem(
        __import__(
            "dingtalk.api.media", fromlist=["MEDIA_UPLOAD_LIMITS"]
        ).MEDIA_UPLOAD_LIMITS,
        "file",
        4,
    )
    big_file = tmp_path / "huge.zip"
    big_file.write_bytes(b"1234567890")
    res_big = await gw.send(ev, MessageSegment.file(path=big_file))
    assert res_big == {"dingpanSent": True}


@pytest.mark.anyio
async def test_ai_card_stream_treats_finished_exception_as_normal() -> None:
    driver = _EdgeMockDriver()
    cfg = Config(dingtalk_bots=[BotConfig(client_id="ding_ai", client_secret="sec_ai")])
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_ai"]

    async def _run_and_finish() -> None:
        async with gw.ai_card_stream(
            card_template_id="tpl.schema",
            user_id="staff_1",
            throttle_interval=0.0,
        ) as stream:
            await stream.append("done answer")
            raise FinishedException

    with pytest.raises(FinishedException):
        await _run_and_finish()

    stream_reqs = [r for r in driver.requests if "/card/streaming" in str(r.url)]
    assert stream_reqs[-1].json["isFinalize"] is True
    assert stream_reqs[-1].json["isError"] is False
    assert stream_reqs[-1].json["content"] == "done answer"


@pytest.mark.anyio
async def test_unknown_action_and_before_invoke_mock_hook() -> None:
    driver = _EdgeMockDriver()
    cfg = Config(dingtalk_bots=[BotConfig(client_id="ding_hk", client_secret="sec_hk")])
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_hk"]

    with pytest.raises(ActionNotAvailable):
        await gw.invoke("nonexistent_action")

    async def _mock_hook(gateway: Any, action: str, params: dict[str, Any]) -> None:
        if action == "get_access_token":
            raise MockInvokeResult("mocked_token_value")

    Gateway.before_invoke(_mock_hook)
    try:
        token = await gw.get_access_token()
        assert token == "mocked_token_value"
    finally:
        Gateway._before_invoke_hooks.discard(_mock_hook)
