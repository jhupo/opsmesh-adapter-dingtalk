"""Tests for GB-scale streaming download, Range resume, SSRF guard, and uploads."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import json
from pathlib import Path
from typing import Any

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
    Config,
    MediaError,
    MediaLimitExceeded,
    MediaType,
    MessageSegment,
)
from dingtalk.api import upgrade_oss_https, validate_download_origin


class _MediaMockDriver(Driver, HTTPClientMixin, WebSocketClientMixin):
    def __init__(self) -> None:
        super().__init__(Env(), OpsMeshConfig())
        self.requests: list[Request] = []
        self.stream_calls: list[Request] = []
        self.fail_first_stream = False

    @property
    def type(self) -> str:
        return "media-mock"

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
                content=json.dumps({"accessToken": "tok_1", "expireIn": 7200}).encode(),
            )
        if url.endswith("/v1.0/robot/messageFiles/download"):
            return Response(
                200,
                content=json.dumps(
                    {
                        "downloadUrl": (
                            "http://dt-bucket.oss-cn-hangzhou.aliyuncs.com"
                            "/obj/archive.file?Expires=999&Signature=abc%2Bdef"
                        )
                    }
                ).encode(),
            )
        if "/media/upload" in url:
            return Response(
                200,
                content=json.dumps({"errcode": 0, "media_id": "@lAz_media_1"}).encode(),
            )
        if "/convFile/conversations/spaces/query" in url:
            return Response(
                200, content=json.dumps({"space": {"spaceId": "sp_01"}}).encode()
            )
        if "/uploadInfos/query" in url:
            return Response(
                200,
                content=json.dumps(
                    {
                        "uploadKey": "uk_01",
                        "headerSignatureInfo": {
                            "resourceUrls": [
                                "https://dt-bucket.oss-cn-hangzhou.aliyuncs.com/up/big.zip"
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
                    {"dentry": {"id": "den_99", "spaceId": "sp_01"}}
                ).encode(),
            )
        if "/convFile/conversations/files/send" in url:
            return Response(200, content=json.dumps({"sent": True}).encode())
        return Response(200, content=b"{}")

    async def stream_request(
        self, setup: Request, *, chunk_size: int = 1024
    ) -> AsyncGenerator[Response, None]:
        self.stream_calls.append(setup)
        range_hdr = setup.headers.get("Range")
        if self.fail_first_stream and len(self.stream_calls) == 1:
            yield Response(
                200,
                headers={"content-type": "application/pdf"},
                content=b"PART1-",
            )
            raise RuntimeError("simulated mid-stream disconnect")

        if range_hdr == "bytes=6-":
            yield Response(
                206,
                headers={"content-type": "application/pdf"},
                content=b"PART2-END",
            )
            return

        yield Response(
            200,
            headers={"content-type": "application/pdf"},
            content=b"FULL-PDF-BYTES",
        )

    def get_session(self, *args: Any, **kwargs: Any) -> HTTPClientSession:
        raise NotImplementedError

    @asynccontextmanager
    async def websocket(self, setup: Request) -> AsyncGenerator[WebSocket, None]:
        raise NotImplementedError
        yield  # pragma: no cover


def test_oss_upgrade_and_ssrf_allowlist() -> None:
    raw = "http://my-bucket.oss-cn-shanghai.aliyuncs.com/a/b?Sig=x%2Fy"
    upgraded = upgrade_oss_https(raw)
    assert upgraded == "https://my-bucket.oss-cn-shanghai.aliyuncs.com/a/b?Sig=x%2Fy"

    allowed = ("dingtalk.com", "alicdn.com", "aliyuncs.com", "ddurl.cn")
    validate_download_origin(upgraded, allowed)

    with pytest.raises(MediaError, match="not allowed"):
        validate_download_origin("https://evil.example.com/file.bin", allowed)
    with pytest.raises(MediaError, match="not allowed"):
        validate_download_origin("http://169.254.169.254/latest/meta-data", allowed)
    with pytest.raises(MediaError, match="not allowed"):
        validate_download_origin("https://user:pass@oapi.dingtalk.com/a", allowed)


@pytest.mark.anyio
async def test_streaming_download_range_resume_and_max_bytes(tmp_path: Path) -> None:
    driver = _MediaMockDriver()
    driver.fail_first_stream = True
    cfg = Config(dingtalk_bots=[BotConfig(client_id="ding_m", client_secret="sec_m")])
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_m"]

    seg = MessageSegment.file(download_code="dl_code_1", name="../report.file")
    media = await gw.download_media(seg, dest_dir=tmp_path)

    assert media.path.parent == tmp_path
    assert media.name == "report.pdf"
    assert media.path.read_bytes() == b"PART1-PART2-END"
    assert media.size == len(b"PART1-PART2-END")
    assert len(driver.stream_calls) == 2
    assert driver.stream_calls[1].headers["Range"] == "bytes=6-"

    # Verify max_bytes guard cleans up .part file
    driver.fail_first_stream = False
    with pytest.raises(MediaLimitExceeded):
        await gw.download_media(seg, dest_dir=tmp_path, max_bytes=4)
    assert list(tmp_path.glob("*.part")) == []

    # Verify iter_media yields raw chunks
    chunks = [c async for c in gw.iter_media(seg)]
    assert b"".join(chunks) == b"FULL-PDF-BYTES"


@pytest.mark.anyio
async def test_upload_media_limit_and_dingpan_large_file(tmp_path: Path) -> None:
    driver = _MediaMockDriver()
    cfg = Config(
        dingtalk_bots=[
            BotConfig(
                client_id="ding_u",
                client_secret="sec_u",
                operator_union_id="union_op_1",
            )
        ]
    )
    adapter = Adapter(driver, dingtalk_config=cfg)
    gw = adapter._bot_gateways["dingtalk:ding_u"]

    # Normal upload succeeds
    mid = await gw.upload_media(b"hello", media_type=MediaType.FILE, filename="a.txt")
    assert mid == "@lAz_media_1"

    # Voice > 2MB rejected locally before HTTP call
    with pytest.raises(MediaLimitExceeded):
        await gw.upload_media(
            b"0" * (2 * 1024 * 1024 + 1),
            media_type=MediaType.VOICE,
            filename="v.amr",
        )

    # DingPan > 20MB group file upload flow
    big_file = tmp_path / "dump.zip"
    big_file.write_bytes(b"BIGDATA")
    res = await gw.send_dingpan_file("cid_ops", big_file)
    assert res == {"sent": True}
    assert any(
        r.method == "PUT" and "aliyuncs.com" in str(r.url) for r in driver.requests
    )
