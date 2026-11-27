"""Per-bot WebSocket keepalive loop, 0s disconnect handoff, and backoff."""

from collections.abc import Awaitable, Callable
import json
import secrets
from typing import TYPE_CHECKING

import anyio
from opsmesh.drivers import Request, Timeout, WebSocket
from opsmesh.exception import WebSocketClosed

from dingtalk.config import BotConfig, Config
from dingtalk.exception import StreamProtocolError
from dingtalk.typing import FrameType, StreamTopic
from dingtalk.utils import log

from .protocol import (
    OPEN_CONNECTION_PATH,
    StreamFrame,
    build_ack_frame,
    build_open_connection_payload,
    build_wss_url,
    parse_stream_frame,
)

if TYPE_CHECKING:
    from opsmesh.adapters import Adapter as BaseAdapter

SendText = Callable[[str], Awaitable[None]]
FrameHandler = Callable[[StreamFrame, SendText], Awaitable[None]]
LifecycleCallback = Callable[[], None]


class StreamSession:
    """Manage a single DingTalk bot's Stream WebSocket lifecycle."""

    def __init__(
        self,
        adapter: "BaseAdapter",
        bot_config: BotConfig,
        config: Config,
        *,
        on_frame: FrameHandler,
        on_connected: LifecycleCallback,
        on_disconnected: LifecycleCallback,
    ) -> None:
        self._adapter = adapter
        self._bot_config = bot_config
        self._config = config
        self._on_frame = on_frame
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._scope: anyio.CancelScope | None = None
        self._closed = anyio.Event()
        self._stopping = False

    def stop(self) -> None:
        """Signal the session loop to terminate and cancel active waits."""
        self._stopping = True
        if self._scope is not None:
            self._scope.cancel()

    async def wait_closed(self) -> None:
        """Wait until the session loop has exited cleanly."""
        await self._closed.wait()

    async def run(self) -> None:
        """Keep the Stream WebSocket connected until stop() is called."""
        attempt = 0
        try:
            with anyio.CancelScope() as scope:
                self._scope = scope
                while not self._stopping:
                    fast_reconnect = False
                    connected = False
                    try:
                        wss_url = await self._open_connection_url()
                        request = Request(
                            "GET",
                            wss_url,
                            timeout=Timeout(
                                total=None,
                                connect=self._config.dingtalk_request_timeout,
                                read=None,
                                close=10.0,
                                ping=20.0,
                            ),
                            ping_interval=20.0,
                        )
                        async with self._adapter.websocket(request) as ws:
                            connected = True
                            attempt = 0
                            self._on_connected()
                            log(
                                "INFO",
                                f"Stream connected for {self._bot_config.gateway_id}",
                            )
                            fast_reconnect = await self._receive_loop(ws)
                    except WebSocketClosed as exc:
                        log(
                            "WARNING",
                            f"Stream closed for {self._bot_config.gateway_id}: {exc}",
                        )
                    except Exception as exc:
                        log(
                            "WARNING",
                            f"Stream error for {self._bot_config.gateway_id}",
                            exception=exc,
                        )
                    finally:
                        if connected:
                            self._on_disconnected()

                    if self._stopping:
                        break
                    if fast_reconnect:
                        log(
                            "INFO",
                            f"Received SYSTEM/disconnect for "
                            f"{self._bot_config.gateway_id}; reconnecting immediately.",
                        )
                        continue

                    delay = min(
                        float(2**attempt),
                        self._config.dingtalk_reconnect_max_delay,
                    ) + (secrets.randbelow(500) / 1000.0)
                    attempt += 1
                    await anyio.sleep(delay)
        finally:
            self._scope = None
            self._closed.set()

    async def _open_connection_url(self) -> str:
        base = self._config.dingtalk_api_base.rstrip("/")
        request = Request(
            "POST",
            f"{base}{OPEN_CONNECTION_PATH}",
            json=build_open_connection_payload(self._bot_config),
            timeout=self._config.dingtalk_request_timeout,
        )
        response = await self._adapter.request(request)
        if response.status_code != 200 or not response.content:
            raise StreamProtocolError(f"Handshake failed (HTTP {response.status_code})")
        raw = (
            response.content.decode("utf-8")
            if isinstance(response.content, bytes)
            else response.content
        )
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise StreamProtocolError("Invalid JSON in Stream handshake.") from exc

        if not isinstance(body, dict):
            raise StreamProtocolError("Stream handshake response is not a JSON object.")

        endpoint = body.get("endpoint")
        ticket = body.get("ticket")
        if not isinstance(endpoint, str) or not isinstance(ticket, str):
            raise StreamProtocolError("Stream handshake missing endpoint or ticket.")
        return str(build_wss_url(endpoint, ticket))

    async def _receive_loop(self, ws: WebSocket) -> bool:
        """Read frames without blocking on callbacks; True triggers 0s reconnect."""
        send_lock = anyio.Lock()

        async def _safe_send(text: str) -> None:
            async with send_lock:
                if not ws.closed:
                    await ws.send_text(text)

        async with anyio.create_task_group() as frame_tg:
            while not self._stopping:
                raw = await ws.receive()
                frame = parse_stream_frame(raw)
                match frame.type:
                    case FrameType.SYSTEM:
                        if frame.topic == StreamTopic.PING:
                            await _safe_send(
                                build_ack_frame(frame.message_id, data=frame.raw_data)
                            )
                        elif frame.topic == StreamTopic.DISCONNECT:
                            return True
                    case FrameType.CALLBACK | FrameType.EVENT:
                        frame_tg.start_soon(self._on_frame, frame, _safe_send)
                    case _:
                        log(
                            "DEBUG",
                            f"Ignoring unknown Stream frame type: {frame.type!r}",
                        )
        return False


__all__ = ["FrameHandler", "LifecycleCallback", "SendText", "StreamSession"]
