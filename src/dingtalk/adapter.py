"""DingTalk Stream Adapter: WebSocket lifecycle, ACK/dedupe, and action dispatch."""

from typing import Any
from typing_extensions import override

import anyio
from opsmesh.adapters import Adapter as BaseAdapter
from opsmesh.adapters import Gateway as BaseGateway
from opsmesh.drivers import Driver, HTTPClientMixin, WebSocket, WebSocketClientMixin
from opsmesh.message import handle_event
from opsmesh.plugin import get_plugin_config

from dingtalk.api import (
    ActionDispatcher,
    ApiClient,
    CardService,
    ContactService,
    MediaService,
    MessagingService,
    TokenManager,
)
from dingtalk.config import Config
from dingtalk.event import (
    CardCallbackEvent,
    LifecycleMetaEvent,
    parse_card_callback_event,
    parse_chatbot_event,
    parse_platform_notice_event,
)
from dingtalk.gateway import Gateway
from dingtalk.stream import SendText, StreamFrame, StreamSession, build_ack_frame
from dingtalk.typing import ADAPTER_NAME, FrameType, StreamTopic
from dingtalk.utils import DedupeRing, log


class Adapter(BaseAdapter):
    """Native asynchronous DingTalk Stream protocol adapter for OpsMesh."""

    @classmethod
    @override
    def get_name(cls) -> str:
        return ADAPTER_NAME

    @override
    def __init__(
        self,
        driver: Driver,
        *,
        dingtalk_config: Config | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, **kwargs)
        if not isinstance(driver, HTTPClientMixin) or not isinstance(
            driver, WebSocketClientMixin
        ):
            raise TypeError(
                "DingTalk Adapter requires HTTPClientMixin and WebSocketClientMixin "
                "(e.g. DRIVER=~httpx+~websockets or DRIVER=~aiohttp)."
            )

        self.dingtalk_config: Config = (
            dingtalk_config
            if dingtalk_config is not None
            else get_plugin_config(Config)
        )
        self._api_client = ApiClient(self, self.dingtalk_config)
        self._media_service = MediaService(self._api_client)
        self._dispatcher = ActionDispatcher(
            messaging=MessagingService(self._api_client),
            media=self._media_service,
            cards=CardService(self._api_client),
            contacts=ContactService(
                self._api_client,
                cache_ttl=self.dingtalk_config.dingtalk_user_cache_ttl,
            ),
        )

        self._bot_gateways: dict[str, Gateway] = {}
        self._token_managers: dict[str, TokenManager] = {}
        self._dedupe_rings: dict[str, DedupeRing] = {}
        self._sessions: dict[str, StreamSession] = {}

        for bot_cfg in self.dingtalk_config.dingtalk_bots:
            gw = Gateway(self, bot_cfg)
            self._bot_gateways[gw.id] = gw
            self._token_managers[gw.id] = TokenManager(self._api_client, bot_cfg)
            self._dedupe_rings[gw.id] = DedupeRing(
                capacity=self.dingtalk_config.dingtalk_dedupe_capacity,
                ttl=self.dingtalk_config.dingtalk_dedupe_ttl,
            )
            self._sessions[gw.id] = StreamSession(
                self,
                bot_cfg,
                self.dingtalk_config,
                on_frame=self._make_frame_handler(gw),
                on_connected=self._make_connected_cb(gw),
                on_disconnected=self._make_disconnected_cb(gw),
            )

        self.driver.on_startup(self._startup)
        self.driver.on_shutdown(self._shutdown)

    def _make_connected_cb(self, gateway: Gateway):
        def _cb() -> None:
            if gateway.id not in self.gateways:
                self.connect(gateway)
            meta = LifecycleMetaEvent(
                sub_type="connected",
                gateway_id=gateway.id,
                robotCode=gateway.robot_code,
                silent=True,
            )
            self.driver.task_group.start_soon(handle_event, gateway, meta)

        return _cb

    def _make_disconnected_cb(self, gateway: Gateway):
        def _cb() -> None:
            if gateway.id in self.gateways:
                self.disconnect(gateway)

        return _cb

    def _make_frame_handler(self, gateway: Gateway):
        async def _handler(frame: StreamFrame, send_text: SendText) -> None:
            await self.handle_stream_frame(gateway, frame, send_text)

        return _handler

    async def _startup(self) -> None:
        if not self._sessions:
            log("WARNING", "No DingTalk bots configured in DINGTALK_BOTS.")
            return
        for session in self._sessions.values():
            self.driver.task_group.start_soon(session.run)

    async def _shutdown(self) -> None:
        for session in self._sessions.values():
            session.stop()
        for session in self._sessions.values():
            await session.wait_closed()

    async def handle_stream_frame(
        self,
        gateway: Gateway,
        frame: StreamFrame,
        target: WebSocket | SendText,
    ) -> None:
        """Process a CALLBACK or EVENT StreamFrame with ACK and deduplication."""
        send_text: SendText = (
            target.send_text if isinstance(target, WebSocket) else target
        )
        dedupe = self._dedupe_rings[gateway.id]

        if (
            frame.type == FrameType.CALLBACK
            and frame.topic == StreamTopic.CARD_CALLBACK
        ):
            await self._handle_card_callback_frame(gateway, dedupe, frame, send_text)
            return

        if frame.message_id:
            await send_text(build_ack_frame(frame.message_id))

        dedupe_key = (
            str(frame.data.get("msgId") or "")
            or str(frame.headers.get("eventId") or "")
            or frame.message_id
        )
        if dedupe_key and dedupe.seen_or_record(dedupe_key):
            log("DEBUG", f"Dropped duplicate Stream frame {dedupe_key!r}")
            return

        if frame.type == FrameType.CALLBACK and frame.topic == StreamTopic.BOT_MESSAGE:
            event = parse_chatbot_event(frame.data)
            self.driver.task_group.start_soon(handle_event, gateway, event)
        elif frame.type == FrameType.EVENT:
            event = parse_platform_notice_event(
                frame.headers, frame.data, robot_code=gateway.robot_code
            )
            self.driver.task_group.start_soon(handle_event, gateway, event)
        else:
            log("DEBUG", f"Unhandled Stream topic {frame.type}:{frame.topic!r}")

    async def _handle_card_callback_frame(
        self,
        gateway: Gateway,
        dedupe: DedupeRing,
        frame: StreamFrame,
        send_text: SendText,
    ) -> None:
        if frame.message_id and dedupe.seen_or_record(frame.message_id):
            await send_text(build_ack_frame(frame.message_id, data={}))
            return

        event = parse_card_callback_event(frame.data, robot_code=gateway.robot_code)

        async def _run_and_signal(gw: Gateway, ev: CardCallbackEvent) -> None:
            try:
                await handle_event(gw, ev)
            finally:
                ev._ack_event.set()

        self.driver.task_group.start_soon(_run_and_signal, gateway, event)

        with anyio.move_on_after(self.dingtalk_config.dingtalk_card_ack_timeout):
            await event._ack_event.wait()

        ack_payload = event._consume_ack_payload()
        if frame.message_id:
            await send_text(build_ack_frame(frame.message_id, data=ack_payload))

    @override
    async def _invoke(
        self,
        gateway: BaseGateway,
        action: str,
        **params: Any,
    ) -> Any:
        if not isinstance(gateway, Gateway):
            raise TypeError(f"Expected dingtalk.Gateway, got {type(gateway).__name__}")
        return await self._dispatcher.dispatch(
            robot_code=gateway.robot_code,
            token_manager=self._token_managers[gateway.id],
            action=action,
            params=params,
        )


__all__ = ["Adapter"]
