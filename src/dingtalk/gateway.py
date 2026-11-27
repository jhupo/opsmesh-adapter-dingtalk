"""DingTalk Gateway: smart dual-channel send routing and typed invoke facade."""

from collections.abc import AsyncIterator
from pathlib import Path
import time
from typing import IO, TYPE_CHECKING, Any
from typing_extensions import override

from opsmesh.adapters import Event as BaseEvent
from opsmesh.adapters import Gateway as BaseGateway
from opsmesh.adapters import Message as BaseMessage
from opsmesh.adapters import MessageSegment as BaseMessageSegment
from opsmesh.adapters import MessageTemplate

from dingtalk.api import AICardStream
from dingtalk.config import BotConfig
from dingtalk.event import (
    DownloadedMedia,
    GroupMessageEvent,
    MessageEvent,
    PrivateMessageEvent,
    UserProfile,
)
from dingtalk.exception import DingTalkActionFailed, MediaError, MediaLimitExceeded
from dingtalk.message import (
    Message,
    MessageSegment,
    OpenAPIUnit,
    build_session_webhook_payload,
    can_use_session_webhook,
    plan_openapi_units,
)
from dingtalk.typing import DingTalkAction, MediaType, T_JsonDict
from dingtalk.utils import log

if TYPE_CHECKING:
    from dingtalk.adapter import Adapter

_WEBHOOK_EXPIRY_BUFFER_MS = 5000


def _coerce_message(
    message: str | BaseMessage | BaseMessageSegment | MessageTemplate,
) -> Message:
    if isinstance(message, Message):
        return message
    if isinstance(message, MessageSegment):
        return Message(message)
    if isinstance(message, str):
        return Message(message)
    return Message(str(message))


def _extract_download_info(
    target: str | MessageSegment,
) -> tuple[str, str | None, str | None]:
    if isinstance(target, str):
        return target, None, None
    code = target.data.get("download_code")
    if not isinstance(code, str) or not code:
        raise MediaError(f"MessageSegment {target.type!r} has no download_code.")
    name = target.data.get("name")
    fallback_ext = target.data.get("video_type") or target.data.get("file_type")
    return (
        code,
        str(name) if isinstance(name, str) and name else None,
        str(fallback_ext) if isinstance(fallback_ext, str) and fallback_ext else None,
    )


class Gateway(BaseGateway):
    """DingTalk bot gateway instance managed by DingTalk Adapter."""

    adapter: "Adapter"

    def __init__(self, adapter: "Adapter", bot_config: BotConfig) -> None:
        super().__init__(adapter, bot_config.gateway_id)
        self.bot_config: BotConfig = bot_config

    @property
    def robot_code(self) -> str:
        return self.bot_config.effective_robot_code

    async def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Obtain a cached or refreshed DingTalk access_token."""
        return await self.invoke(
            DingTalkAction.GET_ACCESS_TOKEN, force_refresh=force_refresh
        )

    @override
    async def send(
        self,
        event: BaseEvent,
        message: str | BaseMessage | BaseMessageSegment | MessageTemplate,
        *,
        at_sender: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Reply to an inbound event via sessionWebhook or OpenAPI automatically."""
        msg = _coerce_message(message)
        if not msg:
            return None

        if isinstance(event, MessageEvent) and self._can_reply_webhook(event, msg):
            at_ids: list[str] = []
            if at_sender and isinstance(event, GroupMessageEvent):
                uid = event.get_user_id()
                if uid:
                    at_ids.append(uid)
            payload = build_session_webhook_payload(msg, at_user_ids=at_ids or None)
            try:
                return await self.invoke(
                    DingTalkAction.SEND_SESSION_WEBHOOK,
                    webhook_url=event.session_webhook,
                    payload=payload,
                )
            except DingTalkActionFailed as exc:
                log(
                    "WARNING",
                    f"sessionWebhook reply failed for {self.id}, falling back "
                    f"to OpenAPI: {exc.message}",
                )

        if isinstance(event, GroupMessageEvent):
            return await self._send_units_to_group(
                event.conversation_id,
                msg,
                sender_staff_id=event.sender_staff_id or None,
            )
        if isinstance(event, PrivateMessageEvent):
            return await self._send_units_to_private([event.get_user_id()], msg)

        raise DingTalkActionFailed(
            f"Cannot determine reply target for event type {type(event).__name__}."
        )

    def _can_reply_webhook(self, event: MessageEvent, msg: Message) -> bool:
        if not event.session_webhook or not can_use_session_webhook(msg):
            return False
        if event.session_webhook_expired_time <= 0:
            return True
        now_ms = int(time.time() * 1000)
        return now_ms < (event.session_webhook_expired_time - _WEBHOOK_EXPIRY_BUFFER_MS)

    async def send_group_message(
        self,
        open_conversation_id: str,
        message: str | Message | MessageSegment,
    ) -> Any:
        """Proactively send a message (text/markdown/media) to a DingTalk group."""
        return await self._send_units_to_group(
            open_conversation_id, _coerce_message(message)
        )

    async def send_private_message(
        self,
        user_ids: list[str],
        message: str | Message | MessageSegment,
    ) -> Any:
        """Proactively send a message to one or more enterprise userIds."""
        return await self._send_units_to_private(user_ids, _coerce_message(message))

    async def _send_units_to_group(
        self,
        open_conversation_id: str,
        msg: Message,
        *,
        sender_staff_id: str | None = None,
    ) -> Any:
        units = plan_openapi_units(msg)
        results: list[Any] = []
        for unit in units:
            try:
                msg_key, msg_param = await self._materialize_unit(unit)
            except MediaLimitExceeded:
                if (
                    unit.media_segment is not None
                    and unit.media_segment.type == "file"
                    and unit.media_segment.data.get("path")
                ):
                    union_id = await self._resolve_operator_union_id(sender_staff_id)
                    if union_id:
                        results.append(
                            await self.send_dingpan_file(
                                open_conversation_id,
                                unit.media_segment.data["path"],
                                union_id=union_id,
                                filename=unit.media_segment.data.get("name"),
                            )
                        )
                        continue
                raise

            res = await self.invoke(
                DingTalkAction.SEND_GROUP_MESSAGE,
                open_conversation_id=open_conversation_id,
                msg_key=msg_key,
                msg_param=msg_param,
            )
            results.append(res)
        return results[0] if len(results) == 1 else results

    async def _send_units_to_private(
        self,
        user_ids: list[str],
        msg: Message,
    ) -> Any:
        units = plan_openapi_units(msg)
        results: list[Any] = []
        for unit in units:
            msg_key, msg_param = await self._materialize_unit(unit)
            res = await self.invoke(
                DingTalkAction.SEND_PRIVATE_MESSAGE,
                user_ids=user_ids,
                msg_key=msg_key,
                msg_param=msg_param,
            )
            results.append(res)
        return results[0] if len(results) == 1 else results

    async def _materialize_unit(self, unit: OpenAPIUnit) -> tuple[str, T_JsonDict]:
        seg = unit.media_segment
        if seg is None:
            return unit.msg_key, unit.msg_param

        source = seg.data.get("path") or seg.data.get("content")
        if source is None:
            raise MediaError(
                f"Outbound {seg.type!r} segment requires path, content, or media_id."
            )

        match seg.type:
            case "image":
                media_id = await self.upload_media(
                    source, media_type=MediaType.IMAGE, filename=seg.data.get("name")
                )
                return "sampleImageMsg", {"photoURL": media_id}
            case "file":
                name = seg.data.get("name") or (
                    Path(source).name if isinstance(source, str | Path) else "file.bin"
                )
                file_type = (
                    seg.data.get("file_type")
                    or Path(name).suffix.lstrip(".").lower()
                    or "bin"
                )
                media_id = await self.upload_media(
                    source, media_type=MediaType.FILE, filename=name
                )
                return "sampleFile", {
                    "mediaId": media_id,
                    "fileName": name,
                    "fileType": file_type,
                }
            case "audio":
                media_id = await self.upload_media(
                    source, media_type=MediaType.VOICE, filename=seg.data.get("name")
                )
                return "sampleAudio", {
                    "mediaId": media_id,
                    "duration": str(seg.data.get("duration") or 1000),
                }
            case "video":
                pic_id = seg.data.get("pic_media_id")
                if not pic_id:
                    pic_src = seg.data.get("pic_path") or seg.data.get("pic_content")
                    if pic_src is None:
                        raise MediaError(
                            "Outbound video segment requires pic_media_id, "
                            "pic_path, or pic_content."
                        )
                    pic_id = await self.upload_media(
                        pic_src, media_type=MediaType.IMAGE, filename="thumb.jpg"
                    )
                vid_id = seg.data.get("media_id") or await self.upload_media(
                    source, media_type=MediaType.VIDEO, filename=seg.data.get("name")
                )
                return "sampleVideo", {
                    "videoMediaId": vid_id,
                    "videoType": str(seg.data.get("video_type") or "mp4"),
                    "duration": str(seg.data.get("duration") or 1000),
                    "picMediaId": str(pic_id),
                }
            case _:
                raise MediaError(f"Cannot upload unsupported segment: {seg.type!r}")

    async def _resolve_operator_union_id(
        self, sender_staff_id: str | None
    ) -> str | None:
        if self.bot_config.operator_union_id:
            return self.bot_config.operator_union_id
        if sender_staff_id:
            try:
                profile = await self.get_user_profile(sender_staff_id)
                return profile.unionid
            except Exception:
                return None
        return None

    async def download_media(
        self,
        target: str | MessageSegment,
        *,
        dest_dir: str | Path | None = None,
        filename: str | None = None,
        max_bytes: int | None = None,
    ) -> DownloadedMedia:
        """Stream an inbound image/file/audio/video to disk (supports GB-scale)."""
        code, seg_name, fallback_ext = _extract_download_info(target)
        return await self.invoke(
            DingTalkAction.DOWNLOAD_MEDIA,
            download_code=code,
            dest_dir=dest_dir,
            filename=filename or seg_name,
            fallback_ext=fallback_ext,
            max_bytes=max_bytes,
        )

    async def iter_media(
        self,
        target: str | MessageSegment,
        *,
        chunk_size: int | None = None,
        max_bytes: int | None = None,
    ) -> AsyncIterator[bytes]:
        """Yield streamed bytes of an inbound attachment without buffering in RAM."""
        code, _, _ = _extract_download_info(target)
        download_url: str = await self.invoke(
            DingTalkAction.EXCHANGE_DOWNLOAD_URL, download_code=code
        )
        async for chunk, _, _ in self.adapter._media_service.iter_url_chunks(
            download_url, chunk_size=chunk_size, max_bytes=max_bytes
        ):
            yield chunk

    async def read_media_bytes(
        self,
        target: str | MessageSegment,
        *,
        max_bytes: int = 32 * 1024 * 1024,
    ) -> bytes:
        """Read a small attachment into memory with an explicit byte guard."""
        code, _, _ = _extract_download_info(target)
        return await self.invoke(
            DingTalkAction.READ_MEDIA_BYTES,
            download_code=code,
            max_bytes=max_bytes,
        )

    async def upload_media(
        self,
        file: str | Path | bytes | IO[bytes],
        *,
        media_type: str | MediaType = MediaType.FILE,
        filename: str | None = None,
    ) -> str:
        """Upload media to oapi /media/upload and return its media_id."""
        return await self.invoke(
            DingTalkAction.UPLOAD_MEDIA,
            file=file,
            media_type=media_type,
            filename=filename,
        )

    async def send_dingpan_file(
        self,
        open_conversation_id: str,
        file_path: str | Path,
        *,
        union_id: str | None = None,
        filename: str | None = None,
    ) -> T_JsonDict:
        """Upload a >20MB file to DingPan via OSS PUT and deliver to a group chat."""
        resolved_union = union_id or self.bot_config.operator_union_id
        if not resolved_union:
            raise MediaError(
                "send_dingpan_file requires union_id or BotConfig.operator_union_id."
            )
        return await self.invoke(
            DingTalkAction.SEND_DINGPAN_FILE,
            open_conversation_id=open_conversation_id,
            file_path=file_path,
            union_id=resolved_union,
            filename=filename,
        )

    async def get_user_profile(
        self,
        staff_id: str,
        *,
        use_cache: bool = True,
    ) -> UserProfile:
        """Fetch enterprise UserProfile (with TTL cache) via topapi/v2/user/get."""
        return await self.invoke(
            DingTalkAction.GET_USER_PROFILE,
            staff_id=staff_id,
            use_cache=use_cache,
        )

    async def create_and_deliver_card(
        self,
        card_template_id: str,
        card_param_map: dict[str, Any],
        *,
        out_track_id: str | None = None,
        open_conversation_id: str | None = None,
        user_id: str | None = None,
        event: MessageEvent | None = None,
        callback_type: str = "STREAM",
    ) -> str:
        """Create and deliver a DingTalk interactive/AI card; returns outTrackId."""
        conv_id = open_conversation_id
        uid = user_id
        if event is not None and not conv_id and not uid:
            if isinstance(event, GroupMessageEvent):
                conv_id = event.conversation_id
            else:
                uid = event.get_user_id()

        res: T_JsonDict = await self.invoke(
            DingTalkAction.CREATE_AND_DELIVER_CARD,
            card_template_id=card_template_id,
            card_param_map=card_param_map,
            out_track_id=out_track_id,
            open_conversation_id=conv_id,
            user_id=uid,
            callback_type=callback_type,
        )
        return str(res["outTrackId"])

    async def update_card(
        self,
        out_track_id: str,
        card_param_map: dict[str, Any],
        *,
        user_private_map: dict[str, dict[str, Any]] | None = None,
    ) -> T_JsonDict:
        """Update an existing interactive card asynchronously via OpenAPI."""
        return await self.invoke(
            DingTalkAction.UPDATE_CARD,
            out_track_id=out_track_id,
            card_param_map=card_param_map,
            user_private_map=user_private_map,
        )

    async def stream_card(
        self,
        out_track_id: str,
        key: str,
        content: str,
        *,
        guid: str | None = None,
        is_full: bool = True,
        is_finalize: bool = False,
        is_error: bool = False,
    ) -> T_JsonDict:
        """Push incremental or final content to an AI streaming card."""
        return await self.invoke(
            DingTalkAction.STREAM_CARD,
            out_track_id=out_track_id,
            key=key,
            content=content,
            guid=guid,
            is_full=is_full,
            is_finalize=is_finalize,
            is_error=is_error,
        )

    def ai_card_stream(
        self,
        event: MessageEvent | None = None,
        *,
        card_template_id: str,
        content_key: str = "content",
        initial_text: str = "",
        open_conversation_id: str | None = None,
        user_id: str | None = None,
        out_track_id: str | None = None,
        throttle_interval: float = 0.15,
        error_fallback: str = "生成失败，请稍后重试。",
    ) -> AICardStream:
        """Create an AICardStream async context manager for LLM typewriter replies."""
        conv_id = open_conversation_id
        uid = user_id
        if event is not None and not conv_id and not uid:
            if isinstance(event, GroupMessageEvent):
                conv_id = event.conversation_id
            else:
                uid = event.get_user_id()
        return AICardStream(
            self,
            card_template_id=card_template_id,
            content_key=content_key,
            initial_text=initial_text,
            open_conversation_id=conv_id,
            user_id=uid,
            out_track_id=out_track_id,
            throttle_interval=throttle_interval,
            error_fallback=error_fallback,
        )


__all__ = ["Gateway"]
