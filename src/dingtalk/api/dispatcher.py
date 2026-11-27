"""Declarative action registry dispatching Gateway.invoke() to API services."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from dingtalk.api.card import CardService
from dingtalk.api.contact import ContactService
from dingtalk.api.media import MediaService
from dingtalk.api.messaging import MessagingService
from dingtalk.api.token import TokenManager
from dingtalk.exception import ActionNotAvailable
from dingtalk.typing import ADAPTER_NAME, DingTalkAction, T_JsonDict


@dataclass(slots=True, kw_only=True, frozen=True)
class ActionContext:
    """Execution context passed to registered DingTalk action handlers."""

    robot_code: str
    token_manager: TokenManager
    params: T_JsonDict

    async def token(self, *, force_refresh: bool = False) -> str:
        return await self.token_manager.get_token(force_refresh=force_refresh)


ActionHandler = Callable[[ActionContext], Awaitable[Any]]


class ActionDispatcher:
    """Map DingTalkAction names to service methods without monolithic match walls."""

    def __init__(
        self,
        *,
        messaging: MessagingService,
        media: MediaService,
        cards: CardService,
        contacts: ContactService,
    ) -> None:
        self._messaging = messaging
        self._media = media
        self._cards = cards
        self._contacts = contacts
        self._handlers: dict[str, ActionHandler] = {
            DingTalkAction.GET_ACCESS_TOKEN: self._get_access_token,
            DingTalkAction.SEND_SESSION_WEBHOOK: self._send_session_webhook,
            DingTalkAction.SEND_GROUP_MESSAGE: self._send_group_message,
            DingTalkAction.SEND_PRIVATE_MESSAGE: self._send_private_message,
            DingTalkAction.EXCHANGE_DOWNLOAD_URL: self._exchange_download_url,
            DingTalkAction.DOWNLOAD_MEDIA: self._download_media,
            DingTalkAction.READ_MEDIA_BYTES: self._read_media_bytes,
            DingTalkAction.UPLOAD_MEDIA: self._upload_media,
            DingTalkAction.SEND_DINGPAN_FILE: self._send_dingpan_file,
            DingTalkAction.GET_USER_PROFILE: self._get_user_profile,
            DingTalkAction.CREATE_AND_DELIVER_CARD: self._create_and_deliver_card,
            DingTalkAction.UPDATE_CARD: self._update_card,
            DingTalkAction.STREAM_CARD: self._stream_card,
        }

    async def dispatch(
        self,
        *,
        robot_code: str,
        token_manager: TokenManager,
        action: str,
        params: T_JsonDict,
    ) -> Any:
        handler = self._handlers.get(action)
        if handler is None:
            raise ActionNotAvailable(ADAPTER_NAME, action)
        ctx = ActionContext(
            robot_code=robot_code,
            token_manager=token_manager,
            params=params,
        )
        return await handler(ctx)

    async def _get_access_token(self, ctx: ActionContext) -> str:
        return await ctx.token(
            force_refresh=bool(ctx.params.get("force_refresh", False))
        )

    async def _send_session_webhook(self, ctx: ActionContext) -> T_JsonDict:
        return await self._messaging.send_session_webhook(
            webhook_url=ctx.params["webhook_url"],
            payload=ctx.params["payload"],
        )

    async def _send_group_message(self, ctx: ActionContext) -> T_JsonDict:
        return await self._messaging.send_group_message(
            access_token=await ctx.token(),
            robot_code=ctx.robot_code,
            open_conversation_id=ctx.params["open_conversation_id"],
            msg_key=ctx.params["msg_key"],
            msg_param=ctx.params["msg_param"],
        )

    async def _send_private_message(self, ctx: ActionContext) -> T_JsonDict:
        return await self._messaging.send_private_message(
            access_token=await ctx.token(),
            robot_code=ctx.robot_code,
            user_ids=list(ctx.params["user_ids"]),
            msg_key=ctx.params["msg_key"],
            msg_param=ctx.params["msg_param"],
        )

    async def _exchange_download_url(self, ctx: ActionContext) -> str:
        return await self._media.exchange_download_url(
            access_token=await ctx.token(),
            robot_code=ctx.robot_code,
            download_code=ctx.params["download_code"],
        )

    async def _download_media(self, ctx: ActionContext) -> Any:
        return await self._media.download_to_disk(
            access_token=await ctx.token(),
            robot_code=ctx.robot_code,
            download_code=ctx.params["download_code"],
            dest_dir=ctx.params.get("dest_dir"),
            filename=ctx.params.get("filename"),
            fallback_ext=ctx.params.get("fallback_ext"),
            max_bytes=ctx.params.get("max_bytes"),
        )

    async def _read_media_bytes(self, ctx: ActionContext) -> bytes:
        return await self._media.read_bytes(
            access_token=await ctx.token(),
            robot_code=ctx.robot_code,
            download_code=ctx.params["download_code"],
            max_bytes=ctx.params.get("max_bytes", 32 * 1024 * 1024),
        )

    async def _upload_media(self, ctx: ActionContext) -> str:
        return await self._media.upload_media(
            access_token=await ctx.token(),
            file=ctx.params["file"],
            media_type=ctx.params.get("media_type", "file"),
            filename=ctx.params.get("filename"),
        )

    async def _send_dingpan_file(self, ctx: ActionContext) -> T_JsonDict:
        return await self._media.send_dingpan_file(
            access_token=await ctx.token(),
            open_conversation_id=ctx.params["open_conversation_id"],
            file_path=ctx.params["file_path"],
            union_id=ctx.params["union_id"],
            filename=ctx.params.get("filename"),
        )

    async def _get_user_profile(self, ctx: ActionContext) -> Any:
        return await self._contacts.get_user(
            access_token=await ctx.token(),
            staff_id=ctx.params["staff_id"],
            use_cache=bool(ctx.params.get("use_cache", True)),
        )

    async def _create_and_deliver_card(self, ctx: ActionContext) -> T_JsonDict:
        return await self._cards.create_and_deliver(
            access_token=await ctx.token(),
            robot_code=ctx.robot_code,
            card_template_id=ctx.params["card_template_id"],
            card_param_map=ctx.params["card_param_map"],
            out_track_id=ctx.params.get("out_track_id"),
            open_conversation_id=ctx.params.get("open_conversation_id"),
            user_id=ctx.params.get("user_id"),
            callback_type=ctx.params.get("callback_type", "STREAM"),
        )

    async def _update_card(self, ctx: ActionContext) -> T_JsonDict:
        return await self._cards.update_card(
            access_token=await ctx.token(),
            out_track_id=ctx.params["out_track_id"],
            card_param_map=ctx.params["card_param_map"],
            user_private_map=ctx.params.get("user_private_map"),
        )

    async def _stream_card(self, ctx: ActionContext) -> T_JsonDict:
        return await self._cards.stream_card(
            access_token=await ctx.token(),
            out_track_id=ctx.params["out_track_id"],
            key=ctx.params["key"],
            content=ctx.params["content"],
            guid=ctx.params.get("guid"),
            is_full=bool(ctx.params.get("is_full", True)),
            is_finalize=bool(ctx.params.get("is_finalize", False)),
            is_error=bool(ctx.params.get("is_error", False)),
        )


__all__ = ["ActionContext", "ActionDispatcher", "ActionHandler"]
