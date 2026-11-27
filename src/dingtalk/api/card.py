"""Interactive card delivery/update and throttled AICardStream context manager."""

import time
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self
import uuid

import anyio
from opsmesh.exception import FinishedException

from dingtalk.typing import SpaceType, T_JsonDict
from dingtalk.utils import log

from .client import ApiClient

if TYPE_CHECKING:
    from dingtalk.gateway import Gateway


def derive_open_space(
    *,
    robot_code: str,
    open_conversation_id: str | None = None,
    user_id: str | None = None,
) -> tuple[str, T_JsonDict]:
    """Derive DingTalk openSpaceId and deliver model fields for group or 1-on-1 chat."""
    if open_conversation_id:
        open_space_id = f"dtv1.card//{SpaceType.IM_GROUP}.{open_conversation_id}"
        models: T_JsonDict = {
            "imGroupOpenSpaceModel": {"supportForward": True},
            "imGroupOpenDeliverModel": {"robotCode": robot_code},
        }
        return open_space_id, models

    if user_id:
        open_space_id = f"dtv1.card//{SpaceType.IM_ROBOT}.{user_id}"
        models = {
            "imRobotOpenSpaceModel": {"supportForward": True},
            "imRobotOpenDeliverModel": {"spaceType": SpaceType.IM_ROBOT},
        }
        return open_space_id, models

    raise ValueError("Either open_conversation_id or user_id must be provided.")


class CardService:
    """OpenAPI wrapper for DingTalk interactive and AI streaming cards."""

    def __init__(self, client: ApiClient) -> None:
        self._client = client

    async def create_and_deliver(
        self,
        *,
        access_token: str,
        robot_code: str,
        card_template_id: str,
        card_param_map: dict[str, Any],
        out_track_id: str | None = None,
        open_conversation_id: str | None = None,
        user_id: str | None = None,
        callback_type: str = "STREAM",
    ) -> T_JsonDict:
        """Create and deliver a card instance, defaulting callbackType='STREAM'."""
        track_id = out_track_id or uuid.uuid4().hex
        open_space_id, deliver_models = derive_open_space(
            robot_code=robot_code,
            open_conversation_id=open_conversation_id,
            user_id=user_id,
        )
        body: T_JsonDict = {
            "cardTemplateId": card_template_id,
            "outTrackId": track_id,
            "callbackType": callback_type,
            "cardData": {
                "cardParamMap": {str(k): str(v) for k, v in card_param_map.items()}
            },
            "openSpaceId": open_space_id,
            **deliver_models,
        }
        resp = await self._client.request_openapi(
            "POST",
            "/v1.0/card/instances/createAndDeliver",
            access_token=access_token,
            json_body=body,
        )
        return {"outTrackId": track_id, **resp}

    async def update_card(
        self,
        *,
        access_token: str,
        out_track_id: str,
        card_param_map: dict[str, Any],
        user_private_map: dict[str, dict[str, Any]] | None = None,
    ) -> T_JsonDict:
        """Update an existing interactive card via PUT /v1.0/card/instances."""
        body: T_JsonDict = {
            "outTrackId": out_track_id,
            "cardData": {
                "cardParamMap": {str(k): str(v) for k, v in card_param_map.items()}
            },
        }
        if user_private_map:
            body["privateData"] = {
                uid: {"cardParamMap": {str(k): str(v) for k, v in params.items()}}
                for uid, params in user_private_map.items()
            }
        return await self._client.request_openapi(
            "PUT",
            "/v1.0/card/instances",
            access_token=access_token,
            json_body=body,
        )

    async def stream_card(
        self,
        *,
        access_token: str,
        out_track_id: str,
        key: str,
        content: str,
        guid: str | None = None,
        is_full: bool = True,
        is_finalize: bool = False,
        is_error: bool = False,
    ) -> T_JsonDict:
        """Push incremental or final content to an AI streaming card."""
        body: T_JsonDict = {
            "outTrackId": out_track_id,
            "guid": guid or uuid.uuid4().hex,
            "key": key,
            "content": content,
            "isFull": is_full,
            "isFinalize": is_finalize,
            "isError": is_error,
        }
        return await self._client.request_openapi(
            "PUT",
            "/v1.0/card/streaming",
            access_token=access_token,
            json_body=body,
        )


class AICardStream:
    """Async context manager for throttled LLM typewriter updates on AI cards."""

    def __init__(
        self,
        gateway: "Gateway",
        *,
        card_template_id: str,
        content_key: str = "content",
        initial_text: str = "",
        open_conversation_id: str | None = None,
        user_id: str | None = None,
        out_track_id: str | None = None,
        throttle_interval: float = 0.15,
        error_fallback: str = "Generation failed. Please try again later.",
    ) -> None:
        self._gateway = gateway
        self._card_template_id = card_template_id
        self._content_key = content_key
        self._initial_text = initial_text
        self._open_conversation_id = open_conversation_id
        self._user_id = user_id
        self._out_track_id = out_track_id or uuid.uuid4().hex
        self._throttle_interval = throttle_interval
        self._error_fallback = error_fallback
        self._buffer: str = ""
        self._last_flush: float = 0.0
        self._delivered: bool = False

    @property
    def out_track_id(self) -> str:
        return self._out_track_id

    @property
    def content(self) -> str:
        return self._buffer

    async def __aenter__(self) -> Self:
        await self._gateway.create_and_deliver_card(
            card_template_id=self._card_template_id,
            card_param_map={self._content_key: self._initial_text},
            out_track_id=self._out_track_id,
            open_conversation_id=self._open_conversation_id,
            user_id=self._user_id,
        )
        self._delivered = True
        self._last_flush = time.monotonic()
        return self

    async def append(self, chunk: str, *, force: bool = False) -> None:
        """Append streamed token text, flushing according to throttle_interval."""
        self._buffer += chunk
        now = time.monotonic()
        if force or (now - self._last_flush) >= self._throttle_interval:
            await self.flush()

    async def set_content(self, text: str, *, force: bool = False) -> None:
        """Replace the full buffer and optionally flush immediately."""
        self._buffer = text
        now = time.monotonic()
        if force or (now - self._last_flush) >= self._throttle_interval:
            await self.flush()

    async def flush(self) -> None:
        """Send the current buffer to PUT /v1.0/card/streaming."""
        await self._gateway.stream_card(
            out_track_id=self._out_track_id,
            key=self._content_key,
            content=self._buffer,
            is_full=True,
            is_finalize=False,
            is_error=False,
        )
        self._last_flush = time.monotonic()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if not self._delivered:
            return
        is_normal = exc_type is None or issubclass(exc_type, FinishedException)
        final_text = (
            self._buffer if is_normal else (self._buffer or self._error_fallback)
        )
        with anyio.CancelScope(shield=True):
            try:
                await self._gateway.stream_card(
                    out_track_id=self._out_track_id,
                    key=self._content_key,
                    content=final_text,
                    is_full=True,
                    is_finalize=True,
                    is_error=not is_normal,
                )
            except Exception as cleanup_exc:
                if is_normal:
                    raise
                log(
                    "WARNING",
                    f"Failed to finalize AI card {self._out_track_id} on error",
                    exception=cleanup_exc,
                )


__all__ = ["AICardStream", "CardService", "derive_open_space"]
