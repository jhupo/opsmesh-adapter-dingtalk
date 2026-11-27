"""CardCallbackEvent (notice) with 2.0s synchronous/asynchronous ACK coordination."""

import json
from typing import Any
from typing_extensions import override

import anyio
from pydantic import Field, PrivateAttr

from dingtalk.message import Message
from dingtalk.typing import T_JsonDict

from .base import DingTalkEvent


def _build_card_ack_response(
    *,
    card_param_map: dict[str, Any] | None = None,
    user_private_map: dict[str, Any] | None = None,
    user_id: str | None = None,
) -> T_JsonDict:
    response: T_JsonDict = {}
    if card_param_map is not None:
        response["cardData"] = {
            "cardParamMap": {str(k): str(v) for k, v in card_param_map.items()}
        }
    if user_private_map is not None:
        key = user_id or ""
        if key:
            response["userPrivateDataMap"] = {
                key: {
                    "cardParamMap": {
                        str(k): str(v) for k, v in user_private_map.items()
                    }
                }
            }
        else:
            response["userPrivateData"] = {
                "cardParamMap": {str(k): str(v) for k, v in user_private_map.items()}
            }
    return response


class CardCallbackEvent(DingTalkEvent):
    """Interactive card button / form callback (/v1.0/card/instances/callback)."""

    out_track_id: str = Field(default="", alias="outTrackId")
    user_id: str = Field(default="", alias="userId")
    corp_id: str = Field(default="", alias="corpId")
    space_type: str = Field(default="", alias="spaceType")
    space_id: str = Field(default="", alias="spaceId")
    action_ids: list[str] = Field(default_factory=list)
    params: T_JsonDict = Field(default_factory=dict)

    _ack_event: anyio.Event = PrivateAttr(default_factory=anyio.Event)
    _ack_payload: T_JsonDict | None = PrivateAttr(default=None)
    _ack_sent: bool = PrivateAttr(default=False)

    @override
    def get_type(self) -> str:
        return "notice"

    @override
    def get_event_name(self) -> str:
        return "notice.card_callback"

    @override
    def get_event_description(self) -> str:
        return (
            f"CardCallback out_track_id={self.out_track_id} "
            f"user={self.user_id} actions={self.action_ids}"
        )

    @override
    def get_user_id(self) -> str:
        return self.user_id

    @override
    def get_session_id(self) -> str:
        return f"card_{self.out_track_id}_{self.user_id}"

    @override
    def get_message(self) -> Message:
        raise ValueError("CardCallbackEvent has no message.")

    @override
    def is_tome(self) -> bool:
        return True

    def reply_card(
        self,
        *,
        card_param_map: dict[str, Any] | None = None,
        user_private_map: dict[str, Any] | None = None,
    ) -> None:
        """Attach card updates to the Stream ACK frame (zero extra HTTP call)."""
        if self._ack_sent:
            return
        self._ack_payload = _build_card_ack_response(
            card_param_map=card_param_map,
            user_private_map=user_private_map,
            user_id=self.user_id,
        )
        self._ack_event.set()

    async def ack_card(
        self,
        *,
        card_param_map: dict[str, Any] | None = None,
        user_private_map: dict[str, Any] | None = None,
    ) -> None:
        """Release the Stream ACK immediately before continuing a long-running task."""
        self.reply_card(
            card_param_map=card_param_map,
            user_private_map=user_private_map,
        )
        await anyio.lowlevel.checkpoint()

    def _consume_ack_payload(self) -> T_JsonDict:
        self._ack_sent = True
        return self._ack_payload if self._ack_payload is not None else {}


def parse_card_callback_event(
    payload: T_JsonDict, *, robot_code: str = ""
) -> CardCallbackEvent:
    """Parse Stream /v1.0/card/instances/callback payload into CardCallbackEvent."""
    raw_content = payload.get("content")
    if isinstance(raw_content, str):
        try:
            content_dict = json.loads(raw_content)
        except json.JSONDecodeError:
            content_dict = {}
    elif isinstance(raw_content, dict):
        content_dict = raw_content
    else:
        content_dict = {}

    private_data = content_dict.get("cardPrivateData")
    if not isinstance(private_data, dict):
        private_data = {}

    raw_actions = private_data.get("actionIds")
    action_ids = [str(a) for a in raw_actions] if isinstance(raw_actions, list) else []
    raw_params = private_data.get("params")
    params = dict(raw_params) if isinstance(raw_params, dict) else {}

    event_kwargs: T_JsonDict = dict(payload)
    event_kwargs["action_ids"] = action_ids
    event_kwargs["params"] = params
    event_kwargs["raw_payload"] = payload
    if robot_code and not event_kwargs.get("robotCode"):
        event_kwargs["robotCode"] = robot_code

    return CardCallbackEvent.model_validate(event_kwargs)


__all__ = ["CardCallbackEvent", "parse_card_callback_event"]
