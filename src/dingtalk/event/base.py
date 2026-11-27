"""DingTalkEvent base class and LifecycleMetaEvent inheriting opsmesh.adapters.Event."""

from typing_extensions import override

from opsmesh.adapters import Event as BaseEvent
from pydantic import ConfigDict, Field

from dingtalk.exception import NoLogException
from dingtalk.message import Message
from dingtalk.typing import ADAPTER_NAME, T_JsonDict


class DingTalkEvent(BaseEvent):
    """Abstract base class for all DingTalk adapter events."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    robot_code: str = Field(default="", alias="robotCode")
    raw_payload: T_JsonDict = Field(default_factory=dict, exclude=True)


class LifecycleMetaEvent(DingTalkEvent):
    """Connection lifecycle meta-event (connected / disconnected / reconnecting)."""

    sub_type: str
    gateway_id: str
    silent: bool = False

    @override
    def get_type(self) -> str:
        return "meta_event"

    @override
    def get_event_name(self) -> str:
        return f"meta_event.{self.sub_type}"

    @override
    def get_event_description(self) -> str:
        return f"gateway={self.gateway_id} state={self.sub_type}"

    @override
    def get_log_string(self) -> str:
        if self.silent:
            raise NoLogException(ADAPTER_NAME)
        return super().get_log_string()

    @override
    def get_user_id(self) -> str:
        raise ValueError("LifecycleMetaEvent has no user_id.")

    @override
    def get_session_id(self) -> str:
        return f"meta_{self.gateway_id}"

    @override
    def get_message(self) -> Message:
        raise ValueError("LifecycleMetaEvent has no message.")

    @override
    def is_tome(self) -> bool:
        return False


__all__ = ["DingTalkEvent", "LifecycleMetaEvent"]
