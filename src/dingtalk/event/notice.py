"""PlatformNoticeEvent (notice) for subscribed DingTalk enterprise EVENT frames."""

from datetime import UTC, datetime
from typing_extensions import override

from pydantic import Field

from dingtalk.message import Message
from dingtalk.typing import T_JsonDict

from .base import DingTalkEvent


class PlatformNoticeEvent(DingTalkEvent):
    """Enterprise event subscription notification (EVENT:* Stream frames)."""

    event_id: str = ""
    event_type: str = ""
    corp_id: str = ""
    data: T_JsonDict = Field(default_factory=dict)

    @override
    def get_type(self) -> str:
        return "notice"

    @override
    def get_event_name(self) -> str:
        return f"notice.platform.{self.event_type or 'unknown'}"

    @override
    def get_event_description(self) -> str:
        return f"PlatformEvent {self.event_type} id={self.event_id} corp={self.corp_id}"

    @override
    def get_user_id(self) -> str:
        uid = self.data.get("staffId") or self.data.get("userId")
        if uid:
            return str(uid)
        raise ValueError("PlatformNoticeEvent has no user_id.")

    @override
    def get_session_id(self) -> str:
        return f"event_{self.event_type}_{self.corp_id}"

    @override
    def get_message(self) -> Message:
        raise ValueError("PlatformNoticeEvent has no message.")

    @override
    def is_tome(self) -> bool:
        return False


def parse_platform_notice_event(
    headers: T_JsonDict,
    data: T_JsonDict,
    *,
    robot_code: str = "",
) -> PlatformNoticeEvent:
    """Construct PlatformNoticeEvent from Stream EVENT frame headers and data."""
    kwargs: T_JsonDict = {
        "event_id": str(headers.get("eventId") or headers.get("messageId") or ""),
        "event_type": str(headers.get("eventType") or headers.get("topic") or ""),
        "corp_id": str(headers.get("eventCorpId") or ""),
        "robotCode": robot_code,
        "data": data,
        "raw_payload": {"headers": headers, "data": data},
    }
    born_time = headers.get("eventBornTime")
    if born_time is not None:
        try:
            ts_ms = int(born_time)
            if ts_ms > 0:
                kwargs["time"] = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
        except (TypeError, ValueError):
            pass

    return PlatformNoticeEvent.model_validate(kwargs)


__all__ = ["PlatformNoticeEvent", "parse_platform_notice_event"]
