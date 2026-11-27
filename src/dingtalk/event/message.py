"""MessageEvent, PrivateMessageEvent, and GroupMessageEvent models."""

from datetime import UTC, datetime
from typing_extensions import override

from pydantic import Field

from dingtalk.message import Message, parse_chatbot_message
from dingtalk.typing import ConversationType, T_JsonDict

from .base import DingTalkEvent
from .models import AtUser


class MessageEvent(DingTalkEvent):
    """Base chatbot message event dispatched through OpsMesh handle_event()."""

    msg_id: str = Field(default="", alias="msgId")
    msgtype: str = "text"
    conversation_id: str = Field(default="", alias="conversationId")
    conversation_type: str = Field(
        default=ConversationType.PRIVATE, alias="conversationType"
    )
    chatbot_corp_id: str = Field(default="", alias="chatbotCorpId")
    chatbot_user_id: str = Field(default="", alias="chatbotUserId")
    sender_id: str = Field(default="", alias="senderId")
    sender_staff_id: str = Field(default="", alias="senderStaffId")
    sender_nick: str = Field(default="", alias="senderNick")
    sender_corp_id: str = Field(default="", alias="senderCorpId")
    sender_platform: str = Field(default="", alias="senderPlatform")
    is_admin: bool = Field(default=False, alias="isAdmin")
    session_webhook: str = Field(default="", alias="sessionWebhook")
    session_webhook_expired_time: int = Field(
        default=0, alias="sessionWebhookExpiredTime"
    )
    create_at: int | None = Field(default=None, alias="createAt")
    message: Message = Field(default_factory=Message)

    @property
    def is_internal_user(self) -> bool:
        """True when sender belongs to the same DingTalk corp as this bot."""
        return bool(self.sender_corp_id and self.sender_corp_id == self.chatbot_corp_id)

    @override
    def get_type(self) -> str:
        return "message"

    @override
    def get_event_description(self) -> str:
        return (
            f"Message {self.msg_id} from {self.sender_nick or self.get_user_id()} "
            f"({self.get_user_id()}): {self.message!s}"
        )

    @override
    def get_user_id(self) -> str:
        return self.sender_staff_id or self.sender_id

    @override
    def get_message(self) -> Message:
        return self.message


class PrivateMessageEvent(MessageEvent):
    """Direct 1-on-1 message sent to the DingTalk bot."""

    @override
    def get_event_name(self) -> str:
        return f"message.private.{self.msgtype}"

    @override
    def get_session_id(self) -> str:
        return f"private_{self.get_user_id()}"

    @override
    def is_tome(self) -> bool:
        return True


class GroupMessageEvent(MessageEvent):
    """Group chat message delivered to the DingTalk bot."""

    conversation_title: str = Field(default="", alias="conversationTitle")
    is_in_at_list: bool = Field(default=False, alias="isInAtList")
    at_users: list[AtUser] = Field(default_factory=list, alias="atUsers")

    @override
    def get_event_name(self) -> str:
        return f"message.group.{self.msgtype}"

    @override
    def get_session_id(self) -> str:
        return f"group_{self.conversation_id}_{self.get_user_id()}"

    @override
    def is_tome(self) -> bool:
        if self.is_in_at_list:
            return True
        if not self.chatbot_user_id:
            return False
        return any(u.dingtalk_id == self.chatbot_user_id for u in self.at_users)


def parse_chatbot_event(payload: T_JsonDict) -> MessageEvent:
    """Construct PrivateMessageEvent or GroupMessageEvent from Stream callback data."""
    conv_type = str(payload.get("conversationType") or ConversationType.PRIVATE)
    is_group = conv_type == ConversationType.GROUP
    parsed_msg = parse_chatbot_message(payload, is_group=is_group)

    event_kwargs: T_JsonDict = dict(payload)
    event_kwargs["message"] = parsed_msg
    event_kwargs["raw_payload"] = payload

    create_at = payload.get("createAt")
    if isinstance(create_at, int | float) and create_at > 0:
        event_kwargs["time"] = datetime.fromtimestamp(create_at / 1000.0, tz=UTC)

    event_cls: type[MessageEvent] = (
        GroupMessageEvent if is_group else PrivateMessageEvent
    )
    return event_cls.model_validate(event_kwargs)


__all__ = [
    "GroupMessageEvent",
    "MessageEvent",
    "PrivateMessageEvent",
    "parse_chatbot_event",
]
