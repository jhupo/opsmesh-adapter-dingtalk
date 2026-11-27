"""DingTalk event hierarchy inheriting directly from opsmesh.adapters.Event."""

from .base import DingTalkEvent, LifecycleMetaEvent
from .card import CardCallbackEvent, parse_card_callback_event
from .message import (
    GroupMessageEvent,
    MessageEvent,
    PrivateMessageEvent,
    parse_chatbot_event,
)
from .models import AtUser, DownloadedMedia, RoleItem, UserProfile
from .notice import PlatformNoticeEvent, parse_platform_notice_event

__all__ = [
    "AtUser",
    "CardCallbackEvent",
    "DingTalkEvent",
    "DownloadedMedia",
    "GroupMessageEvent",
    "LifecycleMetaEvent",
    "MessageEvent",
    "PlatformNoticeEvent",
    "PrivateMessageEvent",
    "RoleItem",
    "UserProfile",
    "parse_card_callback_event",
    "parse_chatbot_event",
    "parse_platform_notice_event",
]
