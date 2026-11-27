"""Python 3.11+ StrEnum constants, TypeAliases, and TypedDict wire contracts."""

from enum import StrEnum
from typing import Any, TypeAlias, TypedDict

ADAPTER_NAME: str = "dingtalk"

T_JsonDict: TypeAlias = dict[str, Any]


class FrameType(StrEnum):
    """DingTalk Stream WebSocket frame types."""

    SYSTEM = "SYSTEM"
    CALLBACK = "CALLBACK"
    EVENT = "EVENT"


class StreamTopic(StrEnum):
    """Standard DingTalk Stream subscription topics."""

    PING = "ping"
    DISCONNECT = "disconnect"
    BOT_MESSAGE = "/v1.0/im/bot/messages/get"
    CARD_CALLBACK = "/v1.0/card/instances/callback"
    ALL_EVENTS = "*"


class ConversationType(StrEnum):
    """DingTalk conversation types."""

    PRIVATE = "1"
    GROUP = "2"


class DingTalkMsgType(StrEnum):
    """Inbound chatbot message types."""

    TEXT = "text"
    PICTURE = "picture"
    RICH_TEXT = "richText"
    FILE = "file"
    AUDIO = "audio"
    VIDEO = "video"
    UNKNOWN = "unknown"


class MediaType(StrEnum):
    """DingTalk oapi /media/upload supported media types."""

    IMAGE = "image"
    VOICE = "voice"
    VIDEO = "video"
    FILE = "file"


class SpaceType(StrEnum):
    """DingTalk interactive card space types."""

    IM_GROUP = "IM_GROUP"
    IM_ROBOT = "IM_ROBOT"


class DingTalkAction(StrEnum):
    """Registered outbound actions routed through Gateway.invoke -> Adapter._invoke."""

    GET_ACCESS_TOKEN = "get_access_token"  # noqa: S105
    SEND_SESSION_WEBHOOK = "send_session_webhook"
    SEND_GROUP_MESSAGE = "send_group_message"
    SEND_PRIVATE_MESSAGE = "send_private_message"
    EXCHANGE_DOWNLOAD_URL = "exchange_download_url"
    DOWNLOAD_MEDIA = "download_media"
    READ_MEDIA_BYTES = "read_media_bytes"
    UPLOAD_MEDIA = "upload_media"
    SEND_DINGPAN_FILE = "send_dingpan_file"
    GET_USER_PROFILE = "get_user_profile"
    CREATE_AND_DELIVER_CARD = "create_and_deliver_card"
    UPDATE_CARD = "update_card"
    STREAM_CARD = "stream_card"


class StreamSubscription(TypedDict):
    """Subscription item sent to POST /v1.0/gateway/connections/open."""

    type: str
    topic: str


__all__ = [
    "ADAPTER_NAME",
    "ConversationType",
    "DingTalkAction",
    "DingTalkMsgType",
    "FrameType",
    "MediaType",
    "SpaceType",
    "StreamSubscription",
    "StreamTopic",
    "T_JsonDict",
]
