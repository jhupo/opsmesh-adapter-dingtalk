"""DingTalk Stream protocol adapter for OpsMesh."""

from importlib.metadata import PackageNotFoundError, version

from .adapter import Adapter
from .api import AICardStream
from .config import BotConfig, Config
from .event import (
    AtUser,
    CardCallbackEvent,
    DingTalkEvent,
    DownloadedMedia,
    GroupMessageEvent,
    LifecycleMetaEvent,
    MessageEvent,
    PlatformNoticeEvent,
    PrivateMessageEvent,
    RoleItem,
    UserProfile,
)
from .exception import (
    DingTalkActionFailed,
    DingTalkAdapterException,
    DingTalkNetworkError,
    MediaError,
    MediaLimitExceeded,
    StreamProtocolError,
    TokenError,
)
from .gateway import Gateway
from .message import Message, MessageSegment
from .typing import (
    ADAPTER_NAME,
    ConversationType,
    DingTalkAction,
    DingTalkMsgType,
    FrameType,
    MediaType,
    SpaceType,
    StreamTopic,
)

try:
    __version__: str | None = version("opsmesh-adapter-dingtalk")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.1.0"

__all__ = [
    "ADAPTER_NAME",
    "AICardStream",
    "Adapter",
    "AtUser",
    "BotConfig",
    "CardCallbackEvent",
    "Config",
    "ConversationType",
    "DingTalkAction",
    "DingTalkActionFailed",
    "DingTalkAdapterException",
    "DingTalkEvent",
    "DingTalkMsgType",
    "DingTalkNetworkError",
    "DownloadedMedia",
    "FrameType",
    "Gateway",
    "GroupMessageEvent",
    "LifecycleMetaEvent",
    "MediaError",
    "MediaLimitExceeded",
    "MediaType",
    "Message",
    "MessageEvent",
    "MessageSegment",
    "PlatformNoticeEvent",
    "PrivateMessageEvent",
    "RoleItem",
    "SpaceType",
    "StreamProtocolError",
    "StreamTopic",
    "TokenError",
    "UserProfile",
    "__version__",
]
