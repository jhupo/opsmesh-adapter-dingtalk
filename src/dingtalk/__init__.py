"""DingTalk Stream protocol adapter for OpsMesh."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str | None = version("opsmesh-adapter-dingtalk")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.1.0"


from .adapter import Adapter as Adapter
from .api import AICardStream as AICardStream
from .config import BotConfig as BotConfig
from .config import Config as Config
from .event import AtUser as AtUser
from .event import CardCallbackEvent as CardCallbackEvent
from .event import DingTalkEvent as DingTalkEvent
from .event import DownloadedMedia as DownloadedMedia
from .event import GroupMessageEvent as GroupMessageEvent
from .event import LifecycleMetaEvent as LifecycleMetaEvent
from .event import MessageEvent as MessageEvent
from .event import PlatformNoticeEvent as PlatformNoticeEvent
from .event import PrivateMessageEvent as PrivateMessageEvent
from .event import RoleItem as RoleItem
from .event import UserProfile as UserProfile
from .exception import DingTalkActionFailed as DingTalkActionFailed
from .exception import DingTalkAdapterException as DingTalkAdapterException
from .exception import DingTalkNetworkError as DingTalkNetworkError
from .exception import MediaError as MediaError
from .exception import MediaLimitExceeded as MediaLimitExceeded
from .exception import StreamProtocolError as StreamProtocolError
from .exception import TokenError as TokenError
from .gateway import Gateway as Gateway
from .message import Message as Message
from .message import MessageSegment as MessageSegment
from .typing import ADAPTER_NAME as ADAPTER_NAME
from .typing import ConversationType as ConversationType
from .typing import DingTalkAction as DingTalkAction
from .typing import DingTalkMsgType as DingTalkMsgType
from .typing import FrameType as FrameType
from .typing import MediaType as MediaType
from .typing import SpaceType as SpaceType
from .typing import StreamTopic as StreamTopic
