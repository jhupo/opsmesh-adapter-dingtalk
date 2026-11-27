"""DingTalk OpenAPI/OAPI client, token lock, messaging, media, card, and contact."""

from dingtalk.api.card import AICardStream, CardService, derive_open_space
from dingtalk.api.client import ApiClient
from dingtalk.api.contact import ContactService
from dingtalk.api.dispatcher import ActionContext, ActionDispatcher, ActionHandler
from dingtalk.api.media import (
    MEDIA_UPLOAD_LIMITS,
    MediaService,
    upgrade_oss_https,
    validate_download_origin,
)
from dingtalk.api.messaging import MessagingService, encode_msg_param
from dingtalk.api.token import TokenManager

__all__ = [
    "MEDIA_UPLOAD_LIMITS",
    "AICardStream",
    "ActionContext",
    "ActionDispatcher",
    "ActionHandler",
    "ApiClient",
    "CardService",
    "ContactService",
    "MediaService",
    "MessagingService",
    "TokenManager",
    "derive_open_space",
    "encode_msg_param",
    "upgrade_oss_https",
    "validate_download_origin",
]
