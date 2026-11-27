"""Multimodal Message & MessageSegment inheriting opsmesh.adapters primitives."""

from .parser import parse_chatbot_message
from .segment import Message, MessageSegment
from .serializer import (
    OpenAPIUnit,
    build_session_webhook_payload,
    can_use_session_webhook,
    plan_openapi_units,
)

__all__ = [
    "Message",
    "MessageSegment",
    "OpenAPIUnit",
    "build_session_webhook_payload",
    "can_use_session_webhook",
    "parse_chatbot_message",
    "plan_openapi_units",
]
