"""Exception hierarchy inheriting directly from opsmesh.exception primitives."""

from opsmesh.exception import (
    ActionFailed,
    ActionNotAvailable,
    AdapterException,
    NetworkError,
    NoLogException,
)

from .typing import ADAPTER_NAME


class DingTalkAdapterException(AdapterException):
    """Base exception for all DingTalk adapter errors."""

    def __init__(self, message: str = "") -> None:
        super().__init__(ADAPTER_NAME, message)
        self.message: str = message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(message={self.message!r})"


class StreamProtocolError(DingTalkAdapterException):
    """Invalid Stream handshake payload, unsafe WSS endpoint, or malformed frame."""


class DingTalkNetworkError(DingTalkAdapterException, NetworkError):
    """HTTP or WebSocket transport failure communicating with DingTalk."""


class DingTalkActionFailed(DingTalkAdapterException, ActionFailed):
    """DingTalk OpenAPI or OAPI returned a non-success status or errcode."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | int | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code: int | None = status_code
        self.code: str | int | None = code
        self.request_id: str | None = request_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"status_code={self.status_code!r}, "
            f"code={self.code!r}, "
            f"message={self.message!r}, "
            f"request_id={self.request_id!r})"
        )


class TokenError(DingTalkActionFailed):
    """Failed to acquire or refresh a DingTalk access_token."""


class MediaError(DingTalkAdapterException):
    """Media link exchange, SSRF guard, download, or upload error."""


class MediaLimitExceeded(MediaError):
    """File size exceeds DingTalk upload limit or configured max_bytes."""

    def __init__(
        self,
        message: str,
        *,
        limit_bytes: int,
        actual_bytes: int | None = None,
    ) -> None:
        super().__init__(message)
        self.limit_bytes: int = limit_bytes
        self.actual_bytes: int | None = actual_bytes


__all__ = [
    "ActionNotAvailable",
    "DingTalkActionFailed",
    "DingTalkAdapterException",
    "DingTalkNetworkError",
    "MediaError",
    "MediaLimitExceeded",
    "NoLogException",
    "StreamProtocolError",
    "TokenError",
]
