"""Double-checked anyio.Lock access_token manager with early refresh buffer."""

import time

import anyio

from dingtalk.config import BotConfig
from dingtalk.exception import DingTalkActionFailed, TokenError

from .client import ApiClient

_REFRESH_BUFFER_SECONDS = 300.0


class TokenManager:
    """Per-bot access_token cache with concurrency-safe double-checked locking."""

    def __init__(self, client: ApiClient, bot_config: BotConfig) -> None:
        self._client = client
        self._bot_config = bot_config
        self._lock = anyio.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    async def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid access_token, refreshing safely when near expiry."""
        now = time.monotonic()
        if not force_refresh and self._token and now < self._expires_at:
            return self._token

        async with self._lock:
            now = time.monotonic()
            if not force_refresh and self._token and now < self._expires_at:
                return self._token
            return await self._fetch_token()

    async def _fetch_token(self) -> str:
        try:
            body = await self._client.request_openapi(
                "POST",
                "/v1.0/oauth2/accessToken",
                json_body={
                    "appKey": self._bot_config.client_id,
                    "appSecret": self._bot_config.client_secret.get_secret_value(),
                },
            )
        except DingTalkActionFailed as exc:
            raise TokenError(
                exc.message,
                status_code=exc.status_code,
                code=exc.code,
                request_id=exc.request_id,
            ) from exc

        token = body.get("accessToken")
        expire_in = body.get("expireIn", 7200)
        if not isinstance(token, str) or not token:
            raise TokenError("DingTalk accessToken response missing 'accessToken'.")

        ttl = float(expire_in) if isinstance(expire_in, int | float) else 7200.0
        self._token = token
        self._expires_at = time.monotonic() + max(60.0, ttl - _REFRESH_BUFFER_SECONDS)
        return token


__all__ = ["TokenManager"]
