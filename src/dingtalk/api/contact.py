"""Enterprise directory topapi/v2/user/get client with TTL memory cache."""

import time

from dingtalk.event.models import UserProfile
from dingtalk.exception import DingTalkActionFailed

from .client import ApiClient


class ContactService:
    """Query enterprise employee profiles via topapi/v2/user/get with TTL cache."""

    def __init__(self, client: ApiClient, *, cache_ttl: float = 300.0) -> None:
        self._client = client
        self._cache_ttl = cache_ttl
        self._cache: dict[str, tuple[UserProfile, float]] = {}

    async def get_user(
        self,
        *,
        access_token: str,
        staff_id: str,
        use_cache: bool = True,
    ) -> UserProfile:
        """Fetch UserProfile by enterprise userid (senderStaffId)."""
        now = time.monotonic()
        if use_cache and self._cache_ttl > 0 and staff_id in self._cache:
            profile, expires_at = self._cache[staff_id]
            if now < expires_at:
                return profile
            del self._cache[staff_id]

        body = await self._client.request_oapi(
            "POST",
            "/topapi/v2/user/get",
            access_token=access_token,
            json_body={"userid": staff_id, "language": "zh_CN"},
        )
        result = body.get("result")
        if not isinstance(result, dict):
            raise DingTalkActionFailed(
                f"DingTalk user/get returned empty result for userid={staff_id!r}"
            )
        profile = UserProfile.model_validate(result)
        if self._cache_ttl > 0:
            self._cache[staff_id] = (profile, time.monotonic() + self._cache_ttl)
        return profile

    def invalidate(self, staff_id: str | None = None) -> None:
        """Clear a specific cached profile or the entire cache."""
        if staff_id is None:
            self._cache.clear()
        else:
            self._cache.pop(staff_id, None)


__all__ = ["ContactService"]
