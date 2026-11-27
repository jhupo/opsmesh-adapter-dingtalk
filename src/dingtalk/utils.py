"""OpsMesh loguru wrapper (with escape_tag) and bounded LRU+TTL DedupeRing."""

from collections import OrderedDict
from pathlib import Path
import re
import time

from opsmesh.log import logger
from opsmesh.utils import escape_tag

_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def log(level: str, message: str, exception: BaseException | None = None) -> None:
    """Emit a tagged log message through OpsMesh's native loguru logger."""
    logger.opt(colors=True, exception=exception).log(
        level, f"<y>DingTalk</y> | {escape_tag(message)}"
    )


def sanitize_filename(name: str | None, fallback: str = "attachment.bin") -> str:
    """Strip directory traversal and unsafe characters from a vendor filename."""
    if not name:
        return fallback
    base = Path(name.replace("\\", "/")).name
    cleaned = _INVALID_FILENAME_CHARS.sub("_", base).strip(" .")
    if not cleaned or cleaned in {".", ".."}:
        return fallback
    return cleaned[:200]


class DedupeRing:
    """Bounded LRU + TTL deduplication set for Stream messageId / eventId."""

    def __init__(self, *, capacity: int = 2048, ttl: float = 600.0) -> None:
        self._capacity = max(1, capacity)
        self._ttl = ttl
        self._entries: OrderedDict[str, float] = OrderedDict()

    def seen_or_record(self, key: str) -> bool:
        """Return True if key was already seen within TTL; otherwise record it."""
        now = time.monotonic()
        self._evict_expired(now)

        if key in self._entries:
            self._entries.move_to_end(key)
            return True

        self._entries[key] = now + self._ttl
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)
        return False

    def _evict_expired(self, now: float) -> None:
        while self._entries:
            oldest_key, expires_at = next(iter(self._entries.items()))
            if expires_at > now:
                break
            del self._entries[oldest_key]

    def __len__(self) -> int:
        self._evict_expired(time.monotonic())
        return len(self._entries)


__all__ = ["DedupeRing", "log", "sanitize_filename"]
