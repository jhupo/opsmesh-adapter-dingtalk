"""Message and MessageSegment subclasses inheriting OpsMesh native primitives."""

from collections.abc import Iterable
from pathlib import Path
from typing import Any
from typing_extensions import override

from opsmesh.adapters import Message as BaseMessage
from opsmesh.adapters import MessageSegment as BaseMessageSegment


class MessageSegment(BaseMessageSegment["Message"]):
    """DingTalk multimodal message segment."""

    @classmethod
    @override
    def get_message_class(cls) -> type["Message"]:
        return Message

    @override
    def __str__(self) -> str:
        match self.type:
            case "text":
                return str(self.data.get("text", ""))
            case "at":
                if self.data.get("is_at_all"):
                    return "@all"
                target = self.data.get("staff_id") or self.data.get("user_id") or ""
                return f"@{target}" if target else "@"
            case "markdown":
                return str(self.data.get("text", ""))
            case _:
                label = (
                    self.data.get("name")
                    or self.data.get("title")
                    or self.data.get("media_id")
                    or self.data.get("download_code")
                    or ""
                )
                return f"[{self.type}:{label}]" if label else f"[{self.type}]"

    @override
    def is_text(self) -> bool:
        return self.type == "text"

    @classmethod
    def text(cls, text: str) -> "MessageSegment":
        return cls("text", {"text": text})

    @classmethod
    def at(
        cls,
        *,
        user_id: str | None = None,
        staff_id: str | None = None,
        is_at_all: bool = False,
    ) -> "MessageSegment":
        return cls(
            "at",
            {
                "user_id": user_id,
                "staff_id": staff_id,
                "is_at_all": is_at_all,
            },
        )

    @classmethod
    def markdown(cls, title: str, text: str) -> "MessageSegment":
        return cls("markdown", {"title": title, "text": text})

    @classmethod
    def image(
        cls,
        *,
        download_code: str | None = None,
        media_id: str | None = None,
        url: str | None = None,
        path: str | Path | None = None,
        content: bytes | None = None,
        name: str = "image.png",
    ) -> "MessageSegment":
        return cls(
            "image",
            {
                "download_code": download_code,
                "media_id": media_id,
                "url": url,
                "path": str(path) if path is not None else None,
                "content": content,
                "name": name,
            },
        )

    @classmethod
    def file(
        cls,
        *,
        download_code: str | None = None,
        media_id: str | None = None,
        path: str | Path | None = None,
        content: bytes | None = None,
        name: str | None = None,
        file_type: str | None = None,
        size: int | None = None,
    ) -> "MessageSegment":
        resolved_name = name or (Path(path).name if path is not None else None)
        resolved_type = file_type
        if not resolved_type and resolved_name:
            suffix = Path(resolved_name).suffix.lstrip(".").lower()
            resolved_type = suffix or None
        return cls(
            "file",
            {
                "download_code": download_code,
                "media_id": media_id,
                "path": str(path) if path is not None else None,
                "content": content,
                "name": resolved_name,
                "file_type": resolved_type,
                "size": size,
            },
        )

    @classmethod
    def audio(
        cls,
        *,
        download_code: str | None = None,
        media_id: str | None = None,
        path: str | Path | None = None,
        content: bytes | None = None,
        duration: int | None = None,
        recognition: str | None = None,
        name: str = "audio.amr",
    ) -> "MessageSegment":
        return cls(
            "audio",
            {
                "download_code": download_code,
                "media_id": media_id,
                "path": str(path) if path is not None else None,
                "content": content,
                "duration": duration,
                "recognition": recognition,
                "name": name,
            },
        )

    @classmethod
    def video(
        cls,
        *,
        download_code: str | None = None,
        media_id: str | None = None,
        path: str | Path | None = None,
        content: bytes | None = None,
        pic_media_id: str | None = None,
        pic_path: str | Path | None = None,
        pic_content: bytes | None = None,
        duration: int | None = None,
        video_type: str = "mp4",
        name: str = "video.mp4",
    ) -> "MessageSegment":
        return cls(
            "video",
            {
                "download_code": download_code,
                "media_id": media_id,
                "path": str(path) if path is not None else None,
                "content": content,
                "pic_media_id": pic_media_id,
                "pic_path": str(pic_path) if pic_path is not None else None,
                "pic_content": pic_content,
                "duration": duration,
                "video_type": video_type,
                "name": name,
            },
        )

    @classmethod
    def action_card(
        cls,
        title: str,
        text: str,
        *,
        single_title: str | None = None,
        single_url: str | None = None,
        btns: list[dict[str, str]] | None = None,
        btn_orientation: str = "0",
    ) -> "MessageSegment":
        data: dict[str, Any] = {
            "title": title,
            "text": text,
            "btn_orientation": btn_orientation,
        }
        if single_title is not None:
            data["single_title"] = single_title
        if single_url is not None:
            data["single_url"] = single_url
        if btns is not None:
            data["btns"] = btns
        return cls("action_card", data)


class Message(BaseMessage[MessageSegment]):
    """DingTalk multimodal message sequence."""

    @classmethod
    @override
    def get_segment_class(cls) -> type[MessageSegment]:
        return MessageSegment

    @staticmethod
    @override
    def _construct(msg: str) -> Iterable[MessageSegment]:
        yield MessageSegment.text(msg)


__all__ = ["Message", "MessageSegment"]
