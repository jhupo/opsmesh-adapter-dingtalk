"""Inbound parser converting DingTalk msgtype JSON payloads into Message segments."""

import json
from typing import Any

from dingtalk.typing import DingTalkMsgType, T_JsonDict

from .segment import Message, MessageSegment


def _as_dict(raw: Any) -> T_JsonDict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _extract_download_code(item: T_JsonDict) -> str | None:
    code = item.get("downloadCode") or item.get("pictureDownloadCode")
    return str(code) if isinstance(code, str) and code else None


def parse_chatbot_message(payload: T_JsonDict, *, is_group: bool = False) -> Message:
    """Parse inbound DingTalk chatbot JSON into a non-empty Message sequence.

    All attachments are stored lazily as metadata references (download_code)
    without performing any network I/O during event construction.
    """
    msgtype = str(payload.get("msgtype") or DingTalkMsgType.UNKNOWN)
    content = _as_dict(payload.get("content"))
    message = Message()

    match msgtype:
        case DingTalkMsgType.TEXT:
            text_obj = _as_dict(payload.get("text"))
            raw_text = str(text_obj.get("content", content.get("content", "")))
            if is_group:
                raw_text = raw_text.lstrip()
            message.append(MessageSegment.text(raw_text))

        case DingTalkMsgType.PICTURE:
            code = _extract_download_code(content)
            message.append(MessageSegment.image(download_code=code))

        case DingTalkMsgType.RICH_TEXT:
            items = content.get("richText")
            if isinstance(items, list):
                first_text = True
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if "text" in item and item["text"] is not None:
                        snippet = str(item["text"])
                        if is_group and first_text:
                            snippet = snippet.lstrip()
                            first_text = False
                        if snippet:
                            message.append(MessageSegment.text(snippet))
                    elif (code := _extract_download_code(item)) is not None:
                        message.append(MessageSegment.image(download_code=code))

        case DingTalkMsgType.FILE:
            raw_size = content.get("fileSize")
            size = int(raw_size) if isinstance(raw_size, int | float) else None
            message.append(
                MessageSegment.file(
                    download_code=_extract_download_code(content),
                    name=content.get("fileName"),
                    size=size,
                )
            )

        case DingTalkMsgType.AUDIO:
            raw_duration = content.get("duration")
            duration = (
                int(raw_duration) if isinstance(raw_duration, int | float) else None
            )
            recognition = content.get("recognition")
            message.append(
                MessageSegment.audio(
                    download_code=_extract_download_code(content),
                    duration=duration,
                    recognition=str(recognition) if recognition is not None else None,
                )
            )

        case DingTalkMsgType.VIDEO:
            raw_duration = content.get("duration")
            duration = (
                int(raw_duration) if isinstance(raw_duration, int | float) else None
            )
            video_type = str(content.get("videoType") or "mp4")
            message.append(
                MessageSegment.video(
                    download_code=_extract_download_code(content),
                    duration=duration,
                    video_type=video_type,
                    name=f"video.{video_type}",
                )
            )

        case _:
            message.append(
                MessageSegment("unknown", {"msgtype": msgtype, "content": content})
            )

    if not message:
        message.append(
            MessageSegment("unknown", {"msgtype": msgtype, "content": content})
        )
    return message


__all__ = ["parse_chatbot_message"]
