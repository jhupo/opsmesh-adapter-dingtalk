"""Outbound serializer compiling Message into sessionWebhook or OpenAPI batches."""

from dataclasses import dataclass

from dingtalk.exception import DingTalkActionFailed
from dingtalk.typing import T_JsonDict

from .segment import Message, MessageSegment

_WEBHOOK_ALLOWED_TYPES = frozenset({"text", "at", "markdown", "action_card"})


@dataclass(slots=True, kw_only=True, frozen=True)
class OpenAPIUnit:
    """Single outbound OpenAPI message unit (msgKey + msgParam or pending media)."""

    msg_key: str
    msg_param: T_JsonDict
    media_segment: MessageSegment | None = None


def can_use_session_webhook(message: Message) -> bool:
    """Return True if message can be delivered directly via sessionWebhook."""
    if not message:
        return False
    types = {seg.type for seg in message}
    if not types.issubset(_WEBHOOK_ALLOWED_TYPES):
        return False
    rich_types = types & {"markdown", "action_card"}
    if len(rich_types) > 1:
        return False
    if rich_types and (
        "text" in types or sum(1 for s in message if s.type in rich_types) > 1
    ):
        return False
    return True


def build_session_webhook_payload(
    message: Message,
    *,
    at_user_ids: list[str] | None = None,
) -> T_JsonDict:
    """Compile a webhook-compatible Message into DingTalk sessionWebhook JSON."""
    collected_at_ids: list[str] = list(at_user_ids or [])
    is_at_all = False

    for seg in message:
        if seg.type == "at":
            if seg.data.get("is_at_all"):
                is_at_all = True
            target = seg.data.get("staff_id") or seg.data.get("user_id")
            if target and target not in collected_at_ids:
                collected_at_ids.append(str(target))

    at_block: T_JsonDict = {}
    if collected_at_ids:
        at_block["atUserIds"] = collected_at_ids
    if is_at_all:
        at_block["isAtAll"] = True

    markdown_segs = message["markdown"]
    if markdown_segs:
        seg = markdown_segs[0]
        payload: T_JsonDict = {
            "msgtype": "markdown",
            "markdown": {
                "title": str(seg.data.get("title", "Notification")),
                "text": str(seg.data.get("text", "")),
            },
        }
        if at_block:
            payload["at"] = at_block
        return payload

    card_segs = message["action_card"]
    if card_segs:
        seg = card_segs[0]
        card_body: T_JsonDict = {
            "title": str(seg.data.get("title", "")),
            "text": str(seg.data.get("text", "")),
            "btnOrientation": str(seg.data.get("btn_orientation", "0")),
        }
        if seg.data.get("btns"):
            card_body["btns"] = seg.data["btns"]
        else:
            card_body["singleTitle"] = str(seg.data.get("single_title", "查看详情"))
            card_body["singleURL"] = str(seg.data.get("single_url", ""))
        return {"msgtype": "actionCard", "actionCard": card_body}

    text_parts: list[str] = []
    for seg in message:
        if seg.type == "text":
            text_parts.append(str(seg.data.get("text", "")))
        elif seg.type == "at" and not seg.data.get("is_at_all"):
            target = seg.data.get("staff_id") or seg.data.get("user_id")
            if target:
                text_parts.append(f"@{target} ")

    payload = {
        "msgtype": "text",
        "text": {"content": "".join(text_parts)},
    }
    if at_block:
        payload["at"] = at_block
    return payload


def plan_openapi_units(message: Message) -> list[OpenAPIUnit]:
    """Split a multimodal Message into ordered DingTalk OpenAPI send units.

    Consecutive text/at segments are coalesced into a single sampleText unit;
    each markdown, action_card, image, file, audio, or video becomes its own unit.
    """
    units: list[OpenAPIUnit] = []
    text_buf: list[str] = []

    def _flush_text() -> None:
        if not text_buf:
            return
        joined = "".join(text_buf)
        text_buf.clear()
        if joined:
            units.append(
                OpenAPIUnit(msg_key="sampleText", msg_param={"content": joined})
            )

    for seg in message:
        match seg.type:
            case "text":
                text_buf.append(str(seg.data.get("text", "")))
            case "at":
                text_buf.append(str(seg) + " ")
            case "markdown":
                _flush_text()
                units.append(
                    OpenAPIUnit(
                        msg_key="sampleMarkdown",
                        msg_param={
                            "title": str(seg.data.get("title", "Notification")),
                            "text": str(seg.data.get("text", "")),
                        },
                    )
                )
            case "action_card":
                _flush_text()
                param: T_JsonDict = {
                    "title": str(seg.data.get("title", "")),
                    "text": str(seg.data.get("text", "")),
                    "singleTitle": str(seg.data.get("single_title", "查看详情")),
                    "singleURL": str(seg.data.get("single_url", "")),
                }
                units.append(OpenAPIUnit(msg_key="sampleActionCard", msg_param=param))
            case "image":
                _flush_text()
                photo = seg.data.get("media_id") or seg.data.get("url")
                if photo:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleImageMsg",
                            msg_param={"photoURL": str(photo)},
                        )
                    )
                else:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleImageMsg",
                            msg_param={},
                            media_segment=seg,
                        )
                    )
            case "file":
                _flush_text()
                media_id = seg.data.get("media_id")
                if media_id:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleFile",
                            msg_param={
                                "mediaId": str(media_id),
                                "fileName": str(seg.data.get("name") or "file.bin"),
                                "fileType": str(seg.data.get("file_type") or "bin"),
                            },
                        )
                    )
                else:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleFile",
                            msg_param={},
                            media_segment=seg,
                        )
                    )
            case "audio":
                _flush_text()
                media_id = seg.data.get("media_id")
                if media_id:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleAudio",
                            msg_param={
                                "mediaId": str(media_id),
                                "duration": str(seg.data.get("duration") or 1000),
                            },
                        )
                    )
                else:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleAudio",
                            msg_param={},
                            media_segment=seg,
                        )
                    )
            case "video":
                _flush_text()
                media_id = seg.data.get("media_id")
                pic_id = seg.data.get("pic_media_id")
                if media_id and pic_id:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleVideo",
                            msg_param={
                                "videoMediaId": str(media_id),
                                "videoType": str(seg.data.get("video_type") or "mp4"),
                                "duration": str(seg.data.get("duration") or 1000),
                                "picMediaId": str(pic_id),
                            },
                        )
                    )
                else:
                    units.append(
                        OpenAPIUnit(
                            msg_key="sampleVideo",
                            msg_param={},
                            media_segment=seg,
                        )
                    )
            case _:
                raise DingTalkActionFailed(
                    f"Unsupported outbound MessageSegment type: {seg.type!r}"
                )

    _flush_text()
    return units


__all__ = [
    "OpenAPIUnit",
    "build_session_webhook_payload",
    "can_use_session_webhook",
    "plan_openapi_units",
]
