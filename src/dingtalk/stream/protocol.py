"""Pure wire models for Stream handshake, SYSTEM/CALLBACK/EVENT frames, and ACK."""

from dataclasses import dataclass
import json
from typing import Any

from opsmesh.drivers import URL

from dingtalk.config import BotConfig
from dingtalk.exception import StreamProtocolError
from dingtalk.typing import FrameType, StreamSubscription, StreamTopic, T_JsonDict

OPEN_CONNECTION_PATH = "/v1.0/gateway/connections/open"
DEFAULT_STREAM_UA = "opsmesh-adapter-dingtalk/0.1.0"


@dataclass(slots=True, kw_only=True, frozen=True)
class StreamFrame:
    """Decoded inbound DingTalk Stream WebSocket frame."""

    type: str
    topic: str
    message_id: str
    headers: T_JsonDict
    data: T_JsonDict
    raw_data: str


def build_subscriptions(bot: BotConfig) -> list[StreamSubscription]:
    """Build subscription list for /v1.0/gateway/connections/open."""
    subs: list[StreamSubscription] = [
        {"type": FrameType.CALLBACK, "topic": StreamTopic.BOT_MESSAGE}
    ]
    if bot.subscribe_card_callbacks:
        subs.append({"type": FrameType.CALLBACK, "topic": StreamTopic.CARD_CALLBACK})
    if bot.subscribe_events:
        subs.append({"type": FrameType.EVENT, "topic": StreamTopic.ALL_EVENTS})
    return subs


def build_open_connection_payload(
    bot: BotConfig, *, ua: str = DEFAULT_STREAM_UA
) -> T_JsonDict:
    """Construct the JSON body for opening a DingTalk Stream connection."""
    return {
        "clientId": bot.client_id,
        "clientSecret": bot.client_secret.get_secret_value(),
        "subscriptions": build_subscriptions(bot),
        "ua": ua,
    }


def build_wss_url(endpoint: str, ticket: str) -> URL:
    """Validate and construct the authenticated WSS connection URL."""
    if not endpoint or not ticket:
        raise StreamProtocolError("Missing endpoint or ticket in Stream handshake.")
    uri = URL(endpoint)
    if uri.scheme != "wss" or not uri.host or uri.user or uri.password:
        raise StreamProtocolError(
            f"Unsafe or invalid Stream WSS endpoint: {endpoint!r}"
        )
    return uri.update_query(ticket=ticket)


def parse_stream_frame(raw: str | bytes) -> StreamFrame:
    """Parse a raw WebSocket text/bytes message into a StreamFrame."""
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise StreamProtocolError(f"Invalid Stream JSON frame: {exc}") from exc

    if not isinstance(payload, dict):
        raise StreamProtocolError("Stream frame root must be a JSON object.")

    headers = payload.get("headers")
    if not isinstance(headers, dict):
        headers = {}

    raw_data_field: Any = payload.get("data", "")
    if isinstance(raw_data_field, dict):
        parsed_data = raw_data_field
        raw_data_str = json.dumps(raw_data_field, ensure_ascii=False)
    elif isinstance(raw_data_field, str):
        raw_data_str = raw_data_field
        if raw_data_field.strip():
            try:
                loaded = json.loads(raw_data_field)
                parsed_data = loaded if isinstance(loaded, dict) else {}
            except json.JSONDecodeError:
                parsed_data = {}
        else:
            parsed_data = {}
    else:
        parsed_data = {}
        raw_data_str = ""

    return StreamFrame(
        type=str(payload.get("type", "")),
        topic=str(headers.get("topic", "")),
        message_id=str(headers.get("messageId", "")),
        headers=headers,
        data=parsed_data,
        raw_data=raw_data_str,
    )


def build_ack_frame(
    message_id: str,
    *,
    data: T_JsonDict | str | None = None,
    code: int = 200,
    message: str = "OK",
) -> str:
    """Serialize a DingTalk Stream AckMessage frame."""
    if data is None:
        encoded_data = json.dumps({"response": None})
    elif isinstance(data, str):
        encoded_data = data
    else:
        encoded_data = json.dumps(data, ensure_ascii=False)

    ack: T_JsonDict = {
        "code": code,
        "headers": {
            "contentType": "application/json",
            "messageId": message_id,
        },
        "message": message,
        "data": encoded_data,
    }
    return json.dumps(ack, ensure_ascii=False)


__all__ = [
    "DEFAULT_STREAM_UA",
    "OPEN_CONNECTION_PATH",
    "StreamFrame",
    "build_ack_frame",
    "build_open_connection_payload",
    "build_subscriptions",
    "build_wss_url",
    "parse_stream_frame",
]
