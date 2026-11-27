"""Tests for multimodal MessageSegment parsing, serialization, and Event models."""

from opsmesh.adapters import Event

from dingtalk import (
    CardCallbackEvent,
    GroupMessageEvent,
    Message,
    MessageSegment,
    PlatformNoticeEvent,
    PrivateMessageEvent,
)
from dingtalk.event import (
    parse_card_callback_event,
    parse_chatbot_event,
    parse_platform_notice_event,
)
from dingtalk.message import (
    build_session_webhook_payload,
    can_use_session_webhook,
    plan_openapi_units,
)


def test_parse_private_and_group_text_with_at_stripping() -> None:
    private_ev = parse_chatbot_event(
        {
            "conversationType": "1",
            "conversationId": "cid_priv",
            "msgId": "m1",
            "msgtype": "text",
            "chatbotCorpId": "corp_1",
            "senderCorpId": "corp_1",
            "senderStaffId": "staff_001",
            "senderId": "$:enc:1",
            "senderNick": "Alice",
            "isAdmin": True,
            "text": {"content": "/deploy api"},
        }
    )
    assert isinstance(private_ev, PrivateMessageEvent)
    assert isinstance(private_ev, Event)
    assert private_ev.is_tome() is True
    assert private_ev.get_user_id() == "staff_001"
    assert private_ev.get_session_id() == "private_staff_001"
    assert private_ev.is_internal_user is True
    assert private_ev.is_admin is True
    assert private_ev.get_plaintext() == "/deploy api"

    group_ev = parse_chatbot_event(
        {
            "conversationType": "2",
            "conversationId": "cid_grp",
            "conversationTitle": "Ops Group",
            "msgId": "m2",
            "msgtype": "text",
            "chatbotCorpId": "corp_1",
            "chatbotUserId": "$:bot:1",
            "senderCorpId": "corp_ext",
            "senderId": "$:enc:2",
            "isInAtList": True,
            "atUsers": [{"dingtalkId": "$:bot:1"}],
            "text": {"content": "   /deploy worker"},
        }
    )
    assert isinstance(group_ev, GroupMessageEvent)
    assert group_ev.is_tome() is True
    assert group_ev.is_internal_user is False
    assert group_ev.get_user_id() == "$:enc:2"
    assert group_ev.get_session_id() == "group_cid_grp_$:enc:2"
    assert group_ev.get_plaintext() == "/deploy worker"


def test_parse_richtext_file_audio_video_and_unknown_fallback() -> None:
    rich_ev = parse_chatbot_event(
        {
            "conversationType": "2",
            "conversationId": "cid_g",
            "msgtype": "richText",
            "content": {
                "richText": [
                    {"text": "  /ocr "},
                    {"type": "picture", "downloadCode": "code_1"},
                    {"type": "picture", "pictureDownloadCode": "code_2"},
                ]
            },
        }
    )
    msg = rich_ev.get_message()
    assert msg.extract_plain_text() == "/ocr "
    assert len(msg["image"]) == 2
    assert msg["image", 0].data["download_code"] == "code_1"
    assert msg["image", 1].data["download_code"] == "code_2"

    file_ev = parse_chatbot_event(
        {
            "conversationType": "1",
            "msgtype": "file",
            "content": {
                "downloadCode": "f_code",
                "fileName": "logs.tar.gz",
                "fileSize": 1048576,
            },
        }
    )
    fseg = file_ev.get_message()["file", 0]
    assert fseg.data["download_code"] == "f_code"
    assert fseg.data["name"] == "logs.tar.gz"
    assert fseg.data["size"] == 1048576

    audio_ev = parse_chatbot_event(
        {
            "conversationType": "1",
            "msgtype": "audio",
            "content": {
                "downloadCode": "a_code",
                "duration": 3500,
                "recognition": "重启服务",
            },
        }
    )
    assert audio_ev.get_message()["audio", 0].data["recognition"] == "重启服务"

    unknown_ev = parse_chatbot_event({"conversationType": "1", "msgtype": "mystery"})
    assert len(unknown_ev.get_message()) == 1
    assert unknown_ev.get_message()[0].type == "unknown"


def test_parse_card_callback_and_platform_notice_events() -> None:
    card_ev = parse_card_callback_event(
        {
            "outTrackId": "track_1",
            "userId": "staff_009",
            "corpId": "corp_1",
            "spaceType": "IM_GROUP",
            "spaceId": "cid_1",
            "content": (
                '{"cardPrivateData":{"actionIds":["btn_ok"],'
                '"params":{"action":"approve","service":"api"}}}'
            ),
        },
        robot_code="ding_bot",
    )
    assert isinstance(card_ev, CardCallbackEvent)
    assert card_ev.get_type() == "notice"
    assert card_ev.get_user_id() == "staff_009"
    assert card_ev.is_tome() is True
    assert card_ev.action_ids == ["btn_ok"]
    assert card_ev.params == {"action": "approve", "service": "api"}

    notice_ev = parse_platform_notice_event(
        {
            "eventId": "ev_100",
            "eventType": "bpms_task_change",
            "eventCorpId": "corp_1",
            "eventBornTime": "1710000000000",
        },
        {"staffId": "staff_002", "result": "agree"},
        robot_code="ding_bot",
    )
    assert isinstance(notice_ev, PlatformNoticeEvent)
    assert notice_ev.get_type() == "notice"
    assert notice_ev.event_type == "bpms_task_change"
    assert notice_ev.get_user_id() == "staff_002"


def test_serializer_webhook_and_openapi_splitting() -> None:
    text_msg = Message("hello ") + MessageSegment.at(staff_id="u1")
    assert can_use_session_webhook(text_msg) is True
    wh = build_session_webhook_payload(text_msg, at_user_ids=["u2"])
    assert wh["msgtype"] == "text"
    assert set(wh["at"]["atUserIds"]) == {"u1", "u2"}

    mixed_msg = Message("report:") + MessageSegment.file(
        media_id="@mid_1", name="daily.pdf"
    )
    assert can_use_session_webhook(mixed_msg) is False
    units = plan_openapi_units(mixed_msg)
    assert len(units) == 2
    assert units[0].msg_key == "sampleText"
    assert units[1].msg_key == "sampleFile"
    assert units[1].msg_param["fileType"] == "pdf"
