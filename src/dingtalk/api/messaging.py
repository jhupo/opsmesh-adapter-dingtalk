"""DingTalk outbound messaging service (sessionWebhook, group, and 1-on-1 OpenAPI)."""

import json
from typing import ClassVar

from dingtalk.api.client import ApiClient
from dingtalk.typing import T_JsonDict


def encode_msg_param(msg_param: T_JsonDict) -> str:
    """Serialize DingTalk msgParam dict as compact UTF-8 JSON string."""
    return json.dumps(msg_param, ensure_ascii=False)


class MessagingService:
    """Encapsulate DingTalk outbound message endpoints and JSON wire schemas."""

    GROUP_SEND_PATH: ClassVar[str] = "/v1.0/robot/groupMessages/send"
    PRIVATE_BATCH_SEND_PATH: ClassVar[str] = "/v1.0/robot/oToMessages/batchSend"

    def __init__(self, client: ApiClient) -> None:
        self._client = client

    async def send_session_webhook(
        self,
        *,
        webhook_url: str,
        payload: T_JsonDict,
    ) -> T_JsonDict:
        """Send a reply through the temporary sessionWebhook endpoint."""
        return await self._client.post_webhook(webhook_url, payload)

    async def send_group_message(
        self,
        *,
        access_token: str,
        robot_code: str,
        open_conversation_id: str,
        msg_key: str,
        msg_param: T_JsonDict,
    ) -> T_JsonDict:
        """Send a template message to a DingTalk group chat via OpenAPI."""
        return await self._client.request_openapi(
            "POST",
            self.GROUP_SEND_PATH,
            access_token=access_token,
            json_body={
                "robotCode": robot_code,
                "openConversationId": open_conversation_id,
                "msgKey": msg_key,
                "msgParam": encode_msg_param(msg_param),
            },
        )

    async def send_private_message(
        self,
        *,
        access_token: str,
        robot_code: str,
        user_ids: list[str],
        msg_key: str,
        msg_param: T_JsonDict,
    ) -> T_JsonDict:
        """Send a template message to one or more users in 1-on-1 chat via OpenAPI."""
        return await self._client.request_openapi(
            "POST",
            self.PRIVATE_BATCH_SEND_PATH,
            access_token=access_token,
            json_body={
                "robotCode": robot_code,
                "userIds": list(user_ids),
                "msgKey": msg_key,
                "msgParam": encode_msg_param(msg_param),
            },
        )


__all__ = ["MessagingService", "encode_msg_param"]
