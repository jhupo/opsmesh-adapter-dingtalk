"""Example plugin: commands, user RBAC, large file downloads, and card callbacks."""

from typing import Annotated

from opsmesh import on_command, on_message, on_type
from opsmesh.params import CommandArg, EventMessage
from opsmesh.rule import Rule, to_me

from dingtalk import CardCallbackEvent, Gateway, Message, MessageEvent, MessageSegment


def _has_file_or_image() -> Rule:
    async def _check(msg: Annotated[Message, EventMessage()]) -> bool:
        return "file" in msg or "image" in msg

    return Rule(_check)


whoami = on_command("whoami", rule=to_me())
file_cmd = on_message(rule=_has_file_or_image(), priority=20)
card_cb = on_type(CardCallbackEvent)


@whoami.handle()
async def handle_whoami(event: MessageEvent) -> None:
    reply = (
        f"昵称：{event.sender_nick}\n"
        f"员工 ID：{event.get_user_id()}\n"
        f"内部员工：{event.is_internal_user}\n"
        f"企业管理员：{event.is_admin}"
    )
    await whoami.finish(MessageSegment.markdown("当前身份", reply))


@file_cmd.handle()
async def handle_file(
    gateway: Gateway,
    msg: Annotated[Message, EventMessage()],
) -> None:
    saved: list[str] = []
    for seg in msg.include("file", "image"):
        media = await gateway.download_media(seg)
        saved.append(f"{media.name} ({media.size} bytes)")
    if saved:
        await file_cmd.finish("已流式接收附件：\n" + "\n".join(saved))


@card_cb.handle()
async def handle_card_click(event: CardCallbackEvent) -> None:
    if event.params.get("action") == "approve":
        event.reply_card(
            card_param_map={"status": "已批准", "approver": event.user_id},
            user_private_map={"tip": "审批操作已生效"},
        )


inspect_cmd = on_command("inspect", rule=to_me())


@inspect_cmd.handle()
async def handle_inspect(args: Annotated[Message, CommandArg()]) -> None:
    target = args.extract_plain_text().strip() or "default"
    await inspect_cmd.finish(f"巡检目标 {target} 状态正常。")
