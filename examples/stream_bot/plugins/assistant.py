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
        f"Nick: {event.sender_nick}\n"
        f"Staff ID: {event.get_user_id()}\n"
        f"Internal user: {event.is_internal_user}\n"
        f"Admin: {event.is_admin}"
    )
    await whoami.finish(MessageSegment.markdown("Identity", reply))


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
        await file_cmd.finish("Received attachments:\n" + "\n".join(saved))


@card_cb.handle()
async def handle_card_click(event: CardCallbackEvent) -> None:
    if event.params.get("action") == "approve":
        event.reply_card(
            card_param_map={"status": "Approved", "approver": event.user_id},
            user_private_map={"tip": "Approval recorded"},
        )


inspect_cmd = on_command("inspect", rule=to_me())


@inspect_cmd.handle()
async def handle_inspect(args: Annotated[Message, CommandArg()]) -> None:
    target = args.extract_plain_text().strip() or "default"
    await inspect_cmd.finish(f"Target {target} is healthy.")
