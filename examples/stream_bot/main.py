"""Client-only DingTalk Stream bot entrypoint using OpsMesh."""

from pathlib import Path

import opsmesh

from dingtalk import Adapter as DingTalkAdapter


def main() -> None:
    opsmesh.init(driver="~httpx+~websockets")
    opsmesh.register_adapter(DingTalkAdapter)
    opsmesh.load_from_toml(Path(__file__).with_name("pyproject.toml"))
    opsmesh.run()


if __name__ == "__main__":
    main()
