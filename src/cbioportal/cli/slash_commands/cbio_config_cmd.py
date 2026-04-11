"""/cbio-config — read or write ~/.cbio/config.toml from inside the TUI."""
from __future__ import annotations

from pathlib import Path
from typing import List

import tomli_w
from cli_textual.core.command import SlashCommand

try:
    import tomllib  # py311+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

CONFIG_PATH = Path("~/.cbio/config.toml").expanduser()


def _read_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return tomllib.loads(CONFIG_PATH.read_text())
    except Exception:
        return {}


def _write_config(cfg: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(tomli_w.dumps(cfg))


class CbioConfigCommand(SlashCommand):
    name = "/cbio-config"
    description = "Read or write ~/.cbio/config.toml (e.g. db_path)"

    async def execute(self, app, args: List[str]) -> None:
        if not args:
            cfg = _read_config()
            if not cfg:
                app.add_to_history(
                    f"_No config at_ `{CONFIG_PATH}`.\n\n"
                    "Set a value with `/cbio-config <key> <value>`."
                )
                return
            lines = [f"**{CONFIG_PATH}**", ""]
            for k, v in sorted(cfg.items()):
                lines.append(f"- `{k}` = `{v}`")
            app.add_to_history("\n".join(lines))
            return

        if len(args) == 1:
            cfg = _read_config()
            v = cfg.get(args[0])
            app.add_to_history(
                f"`{args[0]}` = `{v}`" if v is not None else f"_unset:_ `{args[0]}`"
            )
            return

        key, value = args[0], " ".join(args[1:])
        cfg = _read_config()
        cfg[key] = value
        _write_config(cfg)
        app.add_to_history(f"✅ Set `{key}` = `{value}` in {CONFIG_PATH}")
