"""Slash command registry and tab completer for the cbio TUI."""
from __future__ import annotations

from typing import Callable

from prompt_toolkit.completion import Completer, Completion

COMMANDS: dict[str, tuple[str, Callable]] = {}


def register(name: str, description: str):
    def decorator(fn: Callable):
        COMMANDS[name] = (description, fn)
        return fn
    return decorator


def dispatch(cmd: str, state) -> str | None:
    """Dispatch a slash command. Returns response string or None if unknown."""
    parts = cmd.strip().split()
    name = parts[0].lower()
    if name in COMMANDS:
        _, fn = COMMANDS[name]
        return fn(state, parts[1:])
    return f"Unknown command: {name}. Type help for help."


@register("/help", "Show available commands")
def _help(state, args) -> str:
    lines = [
        "Available commands:",
        "",
        "  search <query>   search cBioPortal studies",
        "  pull             interactive data export wizard",
        "  config           view or change settings",
        "  help             show this help",
        "  exit / quit      quit cbio",
        "",
        "Slash commands:",
    ]
    for name, (desc, _) in sorted(COMMANDS.items()):
        lines.append(f"  {name:<16} {desc}")
    return "\n".join(lines)


@register("/search", "Search cBioPortal studies")
def _search_stub(state, args) -> str:
    # Handled specially in _submit before dispatch; entry exists for tab completion
    return ""


@register("/clear", "Clear history and reset session")
def _clear(state, args) -> str:
    state.history.clear()
    state.show_welcome = True
    return ""


@register("/quit", "Exit the application")
def _quit(state, args) -> str:
    if state.app:
        state.app.exit()
    return "Goodbye!"


class SlashCommandCompleter(Completer):
    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if text.startswith("/"):
            word = text.lstrip()
            for name in COMMANDS:
                if name.startswith(word):
                    yield Completion(name[len(word):], display=name)
