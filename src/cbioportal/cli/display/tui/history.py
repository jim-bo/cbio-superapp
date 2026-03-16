"""History store for the cbio TUI."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class MessageKind(Enum):
    USER = auto()
    COMMAND = auto()
    COMMAND_RESPONSE = auto()
    NOTIFICATION = auto()
    TABLE_ROW = auto()


@dataclass
class HistoryEntry:
    kind: MessageKind
    text: str = ""
    streaming: bool = False
    # For TABLE_ROW: list of (style_class, text) segments
    cells: list[tuple[str, str]] | None = None


class HistoryStore:
    def __init__(self) -> None:
        self._entries: list[HistoryEntry] = []
        self._callbacks: list[Callable] = []

    def add(self, entry: HistoryEntry) -> HistoryEntry:
        self._entries.append(entry)
        self._fire()
        return entry

    def clear(self) -> None:
        self._entries.clear()
        self._fire()

    def on_change(self, cb: Callable) -> None:
        self._callbacks.append(cb)

    def _fire(self) -> None:
        for cb in self._callbacks:
            cb()

    def to_formatted_text(self) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for entry in self._entries:
            if entry.kind == MessageKind.USER:
                result.append(("class:prompt-marker", "> "))
                result.append(("class:history-user", entry.text))
                result.append(("", "\n"))
            elif entry.kind == MessageKind.COMMAND:
                result.append(("class:prompt-marker", "> "))
                result.append(("class:history-command", entry.text))
                result.append(("", "\n"))
            elif entry.kind == MessageKind.COMMAND_RESPONSE:
                result.append(("", "  └ "))
                result.append(("class:history-response", entry.text))
                result.append(("", "\n"))
            elif entry.kind == MessageKind.NOTIFICATION:
                result.append(("", "  ✳ "))
                result.append(("class:notification", entry.text))
                result.append(("", "\n"))
            elif entry.kind == MessageKind.TABLE_ROW:
                if entry.cells:
                    for style, text in entry.cells:
                        result.append((style, text))
                result.append(("", "\n"))
        return result
