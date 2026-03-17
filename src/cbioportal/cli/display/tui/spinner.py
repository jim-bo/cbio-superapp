"""Animated braille spinner for the cbio TUI."""
from __future__ import annotations

import asyncio

from prompt_toolkit.layout.controls import UIControl, UIContent
from prompt_toolkit.formatted_text import StyleAndTextTuples

SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
SPINNER_INTERVAL = 0.08


class SpinnerControl(UIControl):
    def __init__(self) -> None:
        self._frame_index = 0
        self._active = False
        self._task: asyncio.Task | None = None
        self._label = "Working…"

    def create_content(self, width: int, height: int) -> UIContent:
        if self._active:
            frame = SPINNER_FRAMES[self._frame_index % len(SPINNER_FRAMES)]
            text: StyleAndTextTuples = [("class:spinner", f" {frame} {self._label}")]
        else:
            text = [("", "")]
        return UIContent(get_line=lambda i: text, line_count=1)

    def start(self, app, label: str = "Working…") -> None:
        self._label = label
        self._active = True
        self._task = app.create_background_task(self._animate(app))

    def set_label(self, label: str) -> None:
        self._label = label

    def stop(self) -> None:
        self._active = False
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _animate(self, app) -> None:
        try:
            while self._active:
                self._frame_index += 1
                app.invalidate()
                await asyncio.sleep(SPINNER_INTERVAL)
        except asyncio.CancelledError:
            pass
