"""Layout definition for the cbio TUI."""
from __future__ import annotations

from prompt_toolkit.layout.containers import (
    HSplit, VSplit, Window, FloatContainer, Float,
    ConditionalContainer, WindowAlign,
)
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout import Layout
from prompt_toolkit.layout.dimension import D
from prompt_toolkit.filters import Condition
from prompt_toolkit.layout.menus import CompletionsMenu
from prompt_toolkit.layout.scrollable_pane import ScrollOffsets


def build_layout(state) -> Layout:
    from cbioportal.cli.display.tui.welcome import build_welcome_container
    from cbioportal.cli.display.tui.spinner import SpinnerControl

    state.spinner_control = SpinnerControl()

    title_bar = Window(
        content=FormattedTextControl(
            lambda: [("class:title", "── cbio · cBioPortal Data Access ──")]
        ),
        height=1,
    )

    welcome_container = ConditionalContainer(
        content=build_welcome_container(),
        filter=Condition(lambda: state.show_welcome),
    )

    notification_bar = ConditionalContainer(
        content=Window(
            content=FormattedTextControl(
                lambda: [("class:notification", f"  ✳ {state.notification}")]
            ),
            height=1,
        ),
        filter=Condition(lambda: bool(state.notification)),
    )

    history_window = Window(
        content=FormattedTextControl(
            lambda: state.history.to_formatted_text(),
            focusable=False,
        ),
        wrap_lines=True,
        scroll_offsets=ScrollOffsets(bottom=1),
    )

    spinner_container = ConditionalContainer(
        content=Window(
            content=state.spinner_control,
            height=1,
        ),
        filter=Condition(lambda: state.is_thinking),
    )

    def selector_text():
        opts = state.selector_options
        if not opts:
            return []
        lines: list[tuple[str, str]] = []
        lines.append(("class:selector-hint", "  ↑↓ or tab to move · enter to select · esc to cancel\n"))
        for i, opt in enumerate(opts):
            if i == state.selector_index:
                lines.append(("class:selector-active", f"  ❯ {opt}\n"))
            else:
                lines.append(("class:selector-option", f"    {opt}\n"))
        return lines

    selector_container = ConditionalContainer(
        content=Window(
            content=FormattedTextControl(selector_text),
            height=D(min=1),
        ),
        filter=Condition(lambda: bool(state.selector_options)),
    )

    selector_separator = ConditionalContainer(
        content=Window(height=1, char="─", style="class:separator"),
        filter=Condition(lambda: bool(state.selector_options)),
    )

    separator = Window(height=1, char="─", style="class:separator")

    prompt_marker = Window(
        content=FormattedTextControl(lambda: [("class:prompt-marker", "> ")]),
        width=2,
        height=D(max=5),
    )

    input_row = VSplit([
        prompt_marker,
        state.input_field,
    ], height=D(min=1, max=5))

    def status_left():
        return [("class:status-bar.mode", " cbio ")]

    def status_right():
        try:
            from cbioportal.core.cbio_config import get_config
            cfg = get_config()
            url = cfg.get("portal", {}).get("url", "")
        except Exception:
            url = ""
        return [("class:status-bar", f" {url} · ctrl-c to quit ")]

    status_bar = VSplit([
        Window(
            content=FormattedTextControl(status_left),
            style="class:status-bar",
        ),
        Window(
            content=FormattedTextControl(status_right),
            style="class:status-bar",
            align=WindowAlign.RIGHT,
        ),
    ], height=1, style="class:status-bar")

    body = FloatContainer(
        content=HSplit([
            title_bar,
            welcome_container,
            notification_bar,
            history_window,
            spinner_container,
            selector_container,
            selector_separator,
            separator,
            input_row,
            status_bar,
        ]),
        floats=[
            Float(
                xcursor=True,
                ycursor=True,
                content=CompletionsMenu(max_height=8, scroll_offset=1),
            )
        ],
    )

    return Layout(body, focused_element=state.input_field)
