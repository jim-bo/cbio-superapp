"""Welcome panel for the cbio TUI (shown on startup)."""
from __future__ import annotations

import os

from prompt_toolkit.formatted_text import StyleAndTextTuples
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.containers import VSplit, Window
from prompt_toolkit.widgets import Frame

# cbio logo: 4-color block grid (blue/red top, green/cyan bottom)
_LOGO_LINES = [
    [("fg:ansiblue bold", " ████"), ("fg:ansired bold", "████ ")],
    [("fg:ansiblue bold", " ████"), ("fg:ansired bold", "████ ")],
    [("fg:ansigreen bold", " ████"), ("fg:ansicyan bold", "████ ")],
    [("fg:ansigreen bold", " ████"), ("fg:ansicyan bold", "████ ")],
]


def _left_content() -> StyleAndTextTuples:
    from cbioportal.core.cbio_config import get_config
    try:
        cfg = get_config()
        portal_url = cfg.get("portal", {}).get("url", "(not set)")
    except Exception:
        portal_url = "(not set)"

    lines: StyleAndTextTuples = []
    for row in _LOGO_LINES:
        for style, text in row:
            lines.append((style, text))
        lines.append(("", "\n"))
    lines.append(("", "\n"))
    lines.append(("class:welcome-heading", "  cbio  ·  cBioPortal Data Access\n"))
    lines.append(("", "\n"))
    lines.append(("class:meta", "  Portal:  "))
    lines.append(("class:step-value", portal_url))
    lines.append(("", "\n"))
    lines.append(("class:meta", "  Dir:     "))
    lines.append(("class:step-value", os.path.basename(os.getcwd())))
    lines.append(("", "\n"))
    return lines


def _right_content() -> StyleAndTextTuples:
    lines: StyleAndTextTuples = []
    lines.append(("class:welcome-heading", " Quick commands\n"))
    lines.append(("", "\n"))
    tips: StyleAndTextTuples = [
        ("class:step-value",  "  search"), ("class:meta", " <query>   search studies\n"),
        ("class:step-value",  "  pull"),   ("class:meta", "           download data\n"),
        ("class:step-value",  "  config"), ("class:meta", "          manage settings\n"),
        ("class:step-value",  "  help"),   ("class:meta", "            list commands\n"),
        ("", "\n"),
        ("class:welcome-heading", " Tips\n"),
        ("", "\n"),
        ("class:meta", "  [tab]      autocomplete\n"),
        ("class:meta", "  [↑↓ / tab] navigate options\n"),
        ("class:meta", "  [ctrl-c]   quit\n"),
    ]
    lines.extend(tips)
    return lines


def build_welcome_container() -> Frame:
    left = Window(
        content=FormattedTextControl(lambda: _left_content()),
        width=38,
    )
    divider = Window(width=1, char="│", style="class:separator")
    right = Window(
        content=FormattedTextControl(lambda: _right_content()),
    )
    body = VSplit([left, divider, right])
    return Frame(body=body, style="class:welcome-border", height=14)
