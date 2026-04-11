"""Cbio-branded Textual app.

Subclasses ``cli_textual.app.ChatApp`` to:
  * replace the generic landing page with cBioPortal branding
  * rotate biology-inspired verbs in the agent "thinking" spinner
  * gate the slash-command surface down to a safe read-only set in web mode

All three customizations use the upstream subclass hooks added in
cli-textual-demo#12 (``LANDING_WIDGET_CLS`` class attr and ``command_filter``
constructor arg). No monkey-patching.
"""
from __future__ import annotations

import os
import random
from typing import AsyncGenerator

from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.widgets import Label, Static

from cli_textual.app import ChatApp
from cli_textual.core.chat_events import AgentThinking, ChatEvent


# ---------------------------------------------------------------------------
# Spinner verbs
# ---------------------------------------------------------------------------

BIOLOGY_VERBS: list[str] = [
    "Transcribing…",
    "Splicing…",
    "Phosphorylating…",
    "Aligning reads…",
    "Translating codons…",
    "Folding proteins…",
    "Annotating variants…",
    "Sequencing…",
    "Scanning loci…",
    "Normalizing symbols…",
]

# The generic upstream messages we want to substitute. Tool-specific
# messages ("Calling list_studies…") pass through unchanged so the user
# still sees what the agent is actually doing.
_GENERIC_THINKING: frozenset[str] = frozenset(
    {
        "Thinking...",
        "Thinking…",
        "Processing...",
        "Processing…",
        "Manager orchestrator initializing...",
        "Manager orchestrator initializing…",
    }
)


# ---------------------------------------------------------------------------
# Web-mode slash-command allowlist
# ---------------------------------------------------------------------------

CBIO_WEB_ALLOWED_COMMANDS: frozenset[str] = frozenset(
    {
        "/help",
        "/clear",
        "/studies",
        "/study-info",
        "/cancer-types",
        "/genes",
        "/data-types",
    }
)


def _web_mode_command_filter(name: str) -> bool:
    """Predicate passed to ``ChatApp(command_filter=...)`` in web mode."""
    return name in CBIO_WEB_ALLOWED_COMMANDS


# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------


class CbioLandingPage(Static):
    """Landing page shown on cbio TUI startup."""

    def compose(self) -> ComposeResult:
        with Container(id="landing-container"):
            yield Label(
                "cBioPortal — cancer genomics in your terminal",
                id="landing-title",
            )
            with Horizontal(id="landing-content"):
                with Container(id="landing-left"):
                    yield Label("Try asking", classes="landing-header")
                    yield Static(
                        "• Which studies have KRAS data?",
                        classes="landing-item",
                    )
                    yield Static(
                        "• TP53 mutation frequency in msk_chord_2024",
                        classes="landing-item",
                    )
                    yield Static(
                        "• Show me breast cancer studies with CNA",
                        classes="landing-item",
                    )
                with Container(id="landing-right"):
                    yield Label("Slash commands", classes="landing-header")
                    yield Static(
                        "[cyan]/help[/]          show all commands",
                        classes="landing-item",
                    )
                    yield Static(
                        "[cyan]/studies[/]       list studies in the DB",
                        classes="landing-item",
                    )
                    yield Static(
                        "[cyan]/study-info[/]    metadata for one study",
                        classes="landing-item",
                    )
                    yield Static(
                        "[cyan]/cancer-types[/]  organ-system counts",
                        classes="landing-item",
                    )
                    yield Static(
                        "[cyan]/genes[/]         top mutated genes",
                        classes="landing-item",
                    )
                    yield Static(
                        "[cyan]/data-types[/]    genomic data in a study",
                        classes="landing-item",
                    )


# ---------------------------------------------------------------------------
# App subclass
# ---------------------------------------------------------------------------


class CbioApp(ChatApp):
    """Cbio-branded ChatApp."""

    LANDING_WIDGET_CLS = CbioLandingPage

    def __init__(self, *args, **kwargs) -> None:
        if os.environ.get("CBIO_WEB_MODE") == "1":
            kwargs.setdefault("command_filter", _web_mode_command_filter)
        super().__init__(*args, **kwargs)

    async def stream_agent_response(
        self, generator: AsyncGenerator[ChatEvent, None]
    ) -> None:
        """Inject biology-inspired verbs into the spinner label.

        Strategy: wrap the upstream event generator. Yield one
        ``AgentThinking`` with a random biology verb as the first event
        so the base class's ``task_label.update(event.message)`` replaces
        the hard-coded "Thinking..." label immediately. For subsequent
        events, substitute any *generic* thinking messages with another
        biology verb but leave tool-specific messages untouched.
        """

        async def wrapped() -> AsyncGenerator[ChatEvent, None]:
            yield AgentThinking(message=random.choice(BIOLOGY_VERBS))
            async for event in generator:
                if (
                    isinstance(event, AgentThinking)
                    and event.message in _GENERIC_THINKING
                ):
                    event = AgentThinking(message=random.choice(BIOLOGY_VERBS))
                yield event

        return await super().stream_agent_response(wrapped())
