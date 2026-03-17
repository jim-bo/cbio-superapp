"""Full-screen prompt_toolkit TUI for the cbio REPL."""
from __future__ import annotations

import asyncio

from prompt_toolkit import Application
from prompt_toolkit.widgets import TextArea
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.completion import merge_completers
from prompt_toolkit.filters import Condition
from prompt_toolkit.input.vt100_parser import ANSI_SEQUENCES
from prompt_toolkit.keys import Keys

# Kitty keyboard protocol: Shift+Enter → newline insert (for multiline input)
ANSI_SEQUENCES['\x1b[13;2u'] = Keys.ControlJ

from cbioportal.cli.display.tui.history import HistoryStore, HistoryEntry, MessageKind
from cbioportal.cli.display.tui.styles import STYLE
from cbioportal.cli.display.tui.layout import build_layout
from cbioportal.cli.display.tui.commands import SlashCommandCompleter, dispatch
from cbioportal.cli.display.tui.flow import handle_search, handle_config, handle_sync


class AppState:
    def __init__(self) -> None:
        self.history = HistoryStore()
        self.show_welcome = True
        self.is_thinking = False
        self.notification = ""
        self.input_field = None       # TextArea
        self.spinner_control = None   # SpinnerControl (set by build_layout)
        self.app = None               # Application
        self.exit_requested = False

        # Inline selector state
        self.selector_options: list[str] | None = None
        self.selector_index: int = 0
        self.selector_header: list[tuple[str, str]] | None = None
        self.selector_callback = None   # async (idx: int, text: str) -> None

        # Intercept next Enter press (e.g. output path step)
        self.on_submit_override = None  # async (text: str) -> None

        # Wizard flow data
        self.flow_studies: list = []
        self.flow_study = None
        self.flow_data_type = None
        self.flow_format = None


def build_app() -> Application:
    state = AppState()

    state.input_field = TextArea(
        multiline=True,
        completer=merge_completers([SlashCommandCompleter()]),
        complete_while_typing=True,
        style="class:input",
    )

    kb = KeyBindings()
    selector_active = Condition(lambda: state.selector_options is not None)

    @kb.add("enter", filter=~selector_active)
    async def handle_enter(event):
        state.exit_requested = False
        text = state.input_field.text.strip()
        if not text:
            return
        state.input_field.text = ""
        await _submit(text, state, event.app)

    @kb.add("enter", filter=selector_active, eager=True)
    def selector_confirm(event):
        state.exit_requested = False
        idx = state.selector_index
        selected = state.selector_options[idx]
        state.selector_options = None
        state.selector_index = 0
        state.selector_header = None
        if state.selector_callback:
            cb = state.selector_callback
            state.selector_callback = None
            event.app.create_background_task(cb(idx, selected))
        event.app.invalidate()

    @kb.add("up", filter=selector_active, eager=True)
    @kb.add("s-tab", filter=selector_active, eager=True)
    def selector_up(event):
        state.exit_requested = False
        if state.selector_options:
            state.selector_index = (state.selector_index - 1) % len(state.selector_options)
            event.app.invalidate()

    @kb.add("down", filter=selector_active, eager=True)
    @kb.add("tab", filter=selector_active, eager=True)
    def selector_down(event):
        state.exit_requested = False
        if state.selector_options:
            state.selector_index = (state.selector_index + 1) % len(state.selector_options)
            event.app.invalidate()

    @kb.add("escape", filter=selector_active, eager=True)
    def selector_cancel(event):
        state.exit_requested = False
        state.selector_options = None
        state.selector_index = 0
        state.selector_header = None
        state.selector_callback = None
        event.app.invalidate()

    @kb.add("c-c")
    def exit_app_cc(event):
        event.app.exit()

    @kb.add("c-d")
    def exit_app_cd(event):
        if state.exit_requested:
            event.app.exit()
        else:
            state.exit_requested = True
            event.app.invalidate()

    layout = build_layout(state)

    app = Application(
        layout=layout,
        key_bindings=kb,
        style=STYLE,
        full_screen=True,
        mouse_support=False,
    )

    state.app = app
    state.history.on_change(lambda: app.invalidate())

    return app


async def _submit(text: str, state: AppState, app: Application) -> None:
    state.show_welcome = False

    # Submit override: intercepts Enter for e.g. output path or config value
    if state.on_submit_override is not None:
        handler = state.on_submit_override
        state.on_submit_override = None
        await handler(text)
        return

    # Slash commands
    if text.startswith("/"):
        parts = text.strip().split()
        slash_cmd = parts[0].lower()
        slash_args = parts[1:]

        state.history.add(HistoryEntry(MessageKind.COMMAND, text))

        if slash_cmd == "/search":
            if slash_args:
                app.create_background_task(handle_search(slash_args, state, app))
            else:
                _show_search_subprompt(state, app)
            app.invalidate()
            return

        if slash_cmd == "/sync":
            app.create_background_task(handle_sync(state, app))
            app.invalidate()
            return

        response = dispatch(text, state)
        if response:
            state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, response))
        app.invalidate()
        return

    # Word-based commands
    parts = text.strip().split()
    cmd = parts[0].lower()
    args = parts[1:]

    match cmd:
        case "search":
            if not args:
                app.create_background_task(_search_word_cmd(state, app))
            else:
                app.create_background_task(handle_search(args, state, app))
        case "pull":
            state.history.add(HistoryEntry(MessageKind.USER, text))
            state.history.add(HistoryEntry(MessageKind.NOTIFICATION, "Use 'search' first to find a study, then select it to start the pull wizard."))
            app.invalidate()
        case "config":
            app.create_background_task(handle_config(state, app))
        case "help" | "?":
            state.history.add(HistoryEntry(MessageKind.USER, text))
            from cbioportal.cli.display.tui.commands import dispatch as slash_dispatch
            resp = slash_dispatch("/help", state)
            if resp:
                state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, resp))
            app.invalidate()
        case "exit" | "quit" | "q":
            app.exit()
        case _:
            state.history.add(HistoryEntry(MessageKind.USER, text))
            state.history.add(HistoryEntry(MessageKind.NOTIFICATION, f"Unknown command: {cmd}  (try 'help')"))
            app.invalidate()


def _show_search_subprompt(state: AppState, app: Application) -> None:
    """Show an inline sub-prompt asking for the search query."""
    state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, "Search query:"))

    async def on_query(q: str) -> None:
        q = q.strip()
        if q:
            state.history.add(HistoryEntry(MessageKind.USER, q))
            await handle_search(q.split(), state, app)
        else:
            state.history.add(HistoryEntry(MessageKind.NOTIFICATION, "Search cancelled."))
            app.invalidate()

    state.on_submit_override = on_query


async def _search_word_cmd(state: AppState, app: Application) -> None:
    """Handle bare 'search' word command — same sub-prompt as /search with no args."""
    state.history.add(HistoryEntry(MessageKind.USER, "search"))
    _show_search_subprompt(state, app)
    app.invalidate()


def run_repl() -> None:
    """Launch the cbio full-screen TUI."""
    app = build_app()
    asyncio.run(app.run_async())
