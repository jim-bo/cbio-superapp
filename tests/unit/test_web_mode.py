"""Tests for M4: read-only tool set + clamped DuckDB connection under CBIO_WEB_MODE."""
from unittest.mock import patch

import pytest

from cbioportal.cli.tools import (
    CBIO_READ_ONLY_TOOLS,
    CBIO_TOOLS,
    get_tools_for_env,
    load_study_into_db,
    validate_study_folder,
)


# ---------------------------------------------------------------------------
# Tool-set filtering
# ---------------------------------------------------------------------------


def test_full_tool_set_includes_mutating_tools(monkeypatch):
    monkeypatch.delenv("CBIO_WEB_MODE", raising=False)
    tools = get_tools_for_env()
    assert load_study_into_db in tools
    assert validate_study_folder in tools
    assert len(tools) == len(CBIO_TOOLS)


def test_web_mode_drops_load_study_into_db(monkeypatch):
    monkeypatch.setenv("CBIO_WEB_MODE", "1")
    tools = get_tools_for_env()
    assert load_study_into_db not in tools
    assert validate_study_folder in tools  # read-only, still available
    assert tools == list(CBIO_READ_ONLY_TOOLS)


def test_read_only_set_has_no_mutating_tools():
    # Belt-and-braces: make sure load_study_into_db never slipped into
    # the read-only set by accident.
    assert load_study_into_db not in CBIO_READ_ONLY_TOOLS


# ---------------------------------------------------------------------------
# open_conn clamping
# ---------------------------------------------------------------------------


def test_open_conn_honors_read_only_when_not_web_mode(monkeypatch):
    monkeypatch.delenv("CBIO_WEB_MODE", raising=False)
    from cbioportal.cli.tools import _db

    with patch.object(_db.database, "get_connection") as mock_conn:
        mock_conn.return_value.close = lambda: None
        with _db.open_conn(read_only=False):
            pass
        mock_conn.assert_called_once_with(read_only=False)


def test_open_conn_forces_read_only_under_web_mode(monkeypatch):
    monkeypatch.setenv("CBIO_WEB_MODE", "1")
    from cbioportal.cli.tools import _db

    with patch.object(_db.database, "get_connection") as mock_conn:
        mock_conn.return_value.close = lambda: None
        # Caller requested writable; web mode must clamp to read-only.
        with _db.open_conn(read_only=False):
            pass
        mock_conn.assert_called_once_with(read_only=True)


def test_open_conn_read_only_default_preserved(monkeypatch):
    monkeypatch.delenv("CBIO_WEB_MODE", raising=False)
    from cbioportal.cli.tools import _db

    with patch.object(_db.database, "get_connection") as mock_conn:
        mock_conn.return_value.close = lambda: None
        with _db.open_conn():
            pass
        mock_conn.assert_called_once_with(read_only=True)


# ---------------------------------------------------------------------------
# CbioApp command-manager pruning in web mode
# ---------------------------------------------------------------------------


def test_cbio_app_web_mode_prunes_command_manager(monkeypatch):
    """CBIO_WEB_MODE=1 should restrict commands to CBIO_WEB_ALLOWED_COMMANDS."""
    monkeypatch.setenv("CBIO_WEB_MODE", "1")

    from cbioportal.cli.tui_app import CbioApp, CBIO_WEB_ALLOWED_COMMANDS

    app = CbioApp(
        tools=[],
        command_packages=["cbioportal.cli.slash_commands"],
        safe_mode=True,
    )

    registered = set(app.command_manager.commands.keys())

    # Must be a subset of the allowlist
    assert registered <= CBIO_WEB_ALLOWED_COMMANDS, (
        f"Extra commands found: {registered - CBIO_WEB_ALLOWED_COMMANDS}"
    )

    # Required commands must be present
    assert "/help" in registered
    assert "/studies" in registered

    # Dangerous commands must be absent
    assert "/study-load" not in registered
    assert "/cbio-config" not in registered
    assert "/mode" not in registered


def test_cbio_app_local_mode_keeps_all_commands(monkeypatch):
    """Without CBIO_WEB_MODE, all registered commands are kept."""
    monkeypatch.delenv("CBIO_WEB_MODE", raising=False)

    from cbioportal.cli.tui_app import CbioApp

    app = CbioApp(
        tools=[],
        command_packages=["cbioportal.cli.slash_commands"],
        safe_mode=True,
    )

    registered = set(app.command_manager.commands.keys())

    # All expected local-mode commands must be present
    for cmd in ("/studies", "/study-load", "/cbio-config", "/help"):
        assert cmd in registered, f"Expected {cmd!r} in local mode commands"

    # /mode is a built-in from cli_textual — only assert if it was auto-registered
    # (not all builds include it; skip if absent rather than fail)
    # assert "/mode" in registered  # optional — uncomment if cli_textual includes it
