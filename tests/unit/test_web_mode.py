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
