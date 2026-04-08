"""Shared DuckDB connection helper for cbio CLI tools.

Tools are called from an asyncio event loop, but DuckDB's Python binding is
synchronous. Each tool opens a short-lived read-only connection via this helper,
uses it, and closes it. This avoids sharing connections across the cli-textual
tool-runner thread pool (see core/CLAUDE.md: DuckDB is not thread-safe).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

from cbioportal.core import database


@contextmanager
def open_conn(read_only: bool = True):
    """Open a short-lived DuckDB connection for a single tool invocation.

    When ``CBIO_WEB_MODE=1`` (browser terminal tray), ``read_only`` is
    forced to ``True`` regardless of the caller. Belt-and-braces with
    ``get_tools_for_env`` dropping mutating tools entirely — even if a
    writable tool slips through, it cannot open a writable connection.
    """
    if os.environ.get("CBIO_WEB_MODE") == "1":
        read_only = True
    conn = database.get_connection(read_only=read_only)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def db_path() -> Path:
    return database.get_db_path()
