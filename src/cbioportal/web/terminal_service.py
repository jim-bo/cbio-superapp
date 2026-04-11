"""Terminal session service — subprocess lifecycle and cleanup.

Wraps ``textual_serve.AppService`` so the subprocess runs with a
scrubbed environment (no host secrets) and an isolated scratch cwd.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import Awaitable, Callable

from textual_serve.app_service import AppService
from textual_serve.download_manager import DownloadManager

from cbioportal.web.llm_proxy import get_registry
from cbioportal.web.session_limiter import get_limiter

logger = logging.getLogger(__name__)

# Resolve the project root once at import time so the subprocess command
# can pass ``--project`` to uv regardless of its own cwd.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[3])


class CbioAppService(AppService):
    """AppService subclass that uses a scrubbed env and scratch cwd."""

    def __init__(
        self,
        command: str,
        *,
        spawn_env: dict[str, str],
        spawn_cwd: str,
        write_bytes: Callable[[bytes], Awaitable[None]],
        write_str: Callable[[str], Awaitable[None]],
        close: Callable[[], Awaitable[None]],
        download_manager: DownloadManager,
        debug: bool = False,
    ) -> None:
        super().__init__(
            command,
            write_bytes=write_bytes,
            write_str=write_str,
            close=close,
            download_manager=download_manager,
            debug=debug,
        )
        self._spawn_env = spawn_env
        self._spawn_cwd = spawn_cwd

    def _build_environment(self, width: int = 80, height: int = 24) -> dict[str, str]:
        """Merge Textual driver vars into the scrubbed subprocess env.

        Unlike the parent, this does NOT copy ``os.environ``.
        """
        env = dict(self._spawn_env)
        env["TEXTUAL_DRIVER"] = "textual.drivers.web_driver:WebDriver"
        env["TEXTUAL_FPS"] = "60"
        env["TEXTUAL_COLOR_SYSTEM"] = "truecolor"
        env["TERM_PROGRAM"] = "textual"
        env["TERM_PROGRAM_VERSION"] = version("textual-serve")
        env["COLUMNS"] = str(width)
        env["ROWS"] = str(height)
        return env

    async def _open_app_process(self, width: int = 80, height: int = 24):
        """Launch the subprocess in the scratch directory."""
        environment = self._build_environment(width=width, height=height)
        self._process = await asyncio.create_subprocess_shell(
            self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
            cwd=self._spawn_cwd,
        )
        assert self._process.stdin is not None
        self._stdin = self._process.stdin
        return self._process


@dataclass
class SessionHandle:
    """Tracks a live terminal session for cleanup and idle reaping."""

    session_id: str
    app_service: CbioAppService
    session_token: str
    download_manager: DownloadManager
    _cleaned: bool = field(default=False, repr=False)


# Module-level registry so the idle reaper and WebSocket close can both
# find and clean up sessions.
_active_sessions: dict[str, SessionHandle] = {}


def get_active_sessions() -> dict[str, SessionHandle]:
    return _active_sessions


async def cleanup_session(session_id: str) -> None:
    """Idempotent cleanup: stop subprocess, revoke token, release limiter slot."""
    handle = _active_sessions.pop(session_id, None)
    if handle is None or handle._cleaned:
        return
    handle._cleaned = True

    # Stop the subprocess (safe to call multiple times).
    try:
        await handle.app_service.stop()
    except Exception:
        logger.exception("error stopping app service for session %s", session_id)

    # Revoke the LLM proxy token.
    try:
        get_registry().revoke(handle.session_token)
    except Exception:
        logger.debug("token revocation failed for session %s (already revoked?)", session_id)

    # Release the session limiter slot.
    try:
        get_limiter().release(session_id)
    except Exception:
        logger.debug("limiter release failed for session %s", session_id)


async def idle_reaper() -> None:
    """Background task: reap idle sessions every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        try:
            reaped = get_limiter().reap_idle()
            for sid in reaped:
                logger.info("reaping idle terminal session %s", sid)
                await cleanup_session(sid)
        except Exception:
            logger.exception("idle reaper error")


def make_subprocess_command() -> str:
    """Build the shell command to launch ``cbio`` in the subprocess.

    ``--no-env-file`` prevents uv from loading the project's ``.env``
    which may contain Langfuse keys, extra API tokens, etc. that would
    bypass the scrubbed environment built by ``build_subprocess_env``.
    """
    return f"uv run --no-env-file --project {_PROJECT_ROOT} cbio"
