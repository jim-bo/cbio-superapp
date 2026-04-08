"""Per-session resource caps for the web terminal tray (M6).

Best-effort in-memory limits that sit in front of ``textual-serve``'s
subprocess spawn. Real isolation comes from the deployment layer
(cgroups, per-user process limits, egress policy); this module just
makes trivial tab-spam and runaway sessions fail fast inside the app.

Three limits:

- ``max_per_ip`` — reject new sessions from a client that already has
  N live ones (default 2). HTTP 429.
- ``max_total`` — global cap on concurrent sessions (default 10).
  HTTP 503 past the limit.
- ``idle_timeout_seconds`` — sessions with no traffic for this long
  are considered dead; ``reap_idle`` returns their ids so the caller
  can kill the subprocesses.

Configured via env vars read at construction time:

    CBIO_TERMINAL_MAX_PER_IP   default 2
    CBIO_TERMINAL_MAX_TOTAL    default 10
    CBIO_TERMINAL_IDLE_SECONDS default 900  (15 minutes)
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Iterator

from fastapi import HTTPException


@dataclass
class _Session:
    session_id: str
    client_ip: str
    created_at: float
    last_activity: float


class SessionLimiter:
    """Thread-safe in-memory session counter with per-IP and global caps.

    Safe for use under FastAPI's anyio thread pool — all mutations go
    through a single :class:`Lock`. For multi-worker deployments this
    would need to move to Redis; out of scope for the app-layer plan.
    """

    def __init__(
        self,
        *,
        max_per_ip: int | None = None,
        max_total: int | None = None,
        idle_timeout_seconds: int | None = None,
    ):
        self._max_per_ip = (
            max_per_ip
            if max_per_ip is not None
            else int(os.environ.get("CBIO_TERMINAL_MAX_PER_IP", "2"))
        )
        self._max_total = (
            max_total
            if max_total is not None
            else int(os.environ.get("CBIO_TERMINAL_MAX_TOTAL", "10"))
        )
        self._idle = (
            idle_timeout_seconds
            if idle_timeout_seconds is not None
            else int(os.environ.get("CBIO_TERMINAL_IDLE_SECONDS", "900"))
        )
        self._sessions: dict[str, _Session] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Acquire / release
    # ------------------------------------------------------------------

    def acquire(self, session_id: str, client_ip: str) -> None:
        """Register a new session or raise HTTPException on cap breach.

        ``session_id`` must be globally unique (the caller mints it,
        e.g. via ``secrets.token_urlsafe``).
        """
        now = time.time()
        with self._lock:
            if session_id in self._sessions:
                raise HTTPException(
                    status_code=409, detail="session id already registered"
                )
            if len(self._sessions) >= self._max_total:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"terminal at capacity ({self._max_total} concurrent "
                        "sessions); try again shortly"
                    ),
                )
            per_ip = sum(1 for s in self._sessions.values() if s.client_ip == client_ip)
            if per_ip >= self._max_per_ip:
                raise HTTPException(
                    status_code=429,
                    detail=(
                        f"too many terminal sessions from {client_ip} "
                        f"({per_ip}/{self._max_per_ip})"
                    ),
                )
            self._sessions[session_id] = _Session(
                session_id=session_id,
                client_ip=client_ip,
                created_at=now,
                last_activity=now,
            )

    def release(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)

    def touch(self, session_id: str) -> None:
        """Mark the session as active (call on each websocket message)."""
        with self._lock:
            s = self._sessions.get(session_id)
            if s is not None:
                s.last_activity = time.time()

    # ------------------------------------------------------------------
    # Inspection + reaping
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        with self._lock:
            return len(self._sessions)

    def count_for_ip(self, client_ip: str) -> int:
        with self._lock:
            return sum(1 for s in self._sessions.values() if s.client_ip == client_ip)

    def reap_idle(self) -> list[str]:
        """Return (and remove) session ids whose last activity is older than the idle cap.

        The caller is responsible for actually killing the subprocess
        bound to each returned id.
        """
        now = time.time()
        cutoff = now - self._idle
        reaped: list[str] = []
        with self._lock:
            for sid, sess in list(self._sessions.items()):
                if sess.last_activity < cutoff:
                    reaped.append(sid)
                    del self._sessions[sid]
        return reaped


# Module-level singleton used by the terminal route.
_limiter: SessionLimiter | None = None


def get_limiter() -> SessionLimiter:
    global _limiter
    if _limiter is None:
        _limiter = SessionLimiter()
    return _limiter


def reset_limiter_for_tests() -> None:
    """Test helper — recreate the singleton with fresh env-var values."""
    global _limiter
    _limiter = None
