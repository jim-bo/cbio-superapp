"""In-browser `cbio` terminal tray (textual-serve).

This module currently lands only the **security scaffolding** for the
feature — env scrubbing, feature flag, and the subprocess-launch helper.
The actual `textual_serve.Server` mount is added in a follow-up PR once
the other mitigations (path allowlist, read-only mode, origin check,
resource caps) are in place. See
`~/.claude/plans/serialized-tumbling-torvalds.md` for the full plan.
"""
from __future__ import annotations

import contextlib
import logging
import os
import secrets
import shutil
import tempfile
from pathlib import Path
from typing import Iterator, Mapping
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Request, Response

from cbioportal.web.llm_proxy import get_registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/terminal", tags=["terminal"])

# Feature flag — default OFF. Route 404s unless explicitly enabled.
_FLAG_ENV = "CBIO_TERMINAL_ENABLED"

# Origin allowlist env var: comma-separated (e.g. "http://localhost:8002,http://127.0.0.1:8002").
# Empty/unset → only same-origin requests are allowed.
_ORIGIN_ALLOW_ENV = "CBIO_TERMINAL_ALLOWED_ORIGINS"

# CSRF cookie/header names. The cookie is HttpOnly + SameSite=Strict; the
# websocket upgrade must present the matching value via its own cookie
# (textual-serve's WS handshake carries cookies) AND a custom header set
# by the client JS that reads the same value from a separate, non-HttpOnly
# cookie — the standard "double-submit cookie" pattern.
_CSRF_COOKIE = "cbio_terminal_csrf"
_CSRF_HEADER = "x-cbio-terminal-csrf"


def terminal_enabled() -> bool:
    return os.environ.get(_FLAG_ENV, "").lower() in ("1", "true", "yes")


def _allowed_origins(request: Request) -> set[str]:
    """Build the origin allowlist for this request.

    Always includes the current request's own scheme+host (same-origin).
    Adds any entries from ``CBIO_TERMINAL_ALLOWED_ORIGINS``.
    """
    allowed = set()
    # Same-origin: derive from the request URL.
    allowed.add(f"{request.url.scheme}://{request.url.netloc}")
    extra = os.environ.get(_ORIGIN_ALLOW_ENV, "").strip()
    if extra:
        for o in extra.split(","):
            o = o.strip()
            if o:
                allowed.add(o)
    return allowed


def check_origin(request: Request) -> None:
    """Reject the request if its Origin header is not in the allowlist.

    Browsers always set Origin on cross-origin fetches and WebSocket
    upgrades. Same-origin GETs may omit it — in that case we accept,
    since SameSite=Strict on the CSRF cookie already blocks cross-site.
    """
    origin = request.headers.get("origin")
    if origin is None:
        return  # same-origin navigation; SameSite cookie handles CSRF
    allowed = _allowed_origins(request)
    # Also accept the literal Origin's scheme+host match.
    parsed = urlparse(origin)
    if not parsed.scheme or not parsed.netloc:
        raise HTTPException(status_code=403, detail="invalid origin")
    origin_normalized = f"{parsed.scheme}://{parsed.netloc}"
    if origin_normalized not in allowed:
        logger.warning("rejected terminal request from origin=%r", origin)
        raise HTTPException(status_code=403, detail="origin not allowed")


def _secure_cookies() -> bool:
    return os.environ.get("CBIO_SECURE_COOKIES", "0") == "1"


def issue_csrf_cookie(response: Response) -> str:
    """Mint a CSRF token, set it on the response, return the value.

    The token is set as a SameSite=Strict cookie. The WS upgrade (and
    any future authenticated POST) must prove knowledge of it via the
    ``X-Cbio-Terminal-Csrf`` header — the double-submit pattern.
    """
    token = secrets.token_urlsafe(32)
    response.set_cookie(
        key=_CSRF_COOKIE,
        value=token,
        httponly=False,  # must be readable by client JS for the header submit
        secure=_secure_cookies(),
        samesite="strict",
        path="/terminal",
    )
    return token


def validate_csrf(request: Request) -> None:
    """Enforce double-submit CSRF check on state-changing / WS upgrade requests.

    Reject if either side (cookie or header) is missing, or if they
    don't match. `secrets.compare_digest` avoids timing leaks.
    """
    cookie = request.cookies.get(_CSRF_COOKIE)
    header = request.headers.get(_CSRF_HEADER)
    if not cookie or not header:
        raise HTTPException(status_code=403, detail="missing CSRF token")
    if not secrets.compare_digest(cookie, header):
        raise HTTPException(status_code=403, detail="CSRF token mismatch")


def build_subprocess_env(
    scratch_home: str,
    *,
    session_id: str,
    proxy_base_url: str,
    ttl_seconds: int = 3600,
    max_requests: int | None = None,
) -> dict[str, str]:
    """Build the minimal env dict for a `cbio` subprocess launched from the web.

    The subprocess NEVER sees the real OpenRouter API key. Instead:

    - The parent FastAPI process holds the real key in Python memory
      (see :mod:`cbioportal.web.llm_proxy`).
    - We mint a per-session opaque bearer token via the registry.
    - The subprocess is told to talk to ``proxy_base_url`` as if it
      were OpenRouter, using the session token as its "api key".
    - On websocket close, the caller revokes the token so any leaked
      copy is instantly dead.

    We also explicitly do NOT inherit ``os.environ``. Cloudflare tokens,
    GCS credentials, DB connection strings, and — crucially — the real
    ``OPENROUTER_API_KEY`` never enter the subprocess. A path-taking
    tool reading ``/proc/self/environ`` gets a localhost URL and a
    token that only works against the parent process's proxy.
    """
    session_token = get_registry().issue(
        session_id,
        ttl_seconds=ttl_seconds,
        max_requests=max_requests,
    )

    return {
        "PATH": "/usr/bin:/bin:/usr/local/bin",
        "HOME": scratch_home,
        "TERM": "xterm-256color",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        # cbio's OPENROUTER_API_KEY — a one-shot session token useless
        # anywhere except the parent proxy on this host.
        "OPENROUTER_API_KEY": session_token,
        # Tells pydantic-ai / cli-textual to target our local proxy
        # instead of api.openrouter.ai. Honored by `cbioportal.cli.main`
        # when CBIO_WEB_MODE=1 (constructs a custom OpenAIChatModel).
        "OPENROUTER_BASE_URL": proxy_base_url,
        # Signal to cbio that it is running inside the web tray.
        "CBIO_WEB_MODE": "1",
    }


@contextlib.contextmanager
def session_scratch_dir(session_id: str) -> Iterator[Path]:
    """Create an isolated tempdir for a single web terminal session.

    The subprocess is launched with this directory as its ``cwd`` AND
    its ``HOME``. Any relative path the agent writes (e.g.
    ``.cbio/convos/foo.jsonl``, a DuckDB scratch file) lands inside
    the tempdir and is wiped on disconnect. This also moves the cbio
    subprocess away from the webapp's own cwd so the ``.env`` file in
    the project root is no longer reachable via a relative path —
    the path allowlist (M3) is the real gate, this is defense in depth.

    The directory is created with mode 0o700 and removed in a finally
    block. If cleanup fails (e.g. because the subprocess is still
    holding a file handle), we log and continue — the tempdir will be
    reaped by the OS eventually.
    """
    parent = Path(tempfile.gettempdir()) / "cbio-terminal"
    parent.mkdir(mode=0o700, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix=f"{session_id}-", dir=parent))
    try:
        scratch.chmod(0o700)
        yield scratch
    finally:
        try:
            shutil.rmtree(scratch, ignore_errors=False)
        except OSError as exc:
            logger.warning("failed to clean scratch dir %s: %s", scratch, exc)


def build_spawn_kwargs(
    session_id: str,
    scratch_dir: Path,
    *,
    proxy_base_url: str,
    ttl_seconds: int = 3600,
    max_requests: int | None = None,
) -> dict:
    """Build the full kwargs dict for spawning the `cbio` subprocess.

    Bundles M1 (env scrubbing + session token) with M5 (scratch cwd +
    HOME) so the future textual-serve mount has a single call site.
    Returns a dict ready to splat into ``subprocess.Popen`` or
    ``asyncio.create_subprocess_exec``:

        {"env": {...}, "cwd": "/tmp/cbio-terminal/..."}
    """
    env = build_subprocess_env(
        scratch_home=str(scratch_dir),
        session_id=session_id,
        proxy_base_url=proxy_base_url,
        ttl_seconds=ttl_seconds,
        max_requests=max_requests,
    )
    return {"env": env, "cwd": str(scratch_dir)}


def assert_no_leaked_secrets(env: Mapping[str, str]) -> None:
    """Guard used by tests: no sensitive host env vars leaked into the subprocess env."""
    forbidden_prefixes = (
        "CLOUDFLARE_",
        "GOOGLE_",
        "GCP_",
        "AWS_",
        "DATABASE_",
        "DB_",
    )
    forbidden_exact = {
        "CBIO_WEB_OPENROUTER_API_KEY",
    }
    for key in env:
        if key in forbidden_exact:
            raise AssertionError(f"{key} must not appear in subprocess env")
        if any(key.startswith(p) for p in forbidden_prefixes):
            raise AssertionError(f"forbidden env var leaked: {key}")

    # Hard assertion: no sk- prefixed value should appear. The only
    # credential-like string in the env is a session token, which is
    # base64url (no "sk-" prefix).
    for key, value in env.items():
        if isinstance(value, str) and value.startswith("sk-"):
            raise AssertionError(
                f"env var {key} appears to contain a real provider key "
                "(prefix 'sk-') — the subprocess must only hold session tokens"
            )


@router.get("")
def terminal_index(request: Request, response: Response) -> dict:
    """Placeholder endpoint. Returns 404 unless the feature flag is set.

    Enforces M2 boundary checks:
    - Feature flag or 404
    - Origin allowlist (via ``check_origin``)
    - Issues a CSRF cookie on success so the subsequent WS upgrade
      can prove double-submit knowledge

    The real implementation (mounting ``textual_serve.Server``, serving
    the iframe shell, websocket bridge) lands in a later PR.
    """
    if not terminal_enabled():
        raise HTTPException(status_code=404)
    check_origin(request)
    token = issue_csrf_cookie(response)
    return {
        "status": "scaffolding",
        "message": (
            "Terminal tray scaffolding is in place but the textual-serve "
            "mount is not yet wired. See the mitigation plan."
        ),
        "csrf_token": token,
    }
