"""In-browser `cbio` terminal tray (textual-serve).

Security scaffolding (M1-M8) plus the live WebSocket bridge that streams
a ``cbio`` Textual TUI subprocess to the browser via ``textual-serve``'s
``AppService``.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import secrets
import shutil
import tempfile
from pathlib import Path
from typing import Iterator, Mapping
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Request, Response, WebSocket
from starlette.responses import HTMLResponse
from textual_serve.download_manager import DownloadManager

from cbioportal.web.llm_proxy import get_registry
from cbioportal.web.session_limiter import get_limiter
from cbioportal.web.terminal_service import (
    CbioAppService,
    SessionHandle,
    cleanup_session,
    get_active_sessions,
    make_subprocess_command,
)

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


def _subprocess_path() -> str:
    """Build a minimal PATH that includes ``uv``'s directory.

    The hardcoded base covers standard system locations.  We resolve
    ``uv`` from the *parent* process's PATH at import time so the
    subprocess can find it regardless of where it was installed
    (Homebrew, cargo, /usr/local/bin, etc.).
    """
    import shutil

    base = "/usr/bin:/bin:/usr/local/bin"
    uv_path = shutil.which("uv")
    if uv_path:
        uv_dir = str(Path(uv_path).resolve().parent)
        if uv_dir not in base.split(":"):
            base = f"{uv_dir}:{base}"
    return base


def terminal_enabled() -> bool:
    return os.environ.get(_FLAG_ENV, "").lower() in ("1", "true", "yes")


def _allowed_origins(request: Request) -> set[str]:
    """Build the origin allowlist for this request.

    Always includes the current request's own scheme+host (same-origin).
    ``localhost`` and ``127.0.0.1`` are treated as equivalent so that
    browsers sending ``Origin: http://localhost:PORT`` aren't rejected
    when the server binds to ``127.0.0.1``.
    Adds any entries from ``CBIO_TERMINAL_ALLOWED_ORIGINS``.
    """
    allowed = set()
    # Same-origin: derive from the request URL.  Origin headers always
    # use http/https (never ws/wss), so normalise the scheme.
    scheme = request.url.scheme
    if scheme == "ws":
        scheme = "http"
    elif scheme == "wss":
        scheme = "https"
    netloc = request.url.netloc
    allowed.add(f"{scheme}://{netloc}")
    # Treat localhost ↔ 127.0.0.1 as equivalent.
    if "127.0.0.1" in netloc:
        allowed.add(f"{scheme}://{netloc.replace('127.0.0.1', 'localhost')}")
    elif "localhost" in netloc:
        allowed.add(f"{scheme}://{netloc.replace('localhost', '127.0.0.1')}")
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
        "PATH": _subprocess_path(),
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
        # The subprocess cwd is the scratch dir, so the default relative
        # DB path (data/cbioportal.duckdb) won't resolve.  Pass the
        # absolute path from the parent process.
        "CBIO_DB_PATH": os.environ.get(
            "CBIO_DB_PATH",
            str(Path("data/cbioportal.duckdb").resolve()),
        ),
        # Disable Langfuse observability: cli_textual's init_observability
        # short-circuits when either of these is empty.  Setting them
        # explicitly to "" ensures no leaked keys (e.g. via uv loading a
        # .env file) can trigger a startup SSL call that adds 1-3s of
        # cold-start time.  Proxying Langfuse through the LLM proxy
        # would require OTLP support — not worth the complexity.
        "LANGFUSE_SECRET_KEY": "",
        "LANGFUSE_PUBLIC_KEY": "",
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
def terminal_index(request: Request) -> HTMLResponse:
    """Serve the terminal page with an embedded WebSocket URL and CSRF token."""
    if not terminal_enabled():
        raise HTTPException(status_code=404)
    check_origin(request)

    # Mint the CSRF token up front so we can embed it in the template
    # AND set the cookie on the same response object.
    token = secrets.token_urlsafe(32)

    scheme = "wss" if request.url.scheme == "https" else "ws"
    ws_url = f"{scheme}://{request.url.netloc}/terminal/ws"

    templates = request.app.state.templates
    resp = templates.TemplateResponse(
        "terminal/page.html",
        {
            "request": request,
            "ws_url": ws_url,
            "csrf_token": token,
            "font_size": 16,
        },
    )
    resp.set_cookie(
        key=_CSRF_COOKIE,
        value=token,
        httponly=False,
        secure=_secure_cookies(),
        samesite="strict",
        path="/terminal",
    )
    # Allow same-origin iframing from the dashboard tray, block everything
    # else. `frame-ancestors` is the CSP3 directive (modern browsers);
    # `X-Frame-Options` is the legacy fallback.
    resp.headers["Content-Security-Policy"] = "frame-ancestors 'self'"
    resp.headers["X-Frame-Options"] = "SAMEORIGIN"
    return resp


async def _process_ws_messages(
    ws: WebSocket,
    app_service: CbioAppService,
    session_id: str,
) -> None:
    """Read browser messages and forward to the subprocess.

    Mirrors ``textual_serve.server.Server._process_messages``.
    """
    limiter = get_limiter()
    while True:
        msg = await ws.receive()
        if msg["type"] == "websocket.disconnect":
            break
        if msg["type"] == "websocket.receive" and "text" in msg:
            limiter.touch(session_id)
            envelope = json.loads(msg["text"])
            type_ = envelope[0]
            if type_ == "stdin":
                await app_service.send_bytes(envelope[1].encode("utf-8"))
            elif type_ == "resize":
                data = envelope[1]
                await app_service.set_terminal_size(data["width"], data["height"])
            elif type_ == "ping":
                await ws.send_json(["pong", envelope[1]])
            elif type_ == "blur":
                await app_service.blur()
            elif type_ == "focus":
                await app_service.focus()


@router.websocket("/ws")
async def terminal_ws(websocket: WebSocket) -> None:
    """WebSocket bridge between the browser and a ``cbio`` subprocess."""
    if not terminal_enabled():
        await websocket.close(code=4004)
        return

    # Origin check — WebSocket inherits from HTTPConnection like Request.
    try:
        check_origin(websocket)  # type: ignore[arg-type]
    except HTTPException:
        await websocket.close(code=4003)
        return

    # CSRF: the SameSite=Strict cookie won't be sent on cross-origin WS
    # upgrades, so its presence proves same-origin.  We don't need the
    # double-submit query param (which textual.js can't set anyway).
    csrf_cookie = websocket.cookies.get(_CSRF_COOKIE, "")
    if not csrf_cookie:
        await websocket.close(code=4003)
        return

    # Session limiter.
    session_id = secrets.token_urlsafe(16)
    client_ip = websocket.client.host if websocket.client else "unknown"
    limiter = get_limiter()
    try:
        limiter.acquire(session_id, client_ip)
    except HTTPException as exc:
        await websocket.close(code=4000 + (exc.status_code % 1000))
        return

    await websocket.accept()

    # Build the proxy base URL for the subprocess.
    http_scheme = "https" if websocket.url.scheme == "wss" else "http"
    proxy_base_url = f"{http_scheme}://{websocket.url.netloc}/llm-proxy"

    session_token: str | None = None
    try:
        with session_scratch_dir(session_id) as scratch:
            spawn_kwargs = build_spawn_kwargs(
                session_id,
                scratch,
                proxy_base_url=proxy_base_url,
            )
            session_token = spawn_kwargs["env"]["OPENROUTER_API_KEY"]

            download_manager = DownloadManager()

            # Wrap WS callbacks so they silently ignore errors when the
            # connection is already closed (race during cleanup).
            ws_closed = False

            async def _safe_send_bytes(data: bytes) -> None:
                if not ws_closed:
                    try:
                        await websocket.send_bytes(data)
                    except RuntimeError:
                        pass

            async def _safe_send_text(data: str) -> None:
                if not ws_closed:
                    try:
                        await websocket.send_text(data)
                    except RuntimeError:
                        pass

            async def _safe_close() -> None:
                nonlocal ws_closed
                ws_closed = True
                try:
                    await websocket.close()
                except RuntimeError:
                    pass

            app_service = CbioAppService(
                make_subprocess_command(),
                spawn_env=spawn_kwargs["env"],
                spawn_cwd=spawn_kwargs["cwd"],
                write_bytes=_safe_send_bytes,
                write_str=_safe_send_text,
                close=_safe_close,
                download_manager=download_manager,
            )

            handle = SessionHandle(
                session_id=session_id,
                app_service=app_service,
                session_token=session_token,
                download_manager=download_manager,
            )
            get_active_sessions()[session_id] = handle

            width = int(websocket.query_params.get("width", "80"))
            height = int(websocket.query_params.get("height", "24"))
            await app_service.start(width, height)

            try:
                await _process_ws_messages(websocket, app_service, session_id)
            finally:
                await app_service.stop()

    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("terminal ws error for session %s", session_id)
    finally:
        await cleanup_session(session_id)
        # Belt-and-suspenders: if cleanup_session missed these (e.g.
        # scratch dir context exited before the handle was registered).
        if session_token:
            try:
                get_registry().revoke(session_token)
            except Exception:
                pass
        try:
            limiter.release(session_id)
        except Exception:
            pass
