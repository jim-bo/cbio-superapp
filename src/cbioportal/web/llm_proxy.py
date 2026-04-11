"""Local LLM reverse proxy for web-served `cbio` sessions.

The subprocess that backs the browser terminal tray MUST NOT hold the
real OpenRouter API key. Instead, the parent FastAPI process keeps the
key in Python memory and exposes this thin reverse proxy at
``/llm-proxy/*``. Each websocket session is issued a short-lived opaque
bearer token; the subprocess's ``OPENROUTER_API_KEY`` is that token, and
its ``OPENROUTER_BASE_URL`` points at this localhost proxy.

Properties this buys:

- A key leak via ``/proc/self/environ`` or a path-taking tool yields
  only a localhost URL and a one-shot session token — useless off-box.
- Tokens are revoked on websocket close; leaked copies are dead
  immediately.
- The proxy is the single chokepoint for per-session request/model/
  budget enforcement (not yet implemented; hook points marked TODO).

This module exposes:

- ``SessionTokenRegistry`` — in-memory bearer-token store with TTL.
- ``router`` — FastAPI router mounted at ``/llm-proxy``.
- ``set_upstream_key(key)`` — called once at app startup to hand the
  real OpenRouter key to the proxy without putting it back in env.
"""
from __future__ import annotations

import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

_UPSTREAM_BASE = "https://openrouter.ai/api/v1"
# Headers we never forward from the subprocess to upstream, or from
# upstream back to the subprocess. Authorization is rewritten below.
_HOP_BY_HOP = {
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "transfer-encoding",
    "upgrade",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
}


@dataclass
class _SessionEntry:
    session_id: str
    created_at: float
    expires_at: float
    # Future hook points for M6-style enforcement:
    request_count: int = 0
    max_requests: Optional[int] = None


class SessionTokenRegistry:
    """In-memory bearer-token store for per-session proxy access.

    Thread-safety note: FastAPI handles requests in the asyncio event
    loop, so a plain dict is fine for now. If we ever move to a
    multi-worker setup, this needs to become a shared store (Redis).
    """

    def __init__(self, default_ttl_seconds: int = 3600):
        self._tokens: dict[str, _SessionEntry] = {}
        self._default_ttl = default_ttl_seconds

    def issue(
        self,
        session_id: str,
        *,
        ttl_seconds: Optional[int] = None,
        max_requests: Optional[int] = None,
    ) -> str:
        token = secrets.token_urlsafe(32)
        now = time.time()
        self._tokens[token] = _SessionEntry(
            session_id=session_id,
            created_at=now,
            expires_at=now + (ttl_seconds or self._default_ttl),
            max_requests=max_requests,
        )
        return token

    def revoke(self, token: str) -> None:
        self._tokens.pop(token, None)

    def validate(self, token: str) -> _SessionEntry:
        """Return the session entry for ``token`` or raise HTTPException(401)."""
        entry = self._tokens.get(token)
        if entry is None:
            raise HTTPException(status_code=401, detail="invalid session token")
        if time.time() >= entry.expires_at:
            self._tokens.pop(token, None)
            raise HTTPException(status_code=401, detail="session token expired")
        if entry.max_requests is not None and entry.request_count >= entry.max_requests:
            raise HTTPException(status_code=429, detail="session request limit reached")
        entry.request_count += 1
        return entry

    def __len__(self) -> int:
        return len(self._tokens)


# Module-level singletons populated at app startup.
_registry = SessionTokenRegistry()
_upstream_key: Optional[str] = None
_client: Optional[httpx.AsyncClient] = None


def get_registry() -> SessionTokenRegistry:
    return _registry


def set_upstream_key(key: str) -> None:
    """Stash the real OpenRouter key in module memory. Called once at startup."""
    global _upstream_key
    _upstream_key = key


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(base_url=_UPSTREAM_BASE, timeout=httpx.Timeout(60.0, read=None))
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


router = APIRouter(prefix="/llm-proxy", tags=["llm-proxy"])


def _extract_bearer(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    return auth[len("bearer ") :].strip()


def _only_localhost(request: Request) -> None:
    # The proxy is for the spawned subprocess on the same host — never the browser.
    client_host = request.client.host if request.client else None
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(status_code=403, detail="proxy is localhost-only")


@router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def proxy(path: str, request: Request):
    _only_localhost(request)
    token = _extract_bearer(request)
    _registry.validate(token)

    if _upstream_key is None:
        logger.error("llm-proxy invoked but upstream key not configured")
        raise HTTPException(status_code=503, detail="proxy not configured")

    # Build forwarded headers: strip hop-by-hop and substitute Authorization.
    fwd_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP and k.lower() != "authorization"
    }
    fwd_headers["authorization"] = f"Bearer {_upstream_key}"

    client = _get_client()
    upstream_req = client.build_request(
        request.method,
        "/" + path,
        params=request.query_params,
        content=await request.body(),
        headers=fwd_headers,
    )
    upstream_resp = await client.send(upstream_req, stream=True)

    resp_headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }

    async def _iter():
        try:
            async for chunk in upstream_resp.aiter_raw():
                yield chunk
        finally:
            await upstream_resp.aclose()

    return StreamingResponse(
        _iter(),
        status_code=upstream_resp.status_code,
        headers=resp_headers,
        media_type=upstream_resp.headers.get("content-type"),
    )
