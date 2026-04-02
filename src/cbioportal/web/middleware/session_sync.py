"""SessionSyncMiddleware — server-side session auto-save.

Intercepts every successful POST to /study/summary/chart/* and saves the
session state synchronously before returning the response.

Why synchronous? `BaseHTTPMiddleware.call_next` returns a streaming response
wrapper that does not honour `.background` (the body streams through a
different mechanism). `asyncio.create_task` inside the middleware can also be
GC'd before running. Since the save is a local SQLite write (<1 ms), running
it inline has no meaningful impact on latency.

Starlette/FastAPI form-body reads are single-pass: the route handler consumes
the body stream. This middleware buffers the raw bytes *before* passing the
request to the next handler, then re-injects them so the route handler sees a
fresh stream. Form parsing (including multipart) is done on a second rebind
using Starlette's own parser so both content-types are handled correctly.
"""
from __future__ import annotations

import json

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from cbioportal.core.session_repository import upsert_settings


class SessionSyncMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next) -> Response:
        # Only intercept chart POSTs — everything else is untouched.
        should_sync = (
            request.method == "POST"
            and request.url.path.startswith("/study/summary/chart/")
        )

        if not should_sync:
            return await call_next(request)

        # Buffer the body so the route handler can still read it.
        raw_body = await request.body()
        raw_token = request.cookies.get("cbio_session_token")
        session_factory = request.app.state.session_factory

        response = await call_next(_rebind_body(request, raw_body))

        # Save synchronously — await the async form parse inline so it
        # completes before we return the response.  SQLite write is <1 ms.
        if response.status_code == 200 and raw_token:
            await _save_session(request, raw_body, raw_token, session_factory)

        return response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rebind_body(request: Request, raw_body: bytes) -> Request:
    """Return a new Request that replays the buffered body bytes."""

    async def _receive():
        return {"type": "http.request", "body": raw_body, "more_body": False}

    return Request(request.scope, _receive, request._send)


async def _save_session(
    request: Request,
    raw_body: bytes,
    raw_token: str,
    session_factory,
) -> None:
    """Parse form fields via Starlette's parser and upsert a settings session. Never raises."""
    try:
        # Use a fresh rebound request so Starlette's form parser (which handles
        # both multipart/form-data and application/x-www-form-urlencoded) gets
        # a clean body stream — the original was already consumed by call_next.
        form_req = _rebind_body(request, raw_body)
        form = await form_req.form()
        study_id: str | None = form.get("study_id")
        filter_json: str | None = form.get("filter_json")

        if not study_id or not filter_json:
            return

        filters = json.loads(filter_json)
        db = session_factory()
        try:
            upsert_settings(
                db,
                page="study_view",
                origin=[study_id],
                data={"filters": filters},
                raw_token=raw_token,
            )
        finally:
            db.close()
    except Exception:
        # Session save must never affect the main request path.
        pass
