"""SessionSyncMiddleware — server-side session auto-save.

Intercepts every successful POST to /study/summary/chart/* and fire-and-forgets
a session upsert using the study_id and filter_json that are already in the
request body. This means filter state is persisted automatically without any
JavaScript changes to the chart update loop.

Starlette/FastAPI form-body reads are single-pass: the route handler consumes
the body stream. This middleware buffers the raw bytes *before* passing the
request to the next handler, then re-injects them so the route handler sees a
fresh stream.
"""
from __future__ import annotations

import asyncio
import json
import urllib.parse

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

        if should_sync:
            # Buffer the body so the route handler can still read it.
            raw_body = await request.body()
            request = _rebind_body(request, raw_body)

        response = await call_next(request)

        if should_sync and response.status_code == 200:
            asyncio.create_task(
                _save_session(
                    raw_body,
                    request.cookies.get("cbio_session_token"),
                    request.app.state.session_factory,
                )
            )

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
    raw_body: bytes,
    raw_token: str | None,
    session_factory,
) -> None:
    """Parse form fields and upsert a settings session. Never raises."""
    try:
        if not raw_token:
            return

        # Parse application/x-www-form-urlencoded body.
        form = dict(urllib.parse.parse_qsl(raw_body.decode("utf-8", errors="replace")))
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
