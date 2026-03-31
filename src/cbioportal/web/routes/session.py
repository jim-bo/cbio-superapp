"""Session service REST API.

Endpoints mirror the legacy cBioPortal session service API so the same
URL patterns work. Anonymous user identity comes from a browser cookie
(cbio_session_token). The raw token is kept browser-side; only its
SHA-256 hash is stored in the database.

Sessions are readable by anyone who knows the UUID (for sharing), but
only writable/deletable by the owner (matched by hashed token).
"""
from __future__ import annotations

import json
import os
import secrets
from typing import Annotated

from fastapi import APIRouter, Body, Cookie, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session as SASession

from cbioportal.core.session_repository import (
    SESSION_TYPES,
    SessionRecord,
    create_session,
    delete_session,
    fetch_settings,
    get_session,
    list_sessions,
    upsert_settings,
)

router = APIRouter(prefix="/api/session")

_TOKEN_COOKIE = "cbio_session_token"
_TOKEN_BYTES = 32  # 256-bit entropy
_COOKIE_MAX_AGE = 60 * 60 * 24 * 365 * 2  # 2 years


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def get_session_db(request: Request):
    """Yield a SQLAlchemy session; always close on exit."""
    db: SASession = request.app.state.session_factory()
    try:
        yield db
    finally:
        db.close()


def ensure_token(
    response: Response,
    cbio_session_token: Annotated[str | None, Cookie()] = None,
) -> str:
    """Return the caller's session token, minting a new one if absent."""
    if cbio_session_token:
        return cbio_session_token
    token = secrets.token_hex(_TOKEN_BYTES)
    response.set_cookie(
        key=_TOKEN_COOKIE,
        value=token,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=os.environ.get("CBIO_SECURE_COOKIES", "0") == "1",
    )
    return token


def _record_to_dict(r: SessionRecord) -> dict:
    return {"id": r.id, "type": r.type, "data": r.data, "checksum": r.checksum}


# ---------------------------------------------------------------------------
# Pydantic request bodies
# ---------------------------------------------------------------------------


class SettingsFetchRequest(BaseModel):
    page: str
    origin: list[str]


class SettingsSaveRequest(BaseModel):
    page: str
    origin: list[str]
    filters: dict | None = None
    gridLayout: list[dict] | None = None
    chartSettings: dict | None = None


# ---------------------------------------------------------------------------
# Specific routes MUST come before generic {session_type} wildcard routes.
# FastAPI matches routes in registration order.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Settings-specific endpoints (mirror legacy /api/session/settings/*)
# ---------------------------------------------------------------------------


@router.post("/settings/fetch")
def fetch_settings_route(
    body: SettingsFetchRequest,
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """POST /api/session/settings/fetch — fetch page settings for (page, origin)."""
    record = fetch_settings(db, body.page, body.origin, token)
    if not record:
        raise HTTPException(404, "No settings found for this page and origin")
    return _record_to_dict(record)


@router.post("/settings", status_code=201)
def save_settings_route(
    body: SettingsSaveRequest,
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """POST /api/session/settings — create or update (upsert) page settings."""
    data = body.model_dump(exclude={"origin"})
    record = upsert_settings(db, body.page, body.origin, data, token)
    return {"id": record.id, "checksum": record.checksum}


# ---------------------------------------------------------------------------
# Virtual study endpoints (mirror legacy /api/session/virtual_study/*)
# ---------------------------------------------------------------------------


@router.post("/virtual_study/save", status_code=201)
def save_virtual_study(
    data: dict = Body(...),
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """POST /api/session/virtual_study/save — save a named virtual study."""
    record = create_session(db, "virtual_study", data, token)
    return {"id": record.id, "checksum": record.checksum}


@router.get("/virtual_study")
def list_virtual_studies(
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """GET /api/session/virtual_study — list the caller's virtual studies."""
    records = list_sessions(db, "virtual_study", token)
    return [_record_to_dict(r) for r in records]


# ---------------------------------------------------------------------------
# Share redirect
# ---------------------------------------------------------------------------


@router.get("/share/{session_id}")
def share_redirect(session_id: str, db: SASession = Depends(get_session_db)):
    """GET /api/session/share/{id} — resolve a session and redirect to the page.

    The target page reads ?session_id= and restores state server-side on render.
    """
    record = get_session(db, session_id)
    if not record:
        raise HTTPException(404, "Session not found")

    if record.type == "settings":
        origin_key = record.data.get("origin_key", "[]")
        study_ids = ",".join(json.loads(origin_key))
        page = record.data.get("page", "study_view")
        if page == "study_view":
            return RedirectResponse(
                f"/study/summary?id={study_ids}&session_id={session_id}"
            )

    if record.type == "virtual_study":
        return RedirectResponse(f"/?session_id={session_id}")

    raise HTTPException(400, "Session type does not support direct sharing")


# ---------------------------------------------------------------------------
# Generic CRUD endpoints — MUST come after all specific routes above.
# ---------------------------------------------------------------------------


@router.post("/{session_type}", status_code=201)
def create_session_route(
    session_type: str,
    data: dict = Body(...),
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """POST /api/session/{type} — create a session, return {id, checksum}."""
    if session_type not in SESSION_TYPES:
        raise HTTPException(400, f"Unknown session type: {session_type!r}")
    record = create_session(db, session_type, data, token)
    return {"id": record.id, "checksum": record.checksum}


@router.get("/{session_type}/{session_id}")
def get_session_route(
    session_type: str,
    session_id: str,
    db: SASession = Depends(get_session_db),
):
    """GET /api/session/{type}/{id} — retrieve a session (public, no auth)."""
    record = get_session(db, session_id)
    if not record or record.type != session_type:
        raise HTTPException(404, "Session not found")
    return _record_to_dict(record)


@router.get("/{session_type}")
def list_sessions_route(
    session_type: str,
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """GET /api/session/{type} — list the caller's sessions of that type."""
    if session_type not in SESSION_TYPES:
        raise HTTPException(400, f"Unknown session type: {session_type!r}")
    records = list_sessions(db, session_type, token)
    return [_record_to_dict(r) for r in records]


@router.delete("/{session_type}/{session_id}", status_code=204)
def delete_session_route(
    session_type: str,
    session_id: str,
    db: SASession = Depends(get_session_db),
    token: str = Depends(ensure_token),
):
    """DELETE /api/session/{type}/{id} — delete the caller's own session."""
    if not delete_session(db, session_id, token):
        raise HTTPException(404, "Session not found or not owned by caller")
