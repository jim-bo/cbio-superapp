"""Tests for M2: origin allowlist + CSRF double-submit on /terminal."""
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from cbioportal.web.routes import terminal as terminal_router
from cbioportal.web.routes.terminal import (
    check_origin,
    validate_csrf,
    issue_csrf_cookie,
)

_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "src" / "cbioportal" / "web" / "templates"


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    a = FastAPI()
    a.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    a.include_router(terminal_router.router)

    # A dummy WS-upgrade-like POST endpoint that applies the M2 checks,
    # so we can exercise them end-to-end.
    @a.post("/terminal/upgrade")
    def upgrade(request):  # type: ignore[no-untyped-def]
        from fastapi import Request as FReq  # noqa
        check_origin(request)
        validate_csrf(request)
        return {"ok": True}

    # The dep-injection above is a bit awkward for a sync handler; redo
    # properly with Request parameter.
    a.router.routes = [r for r in a.router.routes if getattr(r, "path", "") != "/terminal/upgrade"]

    from fastapi import Request

    @a.post("/terminal/upgrade")
    def upgrade2(request: Request):
        check_origin(request)
        validate_csrf(request)
        return {"ok": True}

    return a


@pytest.fixture
def client(app):
    return TestClient(app)


# ---------------------------------------------------------------------------
# Origin allowlist
# ---------------------------------------------------------------------------


def test_get_terminal_accepts_same_origin(client):
    r = client.get("/terminal")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")
    # CSRF cookie was set
    assert "cbio_terminal_csrf" in r.cookies


def test_get_terminal_accepts_no_origin_header(client):
    # No Origin header → same-origin navigation; allowed.
    r = client.get("/terminal", headers={})
    assert r.status_code == 200


def test_get_terminal_rejects_foreign_origin(client):
    r = client.get(
        "/terminal",
        headers={"Origin": "https://evil.example.com"},
    )
    assert r.status_code == 403
    assert "origin" in r.json()["detail"].lower()


def test_allowlist_env_var_permits_extra_origin(client, monkeypatch):
    monkeypatch.setenv(
        "CBIO_TERMINAL_ALLOWED_ORIGINS",
        "https://dev.example.com,http://localhost:3000",
    )
    r = client.get(
        "/terminal",
        headers={"Origin": "https://dev.example.com"},
    )
    assert r.status_code == 200


def test_invalid_origin_header_rejected(client):
    r = client.get("/terminal", headers={"Origin": "not-a-url"})
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# CSRF double-submit
# ---------------------------------------------------------------------------


def test_upgrade_requires_csrf_cookie_and_header(client):
    r = client.post("/terminal/upgrade")
    assert r.status_code == 403
    assert "csrf" in r.json()["detail"].lower()


def test_upgrade_with_only_cookie_rejected(client):
    # First GET to obtain the cookie.
    client.get("/terminal")
    # Then POST without the header.
    r = client.post("/terminal/upgrade")
    assert r.status_code == 403


def test_upgrade_with_matching_cookie_and_header_accepted(client):
    first = client.get("/terminal")
    token = first.cookies["cbio_terminal_csrf"]
    r = client.post(
        "/terminal/upgrade",
        headers={"x-cbio-terminal-csrf": token},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_upgrade_with_mismatched_token_rejected(client):
    client.get("/terminal")
    r = client.post(
        "/terminal/upgrade",
        headers={"x-cbio-terminal-csrf": "not-the-real-token"},
    )
    assert r.status_code == 403
    assert "mismatch" in r.json()["detail"].lower()


def test_feature_flag_off_returns_404(monkeypatch):
    monkeypatch.delenv("CBIO_TERMINAL_ENABLED", raising=False)
    a = FastAPI()
    a.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    a.include_router(terminal_router.router)
    c = TestClient(a)
    r = c.get("/terminal")
    assert r.status_code == 404
