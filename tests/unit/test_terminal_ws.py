"""Tests for the terminal WebSocket endpoint and CbioAppService."""
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from cbioportal.web.routes import terminal as terminal_router
from cbioportal.web.routes.terminal import (
    _CSRF_COOKIE,
    assert_no_leaked_secrets,
    build_subprocess_env,
)
from cbioportal.web.session_limiter import reset_limiter_for_tests
from cbioportal.web.terminal_service import CbioAppService, get_active_sessions

_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "src" / "cbioportal" / "web" / "templates"


@pytest.fixture(autouse=True)
def _clean_sessions():
    """Reset session tracking between tests."""
    get_active_sessions().clear()
    reset_limiter_for_tests()
    yield
    get_active_sessions().clear()


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    a = FastAPI()
    a.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    a.include_router(terminal_router.router)
    return a


@pytest.fixture
def client(app):
    return TestClient(app)


def _get_csrf_token(client):
    """GET /terminal to obtain a CSRF token cookie."""
    r = client.get("/terminal")
    return r.cookies.get(_CSRF_COOKIE, "")


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------


def test_ws_rejects_when_flag_off(monkeypatch):
    monkeypatch.delenv("CBIO_TERMINAL_ENABLED", raising=False)
    a = FastAPI()
    a.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    a.include_router(terminal_router.router)
    c = TestClient(a)
    with pytest.raises(Exception):
        # WebSocket should be closed immediately with code 4004.
        with c.websocket_connect("/terminal/ws"):
            pass


# ---------------------------------------------------------------------------
# CSRF
# ---------------------------------------------------------------------------


def test_ws_rejects_missing_csrf(client):
    with pytest.raises(Exception):
        with client.websocket_connect("/terminal/ws"):
            pass


def test_ws_rejects_without_csrf_cookie(app):
    """A fresh client with no CSRF cookie should be rejected."""
    fresh_client = TestClient(app)
    with pytest.raises(Exception):
        with fresh_client.websocket_connect("/terminal/ws"):
            pass


# ---------------------------------------------------------------------------
# CbioAppService env
# ---------------------------------------------------------------------------


def test_cbio_app_service_env_has_textual_driver():
    """The built env must include the Textual web driver."""
    env = build_subprocess_env(
        "/tmp/scratch",
        session_id="test-session",
        proxy_base_url="http://127.0.0.1:8002/llm-proxy",
    )
    svc = CbioAppService(
        "echo hello",
        spawn_env=env,
        spawn_cwd="/tmp/scratch",
        write_bytes=AsyncMock(),
        write_str=AsyncMock(),
        close=AsyncMock(),
        download_manager=None,  # type: ignore[arg-type]
    )
    built = svc._build_environment(80, 24)
    assert built["TEXTUAL_DRIVER"] == "textual.drivers.web_driver:WebDriver"
    assert built["TEXTUAL_COLOR_SYSTEM"] == "truecolor"
    assert built["COLUMNS"] == "80"
    assert built["ROWS"] == "24"


def test_cbio_app_service_env_no_secrets():
    """The built env must pass the leaked-secrets guard."""
    env = build_subprocess_env(
        "/tmp/scratch",
        session_id="test-session",
        proxy_base_url="http://127.0.0.1:8002/llm-proxy",
    )
    svc = CbioAppService(
        "echo hello",
        spawn_env=env,
        spawn_cwd="/tmp/scratch",
        write_bytes=AsyncMock(),
        write_str=AsyncMock(),
        close=AsyncMock(),
        download_manager=None,  # type: ignore[arg-type]
    )
    built = svc._build_environment(80, 24)
    assert_no_leaked_secrets(built)


def test_cbio_app_service_does_not_copy_os_environ():
    """The scrubbed env must NOT contain typical host env vars."""
    env = build_subprocess_env(
        "/tmp/scratch",
        session_id="test-session",
        proxy_base_url="http://127.0.0.1:8002/llm-proxy",
    )
    svc = CbioAppService(
        "echo hello",
        spawn_env=env,
        spawn_cwd="/tmp/scratch",
        write_bytes=AsyncMock(),
        write_str=AsyncMock(),
        close=AsyncMock(),
        download_manager=None,  # type: ignore[arg-type]
    )
    built = svc._build_environment(80, 24)
    # Should not have inherited random host env vars.
    assert "USER" not in built
    assert "SHELL" not in built
    # But should have the web mode signal.
    assert built["CBIO_WEB_MODE"] == "1"
