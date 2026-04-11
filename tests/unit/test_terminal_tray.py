"""Tests for the terminal tray UI integration in base.html."""
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from cbioportal.web.routes import terminal as terminal_router

_TEMPLATES_DIR = (
    Path(__file__).resolve().parents[2] / "src" / "cbioportal" / "web" / "templates"
)


def _render_base(env_enabled: bool, monkeypatch) -> str:
    """Render base.html directly via Jinja2 with the terminal_enabled global wired up."""
    if env_enabled:
        monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    else:
        monkeypatch.delenv("CBIO_TERMINAL_ENABLED", raising=False)

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    templates.env.globals["terminal_enabled"] = terminal_router.terminal_enabled
    tpl = templates.env.get_template("base.html")
    return tpl.render()


def test_tray_not_rendered_when_flag_off(monkeypatch):
    html = _render_base(env_enabled=False, monkeypatch=monkeypatch)
    assert "cbio-terminal-tray" not in html
    assert "cbio-terminal-launcher" not in html


def test_tray_rendered_when_flag_on(monkeypatch):
    html = _render_base(env_enabled=True, monkeypatch=monkeypatch)
    assert 'id="cbio-terminal-tray"' in html
    assert 'id="cbio-terminal-launcher"' in html
    # The iframe src is lazy-loaded via data-src; the real src attribute
    # must NOT be present on page load so we don't cold-start the
    # subprocess for users who never open the tray.
    assert 'data-src="/terminal"' in html


def test_terminal_page_has_frame_ancestors_self(monkeypatch):
    """/terminal must set frame-ancestors 'self' + X-Frame-Options: SAMEORIGIN."""
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    app = FastAPI()
    app.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    app.include_router(terminal_router.router)
    client = TestClient(app)

    r = client.get("/terminal")
    assert r.status_code == 200

    csp = r.headers.get("content-security-policy", "")
    assert "frame-ancestors" in csp
    assert "'self'" in csp

    xfo = r.headers.get("x-frame-options", "")
    assert xfo.upper() == "SAMEORIGIN"
