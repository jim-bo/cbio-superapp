"""Tests for M8: `cbio beta serve` refuses to bind publicly with terminal enabled."""
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from cbioportal.cli.commands import beta


runner = CliRunner()


@pytest.fixture(autouse=True)
def fake_server(monkeypatch):
    """Stub out server.run so no real uvicorn is launched."""
    called = {}

    def fake_run(port, host, workers):
        called["port"] = port
        called["host"] = host
        called["workers"] = workers

    monkeypatch.setattr(beta.server, "run", fake_run)
    return called


def test_localhost_bind_always_allowed(monkeypatch, fake_server):
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    result = runner.invoke(beta.app, ["serve", "--host", "127.0.0.1", "--port", "8002"])
    assert result.exit_code == 0
    assert fake_server["host"] == "127.0.0.1"


def test_public_bind_refused_when_terminal_enabled(monkeypatch, fake_server):
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    monkeypatch.delenv("CBIO_TERMINAL_ALLOW_PUBLIC_BIND", raising=False)
    result = runner.invoke(beta.app, ["serve", "--host", "0.0.0.0", "--port", "8002"])
    assert result.exit_code == 2
    assert "REFUSED" in result.output
    assert "localhost" in result.output
    # server.run never got called.
    assert "host" not in fake_server


def test_public_bind_allowed_with_override(monkeypatch, fake_server):
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("CBIO_TERMINAL_ALLOW_PUBLIC_BIND", "1")
    result = runner.invoke(beta.app, ["serve", "--host", "0.0.0.0", "--port", "8002"])
    assert result.exit_code == 0
    assert "WARNING" in result.output
    assert fake_server["host"] == "0.0.0.0"


def test_public_bind_allowed_when_terminal_disabled(monkeypatch, fake_server):
    monkeypatch.delenv("CBIO_TERMINAL_ENABLED", raising=False)
    result = runner.invoke(beta.app, ["serve", "--host", "0.0.0.0", "--port", "8002"])
    assert result.exit_code == 0
    assert fake_server["host"] == "0.0.0.0"
