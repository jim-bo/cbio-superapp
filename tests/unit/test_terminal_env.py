"""Tests for the web terminal subprocess env + llm-proxy token registry (M1)."""
import time

import pytest
from fastapi import HTTPException

from cbioportal.web.routes.terminal import (
    build_subprocess_env,
    assert_no_leaked_secrets,
    terminal_enabled,
)
from cbioportal.web.llm_proxy import SessionTokenRegistry, get_registry


# ---------------------------------------------------------------------------
# build_subprocess_env: no real secrets leak into the subprocess
# ---------------------------------------------------------------------------


def test_env_contains_no_real_key_only_session_token(monkeypatch):
    # Simulate a developer machine full of sensitive vars.
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-real-developer-key")
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "cf-secret")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@host/db")
    monkeypatch.setenv("CBIO_WEB_OPENROUTER_API_KEY", "sk-web-capped-key")

    env = build_subprocess_env(
        "/tmp/scratch",
        session_id="test-session",
        proxy_base_url="http://127.0.0.1:8002/llm-proxy",
    )

    # The subprocess's OPENROUTER_API_KEY is a session token, not any
    # provider key — in particular, not the "sk-" prefixed dev key.
    assert not env["OPENROUTER_API_KEY"].startswith("sk-")
    assert len(env["OPENROUTER_API_KEY"]) >= 32

    # It points at the local proxy, not openrouter.ai.
    assert env["OPENROUTER_BASE_URL"] == "http://127.0.0.1:8002/llm-proxy"
    assert "openrouter.ai" not in env["OPENROUTER_BASE_URL"]

    assert env["CBIO_WEB_MODE"] == "1"
    assert env["HOME"] == "/tmp/scratch"

    # None of the host's sensitive vars leaked through.
    assert "CLOUDFLARE_API_TOKEN" not in env
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in env
    assert "DATABASE_URL" not in env
    assert "CBIO_WEB_OPENROUTER_API_KEY" not in env

    # The hard guard passes: no "sk-" values anywhere in env.
    assert_no_leaked_secrets(env)


def test_guard_catches_accidental_sk_leak():
    bad_env = {"OPENROUTER_API_KEY": "sk-oops", "HOME": "/tmp"}
    with pytest.raises(AssertionError, match="sk-"):
        assert_no_leaked_secrets(bad_env)


def test_feature_flag_default_off(monkeypatch):
    monkeypatch.delenv("CBIO_TERMINAL_ENABLED", raising=False)
    assert terminal_enabled() is False
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "1")
    assert terminal_enabled() is True
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "true")
    assert terminal_enabled() is True
    monkeypatch.setenv("CBIO_TERMINAL_ENABLED", "0")
    assert terminal_enabled() is False


# ---------------------------------------------------------------------------
# SessionTokenRegistry
# ---------------------------------------------------------------------------


def test_issued_token_validates_then_revokes():
    reg = SessionTokenRegistry()
    token = reg.issue("sess-1")
    entry = reg.validate(token)
    assert entry.session_id == "sess-1"

    reg.revoke(token)
    with pytest.raises(HTTPException) as exc:
        reg.validate(token)
    assert exc.value.status_code == 401


def test_expired_token_rejected():
    reg = SessionTokenRegistry()
    token = reg.issue("sess-exp", ttl_seconds=-1)  # already expired
    with pytest.raises(HTTPException) as exc:
        reg.validate(token)
    assert exc.value.status_code == 401
    assert "expired" in exc.value.detail.lower()


def test_request_limit_enforced():
    reg = SessionTokenRegistry()
    token = reg.issue("sess-cap", max_requests=2)
    reg.validate(token)
    reg.validate(token)
    with pytest.raises(HTTPException) as exc:
        reg.validate(token)
    assert exc.value.status_code == 429


def test_unknown_token_rejected():
    reg = SessionTokenRegistry()
    with pytest.raises(HTTPException) as exc:
        reg.validate("not-a-real-token")
    assert exc.value.status_code == 401


def test_build_env_and_registry_roundtrip():
    """The token placed in the subprocess env is the same one registered."""
    env = build_subprocess_env(
        "/tmp/scratch",
        session_id="roundtrip",
        proxy_base_url="http://127.0.0.1:8002/llm-proxy",
    )
    token = env["OPENROUTER_API_KEY"]
    entry = get_registry().validate(token)
    assert entry.session_id == "roundtrip"
    get_registry().revoke(token)
