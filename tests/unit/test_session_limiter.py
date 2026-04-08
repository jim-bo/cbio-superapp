"""Tests for M6: per-IP + global session caps and idle reaping."""
import time

import pytest
from fastapi import HTTPException

from cbioportal.web.session_limiter import SessionLimiter


def _make(**overrides):
    defaults = dict(max_per_ip=2, max_total=5, idle_timeout_seconds=10)
    defaults.update(overrides)
    return SessionLimiter(**defaults)


# ---------------------------------------------------------------------------
# Per-IP cap
# ---------------------------------------------------------------------------


def test_acquires_up_to_per_ip_cap():
    lim = _make(max_per_ip=2)
    lim.acquire("a1", "1.2.3.4")
    lim.acquire("a2", "1.2.3.4")
    assert lim.count_for_ip("1.2.3.4") == 2


def test_rejects_third_session_from_same_ip():
    lim = _make(max_per_ip=2)
    lim.acquire("a1", "1.2.3.4")
    lim.acquire("a2", "1.2.3.4")
    with pytest.raises(HTTPException) as exc:
        lim.acquire("a3", "1.2.3.4")
    assert exc.value.status_code == 429
    assert "1.2.3.4" in exc.value.detail


def test_per_ip_cap_is_per_client():
    lim = _make(max_per_ip=1)
    lim.acquire("a", "1.1.1.1")
    # A different IP is independent.
    lim.acquire("b", "2.2.2.2")
    assert len(lim) == 2


def test_release_frees_slot():
    lim = _make(max_per_ip=1)
    lim.acquire("a", "1.1.1.1")
    with pytest.raises(HTTPException):
        lim.acquire("b", "1.1.1.1")
    lim.release("a")
    lim.acquire("b", "1.1.1.1")  # now fits


# ---------------------------------------------------------------------------
# Global cap
# ---------------------------------------------------------------------------


def test_global_cap_rejects_past_max_total():
    lim = _make(max_per_ip=99, max_total=3)
    for i in range(3):
        lim.acquire(f"s{i}", f"10.0.0.{i}")
    with pytest.raises(HTTPException) as exc:
        lim.acquire("extra", "10.0.0.99")
    assert exc.value.status_code == 503
    assert "capacity" in exc.value.detail.lower()


def test_global_cap_checked_before_per_ip():
    lim = _make(max_per_ip=10, max_total=1)
    lim.acquire("a", "1.1.1.1")
    with pytest.raises(HTTPException) as exc:
        lim.acquire("b", "2.2.2.2")
    assert exc.value.status_code == 503  # global, not 429


# ---------------------------------------------------------------------------
# Duplicate session id
# ---------------------------------------------------------------------------


def test_duplicate_session_id_rejected():
    lim = _make()
    lim.acquire("same", "1.1.1.1")
    with pytest.raises(HTTPException) as exc:
        lim.acquire("same", "2.2.2.2")
    assert exc.value.status_code == 409


# ---------------------------------------------------------------------------
# Idle reaping
# ---------------------------------------------------------------------------


def test_reap_idle_returns_stale_sessions():
    lim = _make(idle_timeout_seconds=0)  # everything is immediately idle
    lim.acquire("a", "1.1.1.1")
    lim.acquire("b", "2.2.2.2")
    time.sleep(0.01)
    reaped = sorted(lim.reap_idle())
    assert reaped == ["a", "b"]
    assert len(lim) == 0


def test_touch_prevents_reap():
    lim = _make(idle_timeout_seconds=5)
    lim.acquire("alive", "1.1.1.1")
    lim.touch("alive")
    assert lim.reap_idle() == []
    assert len(lim) == 1


def test_touch_on_unknown_session_is_noop():
    lim = _make()
    lim.touch("ghost")  # no crash


# ---------------------------------------------------------------------------
# Env-var defaults
# ---------------------------------------------------------------------------


def test_env_vars_configure_defaults(monkeypatch):
    monkeypatch.setenv("CBIO_TERMINAL_MAX_PER_IP", "5")
    monkeypatch.setenv("CBIO_TERMINAL_MAX_TOTAL", "20")
    monkeypatch.setenv("CBIO_TERMINAL_IDLE_SECONDS", "60")
    lim = SessionLimiter()
    assert lim._max_per_ip == 5
    assert lim._max_total == 20
    assert lim._idle == 60
