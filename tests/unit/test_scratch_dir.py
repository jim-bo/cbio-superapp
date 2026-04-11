"""Tests for M5: per-session scratch dir + conversation logging disabled under web mode."""
from pathlib import Path

import pytest

from cbioportal.web.routes.terminal import (
    build_spawn_kwargs,
    session_scratch_dir,
)


# ---------------------------------------------------------------------------
# session_scratch_dir
# ---------------------------------------------------------------------------


def test_scratch_dir_created_and_cleaned_up():
    created_path: Path | None = None
    with session_scratch_dir("sess-1") as scratch:
        created_path = scratch
        assert scratch.exists()
        assert scratch.is_dir()
        assert scratch.stat().st_mode & 0o777 == 0o700
        # Agent writes a file — should land inside the scratch dir.
        (scratch / ".cbio" / "convos").mkdir(parents=True)
        (scratch / ".cbio" / "convos" / "foo.jsonl").write_text("x")
    # After the context exits, the whole dir is gone.
    assert created_path is not None
    assert not created_path.exists()


def test_scratch_dir_parent_is_cbio_terminal():
    with session_scratch_dir("sess-2") as scratch:
        assert scratch.parent.name == "cbio-terminal"


def test_scratch_dir_name_includes_session_id():
    with session_scratch_dir("my-session") as scratch:
        assert scratch.name.startswith("my-session-")


def test_multiple_scratch_dirs_are_isolated():
    paths = []
    with session_scratch_dir("a") as a:
        with session_scratch_dir("b") as b:
            assert a != b
            assert a.exists()
            assert b.exists()
            paths = [a, b]
    for p in paths:
        assert not p.exists()


# ---------------------------------------------------------------------------
# build_spawn_kwargs bundles env + cwd
# ---------------------------------------------------------------------------


def test_build_spawn_kwargs_bundles_env_and_cwd(monkeypatch):
    monkeypatch.setenv("CBIO_WEB_OPENROUTER_API_KEY", "sk-web-capped")
    with session_scratch_dir("spawn-test") as scratch:
        kwargs = build_spawn_kwargs(
            session_id="spawn-test",
            scratch_dir=scratch,
            proxy_base_url="http://127.0.0.1:8002/llm-proxy",
        )
        assert kwargs["cwd"] == str(scratch)
        env = kwargs["env"]
        assert env["HOME"] == str(scratch)
        assert env["CBIO_WEB_MODE"] == "1"
        assert env["OPENROUTER_BASE_URL"] == "http://127.0.0.1:8002/llm-proxy"
        # The OPENROUTER_API_KEY is a session token, not the real one.
        assert not env["OPENROUTER_API_KEY"].startswith("sk-")


# ---------------------------------------------------------------------------
# _launch_chat_app: web mode forces log_path=None
# ---------------------------------------------------------------------------


def test_launch_chat_app_disables_logging_under_web_mode(monkeypatch):
    """Under CBIO_WEB_MODE=1, --log is ignored and log_path is None."""
    monkeypatch.setenv("CBIO_WEB_MODE", "1")
    captured = {}

    class FakeChatApp:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self):
            pass

    # Patch cli_textual.app.ChatApp before _launch_chat_app imports it.
    import cli_textual.app as cli_app_mod
    monkeypatch.setattr(cli_app_mod, "ChatApp", FakeChatApp)

    # Stub out the pydantic-ai model construction so we don't need keys.
    monkeypatch.setenv("OPENROUTER_BASE_URL", "http://127.0.0.1:9/llm-proxy")
    monkeypatch.setenv("OPENROUTER_API_KEY", "session-token-abc")

    from cbioportal.cli.main import _launch_chat_app

    _launch_chat_app(log=True)  # caller asks for logging…
    # …but web mode ignores it.
    assert captured["log_path"] is None


def test_launch_chat_app_respects_log_flag_outside_web_mode(monkeypatch, tmp_path):
    monkeypatch.delenv("CBIO_WEB_MODE", raising=False)
    monkeypatch.chdir(tmp_path)
    captured = {}

    class FakeChatApp:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self):
            pass

    import cli_textual.app as cli_app_mod
    monkeypatch.setattr(cli_app_mod, "ChatApp", FakeChatApp)

    from cbioportal.cli.main import _launch_chat_app

    _launch_chat_app(log=True)
    assert captured["log_path"] is not None
    # And it's under the cwd's .cbio/convos, per the project convention.
    assert ".cbio/convos" in str(captured["log_path"])
