"""Tests for M3: path allowlist helper for filesystem-touching tools."""
import os
from pathlib import Path

import pytest

from cbioportal.cli.tools._paths import PathNotAllowed, resolve_safe_path


@pytest.fixture
def studies_dir(tmp_path, monkeypatch):
    """Point CBIO_STUDIES_DIR at a fresh tempdir and cd there."""
    root = tmp_path / "studies"
    root.mkdir()
    monkeypatch.setenv("CBIO_STUDIES_DIR", str(root))
    monkeypatch.chdir(tmp_path)
    return root


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_accepts_file_inside_allowed_root(studies_dir):
    p = studies_dir / "msk_chord_2024" / "meta_study.txt"
    p.parent.mkdir()
    p.write_text("x")
    resolved = resolve_safe_path(str(p))
    assert resolved == p.resolve()


def test_accepts_directory_inside_allowed_root(studies_dir):
    p = studies_dir / "msk_chord_2024"
    p.mkdir()
    resolved = resolve_safe_path(str(p))
    assert resolved == p.resolve()


def test_accepts_relative_path(studies_dir, monkeypatch):
    p = studies_dir / "x"
    p.mkdir()
    monkeypatch.chdir(studies_dir)
    resolved = resolve_safe_path("x")
    assert resolved == p.resolve()


def test_must_exist_flag_rejects_missing(studies_dir):
    with pytest.raises(PathNotAllowed, match="does not exist"):
        resolve_safe_path(str(studies_dir / "nope"), must_exist=True)


# ---------------------------------------------------------------------------
# Traversal + escape
# ---------------------------------------------------------------------------


def test_rejects_parent_traversal(studies_dir):
    with pytest.raises(PathNotAllowed):
        resolve_safe_path(str(studies_dir / ".." / ".." / "etc" / "passwd"))


def test_rejects_absolute_outside_root(studies_dir):
    with pytest.raises(PathNotAllowed):
        resolve_safe_path("/etc/passwd")


def test_rejects_home_dotenv(studies_dir, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("OPENROUTER_API_KEY=sk-real")
    with pytest.raises(PathNotAllowed):
        resolve_safe_path(str(dotenv))


def test_rejects_proc_self_environ(studies_dir):
    with pytest.raises(PathNotAllowed, match="forbidden system tree"):
        resolve_safe_path("/proc/self/environ")


def test_rejects_sys_and_dev(studies_dir):
    with pytest.raises(PathNotAllowed):
        resolve_safe_path("/sys/kernel/debug")
    with pytest.raises(PathNotAllowed):
        resolve_safe_path("/dev/null")


def test_rejects_symlink_escape(studies_dir, tmp_path):
    """A symlink inside the allowlist pointing OUT must be rejected."""
    secret = tmp_path / "secret.txt"
    secret.write_text("top secret")
    link = studies_dir / "escape"
    link.symlink_to(secret)
    with pytest.raises(PathNotAllowed):
        resolve_safe_path(str(link))


def test_rejects_empty_path(studies_dir):
    with pytest.raises(PathNotAllowed):
        resolve_safe_path("")


def test_rejects_none_path(studies_dir):
    with pytest.raises(PathNotAllowed):
        resolve_safe_path(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Root configuration
# ---------------------------------------------------------------------------


def test_multiple_roots_via_colon(tmp_path, monkeypatch):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    monkeypatch.setenv("CBIO_STUDIES_DIR", f"{a}{os.pathsep}{b}")
    monkeypatch.chdir(tmp_path)
    assert resolve_safe_path(str(a / "x.txt")) == (a / "x.txt").resolve()
    assert resolve_safe_path(str(b / "y.txt")) == (b / "y.txt").resolve()


def test_default_roots_cwd_studies_and_data(tmp_path, monkeypatch):
    monkeypatch.delenv("CBIO_STUDIES_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "studies").mkdir()
    (tmp_path / "data").mkdir()
    assert resolve_safe_path(str(tmp_path / "studies" / "x"))
    assert resolve_safe_path(str(tmp_path / "data" / "y"))
    with pytest.raises(PathNotAllowed):
        resolve_safe_path(str(tmp_path / "other" / "z"))


def test_extra_roots_permit_scratch_dir(studies_dir, tmp_path):
    scratch = tmp_path / "session-scratch"
    scratch.mkdir()
    target = scratch / "uploaded-study"
    target.mkdir()
    # Without extra_roots: rejected.
    with pytest.raises(PathNotAllowed):
        resolve_safe_path(str(target))
    # With extra_roots: accepted.
    resolved = resolve_safe_path(str(target), extra_roots=[scratch])
    assert resolved == target.resolve()


def test_no_roots_configured_at_all(tmp_path, monkeypatch):
    monkeypatch.delenv("CBIO_STUDIES_DIR", raising=False)
    monkeypatch.chdir(tmp_path)  # no studies/ or data/ subdirs
    with pytest.raises(PathNotAllowed, match="no allowlisted roots"):
        resolve_safe_path(str(tmp_path / "anything"))


# ---------------------------------------------------------------------------
# Integration: validate_study_folder rejects a bad path
# ---------------------------------------------------------------------------


def test_validate_study_folder_refuses_dotenv(studies_dir, tmp_path):
    import asyncio

    from cbioportal.cli.tools.study_loader import validate_study_folder

    dotenv = tmp_path / ".env"
    dotenv.write_text("OPENROUTER_API_KEY=sk-real")
    result = asyncio.run(validate_study_folder(str(dotenv)))
    assert result.is_error
    assert "Refused" in result.output
