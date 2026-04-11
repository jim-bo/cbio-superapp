"""Path allowlist for filesystem-touching cbio tools (M3).

The LLM agent can pass arbitrary paths to any tool that accepts one.
Without a gate, a crafted prompt (or a prompt-injection payload hidden
inside a tool's own output) can reach ``/proc/self/environ``, ``~/.env``,
``.cbio/convos/*.jsonl``, or any other secret-bearing file the cbio
process can read.

This module provides a single chokepoint: ``resolve_safe_path`` takes a
user-supplied path and either returns a fully-resolved absolute path
that is definitely inside an allowlisted root, or raises
``PathNotAllowed``.

Allowed roots, in priority order:

1. ``$CBIO_STUDIES_DIR`` (colon-separated list), if set
2. ``<cwd>/studies`` and ``<cwd>/data`` — the default developer layout
3. A temporary per-session scratch dir, if caller passes ``extra_roots``

Resolution semantics:

- Input is expanded (``~``) and resolved (``realpath`` via
  ``Path.resolve(strict=False)``), so ``..`` and symlinks are
  flattened BEFORE the prefix check.
- The final resolved path must have one of the allowed roots as a
  strict prefix (``Path.is_relative_to``).
- Symlink escapes: we also resolve the root itself and compare
  resolved-vs-resolved, so a symlink inside the root pointing OUT is
  rejected.
- ``/proc``, ``/sys``, and ``/dev`` are never allowed, even if an
  allowlisted root somehow lands there.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


class PathNotAllowed(ValueError):
    """Raised when a user-supplied path escapes the allowlist."""


_ALWAYS_FORBIDDEN = ("/proc", "/sys", "/dev")


def _default_roots() -> list[Path]:
    roots: list[Path] = []
    env_roots = os.environ.get("CBIO_STUDIES_DIR", "").strip()
    if env_roots:
        for entry in env_roots.split(os.pathsep):
            entry = entry.strip()
            if entry:
                roots.append(Path(entry).expanduser())
    if not roots:
        cwd = Path.cwd()
        roots.extend([cwd / "studies", cwd / "data"])
    return roots


def _resolve_root(root: Path) -> Path | None:
    """Resolve an allowlist root, skipping non-existent ones.

    We permit non-existent roots to simply drop off the allowlist
    rather than crashing — a dev machine without a ``studies/`` folder
    should just be "nothing is allowed" for that root, not a hard
    error.
    """
    try:
        resolved = root.expanduser().resolve(strict=False)
    except (OSError, RuntimeError):
        return None
    # Drop non-existent roots — a default ./studies that doesn't exist
    # should mean "no root", not "a phantom root nothing is under".
    if not resolved.exists():
        return None
    return resolved


def _is_always_forbidden(resolved: Path) -> bool:
    s = str(resolved)
    return any(s == p or s.startswith(p + "/") for p in _ALWAYS_FORBIDDEN)


def resolve_safe_path(
    user_path: str,
    *,
    extra_roots: Iterable[Path] | None = None,
    must_exist: bool = False,
) -> Path:
    """Resolve and allowlist-check a user-supplied path.

    Args:
        user_path: The raw path string from the agent/tool argument.
        extra_roots: Additional roots to permit for this call only
            (e.g. a per-session scratch dir).
        must_exist: If True, also verify the path exists on disk.

    Returns:
        The fully-resolved absolute :class:`Path`.

    Raises:
        PathNotAllowed: the path is outside every allowed root, or
            lands in ``/proc``/``/sys``/``/dev``, or fails existence
            check.
    """
    if not user_path or not isinstance(user_path, str):
        raise PathNotAllowed("empty path")

    try:
        resolved = Path(user_path).expanduser().resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise PathNotAllowed(f"cannot resolve path: {exc}") from exc

    if _is_always_forbidden(resolved):
        raise PathNotAllowed(
            f"path {user_path!r} resolves into a forbidden system tree "
            f"({resolved})"
        )

    roots: list[Path] = []
    for r in _default_roots():
        rr = _resolve_root(r)
        if rr is not None:
            roots.append(rr)
    if extra_roots:
        for r in extra_roots:
            rr = _resolve_root(Path(r))
            if rr is not None:
                roots.append(rr)

    if not roots:
        raise PathNotAllowed(
            "no allowlisted roots are configured; set CBIO_STUDIES_DIR "
            "or create a ./studies or ./data directory"
        )

    if not any(_is_under(resolved, root) for root in roots):
        raise PathNotAllowed(
            f"path {user_path!r} (resolved: {resolved}) is outside the "
            f"allowlisted roots: {[str(r) for r in roots]}"
        )

    if must_exist and not resolved.exists():
        raise PathNotAllowed(f"path does not exist: {resolved}")

    return resolved


def _is_under(child: Path, parent: Path) -> bool:
    """True iff ``child`` is ``parent`` or strictly inside it.

    Both must already be absolute/resolved. Uses string-prefix logic
    rather than ``is_relative_to`` so we can enforce "strict prefix +
    separator" semantics (``/a/bc`` is NOT under ``/a/b``).
    """
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False
