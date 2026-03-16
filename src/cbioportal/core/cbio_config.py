"""~/.cbio/config.toml reader/writer."""
from __future__ import annotations

from pathlib import Path

import tomllib
import tomli_w

CONFIG_PATH = Path.home() / ".cbio" / "config.toml"
DEFAULT_PORTAL_URL = "https://www.cbioportal.org"
DEFAULT_CACHE_TTL_DAYS = 180

_DEFAULTS: dict = {
    "portal": {
        "url": DEFAULT_PORTAL_URL,
        "token": "",
    },
    "oncokb": {
        "token": "",
    },
    "cache": {
        "ttl_days": DEFAULT_CACHE_TTL_DAYS,
    },
}


def get_config() -> dict:
    """Read ~/.cbio/config.toml; apply defaults if missing."""
    if not CONFIG_PATH.exists():
        return _deep_copy(_DEFAULTS)

    with CONFIG_PATH.open("rb") as f:
        on_disk = tomllib.load(f)

    return _merge(_DEFAULTS, on_disk)


def set_config(section: str, key: str, value: str) -> None:
    """Write a single key to config; create file/section if needed."""
    config = get_config()
    config.setdefault(section, {})[key] = value
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("wb") as f:
        tomli_w.dump(config, f)


def get_portal_url() -> str:
    """Convenience: return portal.url with default."""
    return get_config().get("portal", {}).get("url", DEFAULT_PORTAL_URL)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deep_copy(d: dict) -> dict:
    """Shallow-safe deep copy for nested dicts of scalars."""
    return {k: dict(v) if isinstance(v, dict) else v for k, v in d.items()}


def _merge(defaults: dict, overrides: dict) -> dict:
    """Merge overrides into defaults (one level of nesting)."""
    result = _deep_copy(defaults)
    for section, values in overrides.items():
        if isinstance(values, dict):
            result.setdefault(section, {}).update(values)
        else:
            result[section] = values
    return result
