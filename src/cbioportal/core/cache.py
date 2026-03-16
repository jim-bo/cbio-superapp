"""~/.cbio/cache/ with TTL — stubbed."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from cbioportal.core.cbio_config import get_config

CACHE_DIR = Path.home() / ".cbio" / "cache"


def _cache_path(study_id: str, data_type: str) -> Path:
    return CACHE_DIR / study_id / f"{data_type}.json"


def get(study_id: str, data_type: str) -> list | None:
    """Return cached data if exists and within TTL, else None."""
    path = _cache_path(study_id, data_type)
    if not path.exists():
        return None

    with path.open() as f:
        entry = json.load(f)

    fetched_at = datetime.fromisoformat(entry["fetched_at"])
    ttl_days = get_config().get("cache", {}).get("ttl_days", 180)
    age_days = (datetime.now(timezone.utc) - fetched_at).days
    if age_days > ttl_days:
        return None

    return entry["data"]


def put(study_id: str, data_type: str, data: list) -> None:
    """Write data to cache with current timestamp."""
    path = _cache_path(study_id, data_type)
    path.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "study_id": study_id,
        "data_type": data_type,
        "data": data,
    }
    with path.open("w") as f:
        json.dump(entry, f)
