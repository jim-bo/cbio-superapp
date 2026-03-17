"""Sync cBioPortal studies and clinical data to the local cache DB."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Callable


async def sync_all(progress_cb: Callable[[str], None]) -> dict:
    """Fetch all studies + clinical data → cache DB. Returns summary stats."""
    return await asyncio.to_thread(_sync_all_sync, progress_cb)


def _is_clinical_fresh(conn, study_id: str, ttl_days: int) -> bool:
    """Return True if the study's clinical data is cached within TTL."""
    res = conn.execute(
        "SELECT fetched_at FROM cache_manifest WHERE study_id = ? AND data_type = ?",
        [study_id, "clinical"],
    ).fetchone()
    if not res:
        return False
    fetched_at = res[0]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return (now - fetched_at).days <= ttl_days


def _sync_all_sync(progress_cb: Callable[[str], None]) -> dict:
    from cbioportal.core.api.client import CbioPortalClient
    from cbioportal.core.cache import (
        get_cache_connection,
        upsert_studies,
        upsert_clinical_attributes,
        upsert_clinical_data,
    )
    from cbioportal.core.cbio_config import get_config

    progress_cb("Fetching study list from cBioPortal...")
    with CbioPortalClient() as client:
        studies = client.fetch_all_studies()
    progress_cb(f"Fetched {len(studies)} studies.")

    ttl_days = int(get_config().get("cache", {}).get("ttl_days", 180))
    conn = get_cache_connection(read_only=False)
    try:
        upsert_studies(conn, [s.model_dump() for s in studies])

        clinical_rows_total = 0
        skipped = 0

        for i, study in enumerate(studies):
            if _is_clinical_fresh(conn, study.studyId, ttl_days):
                skipped += 1
                continue

            progress_cb(f"[{i + 1}/{len(studies)}] {study.studyId} — syncing clinical data…")

            with CbioPortalClient() as client:
                attrs = client.get_clinical_attributes(study.studyId)
                rows = client.get_clinical_data(study.studyId)

            upsert_clinical_attributes(conn, study.studyId, attrs)
            upsert_clinical_data(conn, study.studyId, rows)

            now = datetime.now(timezone.utc).replace(tzinfo=None)
            conn.execute("""
                INSERT INTO cache_manifest (study_id, data_type, molecular_profile_id, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (study_id, data_type) DO UPDATE SET
                    molecular_profile_id = excluded.molecular_profile_id,
                    fetched_at = excluded.fetched_at
            """, [study.studyId, "clinical", "", now])

            clinical_rows_total += len(rows)

        if skipped:
            progress_cb(f"Skipped {skipped} studies (cache fresh).")

    finally:
        conn.close()

    return {"studies": len(studies), "clinical_rows": clinical_rows_total}
