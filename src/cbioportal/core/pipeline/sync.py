"""Study sync: mirror cBioPortal.org studies to the staging area.

The staging area is a local directory (CBIO_DOWNLOADS).  In cloud jobs it is a
GCS FUSE mount; locally it is just the downloads/ folder.  Either way, existing
`fetcher.download_study()` handles the actual HTTP fetch — this module only
decides *which* studies to sync.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

import typer

from cbioportal.core.fetcher import list_remote_studies, download_study
from cbioportal.core.gcs import get_staging_path


@dataclass
class SyncResult:
    downloaded: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


def sync_studies(
    force: bool = False,
    study_ids: list[str] | None = None,
) -> SyncResult:
    """Download studies from cBioPortal.org to the staging directory.

    Args:
        force:      Re-download even if the study directory already exists.
        study_ids:  If given, sync only these studies; otherwise sync all.
    """
    result = SyncResult()
    staging = get_staging_path()

    if study_ids:
        all_ids = study_ids
    else:
        typer.echo("Fetching study list from cBioPortal API...")
        remote = list_remote_studies()
        all_ids = [s["studyId"] for s in remote]
        typer.echo(f"Found {len(all_ids)} studies.")

    # Point CBIO_DOWNLOADS at staging so fetcher writes there.
    os.environ["CBIO_DOWNLOADS"] = str(staging)

    for sid in all_ids:
        study_dir = staging / sid
        if study_dir.exists() and not force:
            result.skipped.append(sid)
            continue
        try:
            download_study(sid, force=force)
            result.downloaded.append(sid)
        except Exception as exc:
            typer.echo(f"  ERROR syncing {sid}: {exc}", err=True)
            result.failed.append(sid)

    return result
