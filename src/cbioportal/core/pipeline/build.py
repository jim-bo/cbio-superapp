"""Per-study DuckDB builder.

Each Cloud Run build job calls `build_study_db()` for exactly one study.  The
function:
  1. Reads study files from the local staging path.
  2. Creates a fresh per-study DuckDB with gene reference + study data.
  3. Uploads the result to `per-study-dbs/{study_id}.duckdb` via StorageBackend.

The same function works locally when `storage` is a `LocalBackend`.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import typer

from cbioportal.core.gcs import StorageBackend
from cbioportal.core.loader import (
    load_study,
    ensure_gene_reference,
    load_study_metadata,
)

_PER_STUDY_PREFIX = "per-study-dbs"


def build_study_db(
    study_id: str,
    staging_path: Path,
    storage: StorageBackend,
    tmp_dir: Path | None = None,
    load_mutations: bool = True,
    load_cna: bool = True,
    load_sv: bool = True,
    load_timeline: bool = True,
    load_expression: bool = True,
) -> None:
    """Build a per-study DuckDB and upload it to storage.

    Does NOT call create_global_views() — union views are built once by the
    merge job after all per-study DBs are assembled into the master.

    Args:
        study_id:     cBioPortal study identifier.
        staging_path: Local path to the directory of extracted study files
                      (i.e. staging_path / study_id must exist).
        storage:      Backend to upload the finished .duckdb file.
        tmp_dir:      Where to write the temp .duckdb (defaults to system temp).
        load_*:       Which data types to include.
    """
    study_path = staging_path / study_id
    if not study_path.exists():
        raise FileNotFoundError(
            f"Study directory not found at {study_path}. Run sync first."
        )

    remote_key = f"{_PER_STUDY_PREFIX}/{study_id}.duckdb"
    base_tmp = tmp_dir or Path(tempfile.gettempdir())
    local_db = base_tmp / f"{study_id}.duckdb"

    # Start fresh — remove any leftover from a previous failed attempt.
    local_db.unlink(missing_ok=True)

    typer.echo(f"[{study_id}] Building per-study DuckDB...")

    conn = duckdb.connect(str(local_db))
    try:
        ensure_gene_reference(conn)
        load_study_metadata(conn, study_path)
        load_study(
            conn,
            study_path,
            load_mutations=load_mutations,
            load_cna=load_cna,
            load_sv=load_sv,
            load_timeline=load_timeline,
            load_expression=load_expression,
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    typer.echo(f"[{study_id}] Uploading to {remote_key}...")
    storage.upload_file(local_db, remote_key)
    local_db.unlink(missing_ok=True)
    typer.echo(f"[{study_id}] Done.")
