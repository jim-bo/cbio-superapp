"""Merge per-study DuckDB files into a master DuckDB.

Two operations:

`merge_all_studies()` — full rebuild of the master from all per-study DBs in
    storage.  Previous master is backed up before replacement.

`inject_study()` — ad-hoc update of a single study in an existing master.
    Downloads the current master, replaces the study's tables, rebuilds views,
    uploads back.  Backup is written before any mutation.
"""
from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import typer

from cbioportal.core.gcs import StorageBackend
from cbioportal.core.loader import ensure_gene_reference, create_global_views
from cbioportal.core.pipeline.catalog import export_catalog

_PER_STUDY_PREFIX = "per-study-dbs"
_MASTER_KEY = "master/cbioportal.duckdb"

# Tables that live in the master but are NOT copied from per-study DBs.
# They either live in gene_reference tables or are built by create_global_views.
_SKIP_TABLE_SUFFIXES = frozenset([
    "genomic_event_derived",
    "profiled_counts",
])

# Global metadata tables — merged via INSERT OR REPLACE, not CREATE TABLE AS.
_METADATA_TABLES = ("studies", "study_data_types")

# Gene-reference table names — skip when copying per-study → master.
_GENE_REFERENCE_TABLES = frozenset([
    "gene_reference",
    "gene_aliases",
    "gene_symbol_updates",
    "gene_panels",
    "gene_panel_definitions",
    "oncotree_cancer_types",
])

# DDL to create metadata tables with proper PRIMARY KEY constraints.
# CREATE TABLE AS SELECT does not copy constraints, so we need to create these
# explicitly before the copy loop so INSERT OR REPLACE works.
_METADATA_TABLE_DDL = {
    "studies": """
        CREATE TABLE IF NOT EXISTS studies (
            study_id VARCHAR PRIMARY KEY,
            type_of_cancer VARCHAR,
            name VARCHAR,
            description VARCHAR,
            short_name VARCHAR,
            public_study BOOLEAN,
            pmid VARCHAR,
            citation VARCHAR,
            groups VARCHAR,
            category VARCHAR
        )
    """,
    "study_data_types": """
        CREATE TABLE IF NOT EXISTS study_data_types (
            study_id VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            PRIMARY KEY (study_id, data_type)
        )
    """,
}


def _backup_master(storage: StorageBackend) -> str | None:
    """Server-side copy of the current master to backups/. Returns the backup key."""
    if not storage.exists(_MASTER_KEY):
        return None
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_key = f"backups/cbioportal.{ts}.duckdb"
    storage.copy(_MASTER_KEY, backup_key)
    typer.echo(f"Backed up master → {backup_key}")
    return backup_key


def _list_per_study_keys(storage: StorageBackend) -> list[str]:
    """Return all per-study DB keys from storage, sorted."""
    keys = storage.list_prefix(f"{_PER_STUDY_PREFIX}/")
    return sorted(k for k in keys if k.endswith(".duckdb"))


def _study_id_from_key(key: str) -> str:
    return Path(key).stem


def _copy_study_tables(
    master_conn: duckdb.DuckDBPyConnection,
    study_id: str,
    per_study_db_path: Path,
    alias: str,
) -> None:
    """ATTACH per-study DB, copy its study tables into master_conn, DETACH."""
    master_conn.execute(f"ATTACH '{per_study_db_path}' AS {alias} (READ_ONLY)")
    try:
        # Query the attached DB's tables via duckdb_tables() — the catalog_name
        # of an ATTACHed database equals the alias given.
        rows = master_conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = 'main'",
            [alias],
        ).fetchall()
        src_tables = [r[0] for r in rows]

        for tname in src_tables:
            # Skip gene reference tables — master gets them from ensure_gene_reference.
            if tname in _GENE_REFERENCE_TABLES:
                continue
            # Skip metadata tables — handled separately via INSERT OR REPLACE.
            if tname in _METADATA_TABLES:
                continue
            # Skip derived/profiled tables (shouldn't exist, but guard anyway).
            if any(tname.endswith(f"_{s}") for s in _SKIP_TABLE_SUFFIXES):
                continue

            master_conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
            master_conn.execute(
                f'CREATE TABLE "{tname}" AS SELECT * FROM {alias}."{tname}"'
            )

        # Merge metadata rows.
        # Metadata tables must already exist in master (created by the caller
        # via _METADATA_TABLE_DDL before the copy loop starts).
        for meta_tbl in _METADATA_TABLES:
            if meta_tbl not in src_tables:
                continue
            master_conn.execute(
                f"INSERT OR REPLACE INTO {meta_tbl} "
                f"SELECT * FROM {alias}.{meta_tbl}"
            )
    finally:
        master_conn.execute(f"DETACH {alias}")


def merge_all_studies(
    storage: StorageBackend,
    study_ids: list[str] | None = None,
    backup: bool = True,
    tmp_dir: Path | None = None,
) -> None:
    """Rebuild the master DuckDB from all per-study DBs in storage.

    Args:
        storage:    Storage backend (GCS or local).
        study_ids:  If given, merge only these studies; otherwise all in storage.
        backup:     Write a timestamped backup of the current master before replacing.
        tmp_dir:    Temp directory for local files (default: system temp).
    """
    base_tmp = tmp_dir or Path(tempfile.gettempdir())
    master_path = base_tmp / "master_new.duckdb"
    master_path.unlink(missing_ok=True)

    # Determine which per-study DBs to merge.
    if study_ids:
        keys = [f"{_PER_STUDY_PREFIX}/{sid}.duckdb" for sid in study_ids]
    else:
        keys = _list_per_study_keys(storage)

    typer.echo(f"Merging {len(keys)} per-study DB(s) into master...")

    conn = duckdb.connect(str(master_path))
    try:
        ensure_gene_reference(conn)

        # Initialize metadata tables with proper PK constraints before the loop
        # so INSERT OR REPLACE works correctly during copy.
        for ddl in _METADATA_TABLE_DDL.values():
            conn.execute(ddl)

        for i, key in enumerate(keys, 1):
            sid = _study_id_from_key(key)
            local_study_db = base_tmp / f"_study_{sid}.duckdb"
            local_study_db.unlink(missing_ok=True)

            typer.echo(f"  [{i}/{len(keys)}] {sid}")
            storage.download_file(key, local_study_db)

            try:
                _copy_study_tables(conn, sid, local_study_db, alias=f"src_{i}")
            finally:
                local_study_db.unlink(missing_ok=True)

        typer.echo("Building global views...")
        create_global_views(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    if backup:
        _backup_master(storage)

    typer.echo("Uploading new master...")
    storage.upload_file(master_path, _MASTER_KEY)

    typer.echo("Exporting catalog DB...")
    export_catalog(master_path, storage, tmp_dir=base_tmp)

    master_path.unlink(missing_ok=True)
    typer.echo("Merge complete.")


def inject_study(
    study_id: str,
    storage: StorageBackend,
    backup: bool = True,
    tmp_dir: Path | None = None,
) -> None:
    """Replace one study's tables in the existing master DuckDB.

    Downloads the current master, drops the study's existing tables, copies in
    the fresh per-study DB tables, rebuilds global views, and re-uploads.

    Args:
        study_id: Study to replace.
        storage:  Storage backend.
        backup:   Write a timestamped backup before modifying.
        tmp_dir:  Temp directory for local files.
    """
    base_tmp = tmp_dir or Path(tempfile.gettempdir())
    per_study_key = f"{_PER_STUDY_PREFIX}/{study_id}.duckdb"

    if not storage.exists(per_study_key):
        raise FileNotFoundError(
            f"Per-study DB not found in storage: {per_study_key}. "
            "Run `cbio beta cloud build --study-id {study_id}` first."
        )

    master_local = base_tmp / "master_work.duckdb"
    master_local.unlink(missing_ok=True)

    typer.echo(f"Downloading master DB...")
    storage.download_file(_MASTER_KEY, master_local)

    if backup:
        _backup_master(storage)

    local_study_db = base_tmp / f"_inject_{study_id}.duckdb"
    local_study_db.unlink(missing_ok=True)

    typer.echo(f"Downloading per-study DB for {study_id}...")
    storage.download_file(per_study_key, local_study_db)

    conn = duckdb.connect(str(master_local))
    try:
        # Drop all existing tables for this study from the master.
        # Use duckdb_tables() to avoid picking up tables from the (not-yet-attached)
        # per-study DB; filter to the main catalog only.
        master_db_name = conn.execute("SELECT current_database()").fetchone()[0]
        existing = conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = 'main' AND table_name LIKE ?",
            [master_db_name, f"{study_id}_%"],
        ).fetchall()
        for (tname,) in existing:
            conn.execute(f'DROP TABLE IF EXISTS "{tname}"')

        _copy_study_tables(conn, study_id, local_study_db, alias="inject_src")
        local_study_db.unlink(missing_ok=True)

        typer.echo("Rebuilding global views...")
        create_global_views(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    typer.echo("Uploading updated master...")
    storage.upload_file(master_local, _MASTER_KEY)
    master_local.unlink(missing_ok=True)
    typer.echo(f"Inject of {study_id} complete.")
