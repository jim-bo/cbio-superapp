"""Export a lightweight catalog.duckdb from the master DB.

catalog.duckdb is a small (~1-5 MB) database containing only study metadata and
pre-aggregated sample counts. It powers the homepage on cold start without needing
the full master DB to be warmed in the OS page cache.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import typer

from cbioportal.core.gcs import StorageBackend

CATALOG_KEY = "master/catalog.duckdb"

# Tables copied verbatim from master into the catalog.
_CATALOG_COPY_TABLES = (
    "studies",
    "study_data_types",
    "gene_reference",
    "gene_alias",
    "gene_symbol_updates",
)


def export_catalog(
    master_path: Path,
    storage: StorageBackend,
    tmp_dir: Path | None = None,
) -> None:
    """Build catalog.duckdb from the master DB and upload it to storage.

    Opens the master read-only via ATTACH, copies metadata tables, and computes
    pre-aggregated sample counts so the homepage never touches the heavy
    per-study tables or the clinical_sample union view at request time.

    Args:
        master_path: Local path to the fully-built master DuckDB.
        storage:     Storage backend (GCS or local).
        tmp_dir:     Temp directory for the output file (default: system temp).
    """
    base_tmp = tmp_dir or Path(tempfile.gettempdir())
    catalog_path = base_tmp / "catalog_new.duckdb"
    catalog_path.unlink(missing_ok=True)

    # Open master directly to query sample counts (ATTACH does not expose views).
    # Try the clinical_sample union view first; fall back to per-study tables when
    # the view is absent (e.g. in unit tests where create_global_views is patched).
    master_conn = duckdb.connect(str(master_path), read_only=True)
    try:
        try:
            sample_counts = master_conn.execute(
                "SELECT study_id, COUNT(DISTINCT SAMPLE_ID) AS sample_count "
                "FROM clinical_sample GROUP BY study_id"
            ).fetchall()
        except Exception:
            # Fall back: iterate per-study sample tables from the studies table.
            study_ids = [r[0] for r in master_conn.execute("SELECT study_id FROM studies").fetchall()]
            sample_counts = []
            for sid in study_ids:
                try:
                    n = master_conn.execute(
                        f'SELECT COUNT(DISTINCT SAMPLE_ID) FROM "{sid}_sample"'
                    ).fetchone()[0]
                    sample_counts.append((sid, n))
                except Exception:
                    sample_counts.append((sid, 0))
    finally:
        master_conn.close()

    cat_conn = duckdb.connect(str(catalog_path))
    try:
        cat_conn.execute(f"ATTACH '{master_path}' AS src (READ_ONLY)")

        for tname in _CATALOG_COPY_TABLES:
            try:
                cat_conn.execute(
                    f'CREATE TABLE "{tname}" AS SELECT * FROM src."{tname}"'
                )
            except Exception:
                pass  # Table absent in master (e.g. gene_alias for a minimal DB)

        cat_conn.execute("DETACH src")

        # Store pre-aggregated sample counts so homepage queries never touch
        # the heavy per-study sample tables or the clinical_sample UNION ALL view.
        cat_conn.execute(
            "CREATE TABLE catalog_sample_counts (study_id VARCHAR PRIMARY KEY, sample_count BIGINT)"
        )
        cat_conn.executemany(
            "INSERT INTO catalog_sample_counts VALUES (?, ?)", sample_counts
        )

        cat_conn.execute("CHECKPOINT")
    finally:
        cat_conn.close()

    size_kb = catalog_path.stat().st_size // 1024
    typer.echo(f"Uploading catalog DB ({size_kb} KB) → {CATALOG_KEY}")
    storage.upload_file(catalog_path, CATALOG_KEY)
    catalog_path.unlink(missing_ok=True)
    typer.echo("Catalog export complete.")
