"""Unit tests for the merge and inject pipeline operations."""
import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from cbioportal.core.gcs import LocalBackend
from cbioportal.core.pipeline.merge import (
    _MASTER_KEY,
    _PER_STUDY_PREFIX,
    inject_study,
    merge_all_studies,
)

# ensure_gene_reference makes live network calls (HGNC, cBioPortal API).
# create_global_views introspects union views across complex schema.
# Both are patched for unit tests so tests are fast, offline-safe, and schema-minimal.
_PATCH_GENE_REF = patch(
    "cbioportal.core.pipeline.merge.ensure_gene_reference",
    return_value=None,
)
_PATCH_VIEWS = patch(
    "cbioportal.core.pipeline.merge.create_global_views",
    return_value=None,
)


def _make_per_study_db(path: Path, study_id: str, n_samples: int = 2) -> None:
    """Create a minimal per-study DuckDB at `path` with patient and sample tables."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))

    # Gene reference tables (as created by ensure_gene_reference).
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR
        )
    """)
    conn.execute("INSERT INTO gene_reference VALUES (1956, 'EGFR', 'protein-coding')")

    # Studies table.
    conn.execute("""
        CREATE TABLE studies (
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
    """)
    conn.execute(
        "INSERT INTO studies VALUES (?, 'mixed', ?, NULL, NULL, false, NULL, NULL, NULL, 'Other')",
        [study_id, f"Study {study_id}"],
    )

    # study_data_types table.
    conn.execute("""
        CREATE TABLE study_data_types (
            study_id VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            PRIMARY KEY (study_id, data_type)
        )
    """)
    conn.execute("INSERT INTO study_data_types VALUES (?, 'CLINICAL')", [study_id])

    # Patient table.
    conn.execute(f"""
        CREATE TABLE "{study_id}_patient" (
            PATIENT_ID VARCHAR PRIMARY KEY,
            AGE INTEGER
        )
    """)
    for i in range(n_samples):
        conn.execute(f'INSERT INTO "{study_id}_patient" VALUES (?, ?)', [f"P{i:03d}", 40 + i])

    # Sample table.
    conn.execute(f"""
        CREATE TABLE "{study_id}_sample" (
            SAMPLE_ID VARCHAR PRIMARY KEY,
            PATIENT_ID VARCHAR
        )
    """)
    for i in range(n_samples):
        conn.execute(f'INSERT INTO "{study_id}_sample" VALUES (?, ?)', [f"S{i:03d}", f"P{i:03d}"])

    conn.execute("CHECKPOINT")
    conn.close()


def _upload_per_study_db(storage: LocalBackend, study_id: str, db_path: Path) -> None:
    storage.upload_file(db_path, f"{_PER_STUDY_PREFIX}/{study_id}.duckdb")


def test_merge_copies_tables_from_two_studies(tmp_path):
    """merge_all_studies should produce a master with tables from both input studies."""
    storage = LocalBackend(tmp_path / "storage")

    for sid in ("study_a", "study_b"):
        db_path = tmp_path / f"{sid}.duckdb"
        _make_per_study_db(db_path, sid)
        _upload_per_study_db(storage, sid, db_path)

    with _PATCH_GENE_REF, _PATCH_VIEWS:
        merge_all_studies(storage=storage, backup=False, tmp_dir=tmp_path)

    master_local = tmp_path / "master_check.duckdb"
    storage.download_file(_MASTER_KEY, master_local)
    conn = duckdb.connect(str(master_local), read_only=True)

    tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    conn.close()

    assert "study_a_patient" in tables
    assert "study_a_sample" in tables
    assert "study_b_patient" in tables
    assert "study_b_sample" in tables


def test_merge_skips_gene_reference_tables(tmp_path):
    """Merge copy loop must not attempt to copy gene_reference from per-study DBs.

    If it did, the second study's EGFR row would hit a PRIMARY KEY violation.
    Passing without error is the assertion.
    """
    storage = LocalBackend(tmp_path / "storage")

    for sid in ("study_x", "study_y"):
        db_path = tmp_path / f"{sid}.duckdb"
        _make_per_study_db(db_path, sid)
        _upload_per_study_db(storage, sid, db_path)

    # Should not raise even though both per-study DBs have the same EGFR row.
    with _PATCH_GENE_REF, _PATCH_VIEWS:
        merge_all_studies(storage=storage, backup=False, tmp_dir=tmp_path)

    master_local = tmp_path / "master_check2.duckdb"
    storage.download_file(_MASTER_KEY, master_local)
    conn = duckdb.connect(str(master_local), read_only=True)
    tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    conn.close()
    # gene_reference should NOT be in master (ensure_gene_reference was mocked).
    assert "gene_reference" not in tables


def test_merge_creates_backup(tmp_path):
    """merge_all_studies with backup=True should write a backup before replacing."""
    storage = LocalBackend(tmp_path / "storage")

    # Seed a "current" master.
    master_dir = tmp_path / "storage" / "master"
    master_dir.mkdir(parents=True)
    (master_dir / "cbioportal.duckdb").write_text("old-master")

    db_path = tmp_path / "study_z.duckdb"
    _make_per_study_db(db_path, "study_z")
    _upload_per_study_db(storage, "study_z", db_path)

    with _PATCH_GENE_REF, _PATCH_VIEWS:
        merge_all_studies(storage=storage, backup=True, tmp_dir=tmp_path)

    backup_keys = storage.list_prefix("backups/")
    assert len(backup_keys) == 1, "Expected exactly one backup file"


def test_inject_replaces_study_tables(tmp_path):
    """inject_study should replace the study's tables and rebuild views."""
    storage = LocalBackend(tmp_path / "storage")

    # Build a master with study_a (2 samples).
    db_path_a = tmp_path / "study_a.duckdb"
    _make_per_study_db(db_path_a, "study_a", n_samples=2)
    _upload_per_study_db(storage, "study_a", db_path_a)
    with _PATCH_GENE_REF, _PATCH_VIEWS:
        merge_all_studies(storage=storage, backup=False, tmp_dir=tmp_path)

    # Rebuild study_a with 5 samples (simulating a refresh).
    db_path_a_v2 = tmp_path / "study_a_v2.duckdb"
    _make_per_study_db(db_path_a_v2, "study_a", n_samples=5)
    # Upload as the per-study DB (overwrite).
    storage.upload_file(db_path_a_v2, f"{_PER_STUDY_PREFIX}/study_a.duckdb")

    with _PATCH_VIEWS:
        inject_study(study_id="study_a", storage=storage, backup=False, tmp_dir=tmp_path)

    master_local = tmp_path / "master_inject_check.duckdb"
    storage.download_file(_MASTER_KEY, master_local)
    conn = duckdb.connect(str(master_local), read_only=True)
    count = conn.execute('SELECT COUNT(*) FROM "study_a_sample"').fetchone()[0]
    conn.close()

    assert count == 5, f"Expected 5 samples after inject, got {count}"


def test_inject_raises_if_per_study_db_missing(tmp_path):
    """inject_study should raise FileNotFoundError if the per-study DB doesn't exist."""
    storage = LocalBackend(tmp_path / "storage")
    with pytest.raises(FileNotFoundError, match="Per-study DB not found"):
        inject_study(study_id="nonexistent", storage=storage, backup=False, tmp_dir=tmp_path)
