"""Unit tests for the per-study DuckDB build pipeline."""
import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from cbioportal.core.gcs import LocalBackend
from cbioportal.core.pipeline.build import build_study_db

# ensure_gene_reference makes live network calls; load_study_metadata
# calls get_oncotree_root which needs cancer_types table populated by gene ref.
# Both are patched for unit tests.
_PATCH_GENE_REF = patch(
    "cbioportal.core.pipeline.build.ensure_gene_reference",
    return_value=None,
)
_PATCH_METADATA = patch(
    "cbioportal.core.pipeline.build.load_study_metadata",
    return_value=True,
)


def _make_minimal_study(base: Path, study_id: str) -> Path:
    """Write the minimum files needed for load_study to succeed (clinical only)."""
    study_dir = base / study_id
    study_dir.mkdir(parents=True)

    (study_dir / "meta_study.txt").write_text(
        f"cancer_study_identifier: {study_id}\n"
        f"name: Test Study\n"
        f"type_of_cancer: mixed\n"
        f"public_study: false\n"
    )
    (study_dir / "meta_clinical_patient.txt").write_text(
        f"cancer_study_identifier: {study_id}\n"
        f"data_filename: data_clinical_patient.txt\n"
        f"datatype: PATIENT_ATTRIBUTES\n"
    )
    (study_dir / "data_clinical_patient.txt").write_text(
        "#Patient Identifier\tAGE\n"
        "#Patient Identifier\tAge\n"
        "#STRING\tNUMBER\n"
        "#1\t1\n"
        "PATIENT_ID\tAGE\n"
        "P001\t45\n"
        "P002\t60\n"
    )
    (study_dir / "meta_clinical_sample.txt").write_text(
        f"cancer_study_identifier: {study_id}\n"
        f"data_filename: data_clinical_sample.txt\n"
        f"datatype: SAMPLE_ATTRIBUTES\n"
    )
    (study_dir / "data_clinical_sample.txt").write_text(
        "#Sample Identifier\tPatient Identifier\n"
        "#Sample Identifier\tPatient Identifier\n"
        "#STRING\tSTRING\n"
        "#1\t1\n"
        "SAMPLE_ID\tPATIENT_ID\n"
        "S001\tP001\n"
        "S002\tP002\n"
    )
    return study_dir


def test_build_creates_study_tables(tmp_path):
    """build_study_db should produce patient and sample tables in the per-study DB."""
    staging = tmp_path / "staging"
    storage_root = tmp_path / "storage"
    study_id = "test_study_001"

    _make_minimal_study(staging, study_id)
    storage = LocalBackend(storage_root)

    with _PATCH_GENE_REF, _PATCH_METADATA:
        build_study_db(
            study_id=study_id,
            staging_path=staging,
            storage=storage,
            tmp_dir=tmp_path,
            load_mutations=False,
            load_cna=False,
            load_sv=False,
            load_timeline=False,
            load_expression=False,
        )

    # The per-study DB should have been uploaded.
    per_study_local = storage_root / "per-study-dbs" / f"{study_id}.duckdb"
    assert per_study_local.exists(), "Per-study DB was not uploaded to storage"

    conn = duckdb.connect(str(per_study_local), read_only=True)
    tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    conn.close()

    assert f"{study_id}_patient" in tables
    assert f"{study_id}_sample" in tables


def test_build_no_global_views(tmp_path):
    """build_study_db must not create union views in the per-study DB."""
    staging = tmp_path / "staging"
    storage_root = tmp_path / "storage"
    study_id = "test_study_002"

    _make_minimal_study(staging, study_id)
    storage = LocalBackend(storage_root)

    with _PATCH_GENE_REF, _PATCH_METADATA:
        build_study_db(
            study_id=study_id,
            staging_path=staging,
            storage=storage,
            tmp_dir=tmp_path,
            load_mutations=False,
            load_cna=False,
            load_sv=False,
            load_timeline=False,
            load_expression=False,
        )

    per_study_local = storage_root / "per-study-dbs" / f"{study_id}.duckdb"
    conn = duckdb.connect(str(per_study_local), read_only=True)
    views = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='VIEW'"
    ).fetchall()}
    conn.close()

    # No union views like 'mutations', 'clinical_sample', etc. should exist.
    assert "mutations" not in views
    assert "clinical_sample" not in views


def test_build_missing_study_raises(tmp_path):
    """build_study_db should raise FileNotFoundError if the study dir is missing."""
    storage = LocalBackend(tmp_path / "storage")
    with pytest.raises(FileNotFoundError, match="not found"):
        build_study_db(
            study_id="nonexistent_study",
            staging_path=tmp_path / "staging",
            storage=storage,
            tmp_dir=tmp_path,
        )
