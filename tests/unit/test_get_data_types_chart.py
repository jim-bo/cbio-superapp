"""Unit tests for get_data_types_chart() using in-memory DuckDB.

Data types are taken from study_data_types table.
Only molecular types (mutation/cna/sv/mrna/protein/methylation) are shown.
Clinical is excluded even if present in study_data_types.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_data_types_chart

STUDY = "test_dt_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute("""
        CREATE TABLE study_data_types (
            study_id VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            PRIMARY KEY (study_id, data_type)
        )
    """)
    # Add 3 samples
    for i in range(1, 4):
        conn.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (f"S{i}", f"P{i}"))
    yield conn
    conn.close()


def _add_data_type(db, data_type):
    db.execute("INSERT INTO study_data_types VALUES (?, ?)", (STUDY, data_type))


def test_mutation_appears(db):
    _add_data_type(db, "mutation")
    result = get_data_types_chart(db, STUDY, None)
    names = [r["display_name"] for r in result]
    # Mutations should appear under their display name
    assert any("Mutation" in n for n in names)


def test_cna_appears(db):
    _add_data_type(db, "cna")
    result = get_data_types_chart(db, STUDY, None)
    assert len(result) == 1
    # CNA display name contains "copy" or "alterations" (exact text from meta.py)
    assert any(
        "copy" in r["display_name"].lower() or "alterations" in r["display_name"].lower()
        for r in result
    )


def test_clinical_excluded(db):
    """Clinical data type should not appear in the chart."""
    _add_data_type(db, "clinical")
    result = get_data_types_chart(db, STUDY, None)
    assert result == []


def test_multiple_data_types(db):
    _add_data_type(db, "mutation")
    _add_data_type(db, "cna")
    result = get_data_types_chart(db, STUDY, None)
    assert len(result) == 2


def test_freq_uses_total_samples(db):
    """When no gene panel table exists, count = total filtered samples."""
    _add_data_type(db, "mutation")
    result = get_data_types_chart(db, STUDY, None)
    mut_row = result[0]
    # 3 samples, no gene panel → count = 3
    assert mut_row["count"] == 3
    assert mut_row["freq"] == pytest.approx(100.0, abs=0.1)


def test_empty_study_data_types(db):
    result = get_data_types_chart(db, STUDY, None)
    assert result == []


def test_required_keys_present(db):
    _add_data_type(db, "mutation")
    result = get_data_types_chart(db, STUDY, None)
    for key in ("display_name", "count", "freq"):
        assert key in result[0], f"Missing key: {key}"
