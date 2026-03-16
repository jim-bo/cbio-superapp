"""Unit tests for get_clinical_counts() using in-memory DuckDB.

Tests cover: reserved colors, NA handling, patient vs sample level,
frequency calculation, and filter interaction.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_clinical_counts

STUDY = "test_clinical_counts_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sample" (
            SAMPLE_ID VARCHAR,
            PATIENT_ID VARCHAR,
            CANCER_TYPE VARCHAR,
            SAMPLE_TYPE VARCHAR
        )
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_patient" (
            PATIENT_ID VARCHAR,
            GENDER VARCHAR,
            OS_STATUS VARCHAR
        )
    """)
    yield conn
    conn.close()


def _add_sample(db, sample_id, patient_id, cancer_type="BRCA", sample_type="Primary"):
    db.execute(
        f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?, ?)',
        (sample_id, patient_id, cancer_type, sample_type)
    )


def _add_patient(db, patient_id, gender="Male", os_status="LIVING"):
    db.execute(
        f'INSERT INTO "{STUDY}_patient" VALUES (?, ?, ?)',
        (patient_id, gender, os_status)
    )


def test_basic_count(db):
    _add_sample(db, "S1", "P1", cancer_type="BRCA")
    _add_sample(db, "S2", "P2", cancer_type="BRCA")
    _add_sample(db, "S3", "P3", cancer_type="LUAD")
    result = get_clinical_counts(db, STUDY, "CANCER_TYPE", "sample")
    brca_row = next((r for r in result if r["value"] == "BRCA"), None)
    luad_row = next((r for r in result if r["value"] == "LUAD"), None)
    assert brca_row is not None and brca_row["count"] == 2
    assert luad_row is not None and luad_row["count"] == 1


def test_frequency_calculation(db):
    """4 samples, 3 are BRCA → pct = 75.0."""
    for i in range(4):
        _add_sample(db, f"S{i}", f"P{i}", cancer_type="BRCA" if i < 3 else "LUAD")
    result = get_clinical_counts(db, STUDY, "CANCER_TYPE", "sample")
    brca_row = next(r for r in result if r["value"] == "BRCA")
    assert brca_row["pct"] == pytest.approx(75.0, abs=0.1)


def test_null_values_bucketed_as_na(db):
    """NULL attribute values appear as 'NA' in the results."""
    _add_sample(db, "S1", "P1", cancer_type=None)
    _add_sample(db, "S2", "P2", cancer_type="BRCA")
    result = get_clinical_counts(db, STUDY, "CANCER_TYPE", "sample")
    na_row = next((r for r in result if r["value"] == "NA"), None)
    assert na_row is not None and na_row["count"] == 1


def test_patient_level_attribute(db):
    """Patient-level attributes use the _patient table and deduplicate by patient."""
    _add_sample(db, "S1", "P1")
    _add_sample(db, "S2", "P1")  # Same patient, 2 samples
    _add_patient(db, "P1", gender="Male")
    result = get_clinical_counts(db, STUDY, "GENDER", "patient")
    male_row = next((r for r in result if r["value"] == "Male"), None)
    assert male_row is not None and male_row["count"] == 1


def test_reserved_color_male(db):
    """Male should receive the reserved color #2986E2."""
    _add_sample(db, "S1", "P1")
    _add_patient(db, "P1", gender="Male")
    result = get_clinical_counts(db, STUDY, "GENDER", "patient")
    male_row = next(r for r in result if r["value"] == "Male")
    # Reserved color is case-insensitive matched on lowercased value
    assert male_row["color"] == "#2986E2"


def test_reserved_color_deceased(db):
    """DECEASED (status) should receive the reserved color #d95f02."""
    _add_sample(db, "S1", "P1")
    _add_patient(db, "P1", os_status="DECEASED")
    result = get_clinical_counts(db, STUDY, "OS_STATUS", "patient")
    dec_row = next(r for r in result if r["value"] == "DECEASED")
    assert dec_row["color"] == "#d95f02"


def test_results_sorted_by_count_desc(db):
    """Results are sorted from most to least frequent."""
    for i in range(5):
        _add_sample(db, f"S{i}", f"P{i}", cancer_type="BRCA")
    for i in range(5, 7):
        _add_sample(db, f"S{i}", f"P{i}", cancer_type="LUAD")
    result = get_clinical_counts(db, STUDY, "CANCER_TYPE", "sample")
    counts = [r["count"] for r in result]
    assert counts == sorted(counts, reverse=True)


def test_empty_table_returns_empty_list(db):
    result = get_clinical_counts(db, STUDY, "CANCER_TYPE", "sample")
    assert result == []
