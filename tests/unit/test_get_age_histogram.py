"""Unit tests for get_age_histogram() using in-memory DuckDB."""
import json

import duckdb
import pytest

from cbioportal.core.study_view_repository import get_age_histogram

STUDY = "age_test_study"


def _make_db(patient_extra_cols: str = "", sample_extra_cols: str = "") -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE "{STUDY}_patient" (
            study_id VARCHAR,
            PATIENT_ID VARCHAR
            {', ' + patient_extra_cols if patient_extra_cols else ''}
        )
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sample" (
            study_id VARCHAR,
            SAMPLE_ID VARCHAR,
            PATIENT_ID VARCHAR
            {', ' + sample_extra_cols if sample_extra_cols else ''}
        )
    """)
    return conn


def _add_patient(conn, patient_id, **kwargs):
    cols = ["PATIENT_ID"] + list(kwargs.keys())
    vals = [patient_id] + list(kwargs.values())
    placeholders = ", ".join("?" * len(vals))
    col_str = ", ".join(cols)
    conn.execute(f'INSERT INTO "{STUDY}_patient" ({col_str}) VALUES ({placeholders})', vals)


def _add_sample(conn, sample_id, patient_id, **kwargs):
    cols = ["SAMPLE_ID", "PATIENT_ID"] + list(kwargs.keys())
    vals = [sample_id, patient_id] + list(kwargs.values())
    placeholders = ", ".join("?" * len(vals))
    col_str = ", ".join(cols)
    conn.execute(f'INSERT INTO "{STUDY}_sample" ({col_str}) VALUES ({placeholders})', vals)


def _by_bin(rows, bin_label):
    return next((r["y"] for r in rows if r["x"] == bin_label), 0)


# ---------------------------------------------------------------------------
# Happy path: patient-level age column
# ---------------------------------------------------------------------------

def test_patient_level_age_returns_data():
    conn = _make_db(patient_extra_cols="CURRENT_AGE_DEID VARCHAR")
    _add_patient(conn, "P1", CURRENT_AGE_DEID="55")
    _add_sample(conn, "S1", "P1")
    result = get_age_histogram(conn, STUDY)
    assert result, "Expected histogram data"
    total = sum(r["y"] for r in result)
    assert total == 1


def test_sample_level_age_returns_data():
    conn = _make_db(sample_extra_cols="AGE VARCHAR")
    _add_patient(conn, "P1")
    _add_sample(conn, "S1", "P1", AGE="62")
    result = get_age_histogram(conn, STUDY)
    assert result, "Expected histogram data"
    assert _by_bin(result, "60-65") == 1


# ---------------------------------------------------------------------------
# Bin boundary correctness
# ---------------------------------------------------------------------------

def test_bin_boundaries():
    conn = _make_db(sample_extra_cols="AGE VARCHAR")
    cases = [
        ("S1", "P1", "30"),   # <=35
        ("S2", "P2", "42"),   # 40-45
        ("S3", "P3", "85"),   # 80-85
        ("S4", "P4", "90"),   # >85
    ]
    for sample_id, patient_id, age in cases:
        _add_patient(conn, patient_id)
        _add_sample(conn, sample_id, patient_id, AGE=age)

    result = get_age_histogram(conn, STUDY)
    assert _by_bin(result, "<=35") == 1
    assert _by_bin(result, "40-45") == 1
    assert _by_bin(result, "80-85") == 1
    assert _by_bin(result, ">85") == 1


def test_bin_boundary_exact_35():
    """Value of exactly 35 goes in '<=35' bin."""
    conn = _make_db(sample_extra_cols="AGE VARCHAR")
    _add_patient(conn, "P1")
    _add_sample(conn, "S1", "P1", AGE="35")
    result = get_age_histogram(conn, STUDY)
    assert _by_bin(result, "<=35") == 1


# ---------------------------------------------------------------------------
# Column priority
# ---------------------------------------------------------------------------

def test_current_age_deid_takes_priority_over_age():
    """CURRENT_AGE_DEID (patient) wins over AGE (sample) when both exist."""
    conn = _make_db(
        patient_extra_cols="CURRENT_AGE_DEID VARCHAR",
        sample_extra_cols="AGE VARCHAR",
    )
    # Patient age 42 → 40-45; sample age 72 → 70-75
    _add_patient(conn, "P1", CURRENT_AGE_DEID="42")
    _add_sample(conn, "S1", "P1", AGE="72")

    result = get_age_histogram(conn, STUDY)
    # Should use CURRENT_AGE_DEID (patient), so 40-45 bin
    assert _by_bin(result, "40-45") == 1
    assert _by_bin(result, "70-75") == 0


# ---------------------------------------------------------------------------
# No recognized age column
# ---------------------------------------------------------------------------

def test_no_age_column_returns_empty():
    conn = _make_db()  # no age column at all
    _add_patient(conn, "P1")
    _add_sample(conn, "S1", "P1")
    result = get_age_histogram(conn, STUDY)
    assert result == []


# ---------------------------------------------------------------------------
# Non-numeric / NA values excluded
# ---------------------------------------------------------------------------

def test_non_numeric_age_excluded():
    conn = _make_db(patient_extra_cols="CURRENT_AGE_DEID VARCHAR")
    _add_patient(conn, "P1", CURRENT_AGE_DEID="NA")
    _add_patient(conn, "P2", CURRENT_AGE_DEID="unknown")
    _add_patient(conn, "P3", CURRENT_AGE_DEID="55")
    _add_sample(conn, "S1", "P1")
    _add_sample(conn, "S2", "P2")
    _add_sample(conn, "S3", "P3")
    result = get_age_histogram(conn, STUDY)
    numeric_total = sum(r["y"] for r in result if r["x"] != "NA")
    na_total = sum(r["y"] for r in result if r["x"] == "NA")
    assert numeric_total == 1  # only P3 in numeric bins
    assert na_total == 2       # P1 and P2 in NA bin
    assert na_total + numeric_total == 3


# ---------------------------------------------------------------------------
# Empty table
# ---------------------------------------------------------------------------

def test_empty_patient_table_returns_empty():
    conn = _make_db(patient_extra_cols="CURRENT_AGE_DEID VARCHAR")
    # no rows inserted
    result = get_age_histogram(conn, STUDY)
    assert result == []


# ---------------------------------------------------------------------------
# Filter respected
# ---------------------------------------------------------------------------

def test_filter_restricts_patients():
    """Clinical filter on sample attribute narrows which patients are counted."""
    conn = _make_db(
        patient_extra_cols="CURRENT_AGE_DEID VARCHAR",
        sample_extra_cols="CANCER_TYPE VARCHAR",
    )
    _add_patient(conn, "P1", CURRENT_AGE_DEID="42")
    _add_patient(conn, "P2", CURRENT_AGE_DEID="72")
    _add_sample(conn, "S1", "P1", CANCER_TYPE="Breast")
    _add_sample(conn, "S2", "P2", CANCER_TYPE="Lung")

    # Filter to only include Breast samples (S1 / P1)
    filter_json = json.dumps({
        "clinicalDataFilters": [
            {"attributeId": "CANCER_TYPE", "values": [{"value": "Breast"}]}
        ]
    })
    result = get_age_histogram(conn, STUDY, filter_json=filter_json)

    total = sum(r["y"] for r in result)
    assert total == 1
    assert _by_bin(result, "40-45") == 1
    assert _by_bin(result, "70-75") == 0


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------

def test_result_has_x_and_y_keys():
    conn = _make_db(sample_extra_cols="AGE VARCHAR")
    _add_patient(conn, "P1")
    _add_sample(conn, "S1", "P1", AGE="50")
    result = get_age_histogram(conn, STUDY)
    assert result
    row = result[0]
    assert "x" in row and "y" in row
