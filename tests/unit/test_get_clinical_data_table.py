"""Unit tests for get_clinical_data_table() using in-memory DuckDB.

Tests cover: pagination (offset/limit), sort direction, and search filter.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_clinical_data_table

STUDY = "test_cdt_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sample" (
            SAMPLE_ID VARCHAR,
            PATIENT_ID VARCHAR,
            CANCER_TYPE VARCHAR
        )
    """)
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR,
            patient_attribute BOOLEAN, priority INTEGER,
            PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute(
        "INSERT INTO clinical_attribute_meta VALUES (?,?,?,?,?,?,?)",
        (STUDY, "CANCER_TYPE", "Cancer Type", "", "STRING", False, 1)
    )
    # Populate 5 samples
    for i in range(1, 6):
        conn.execute(
            f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)',
            (f"S{i:02d}", f"P{i}", f"TYPE_{i}")
        )
        conn.execute(f'INSERT INTO "{STUDY}_patient" VALUES (?)', (f"P{i}",))
    yield conn
    conn.close()


def test_total_count(db):
    result = get_clinical_data_table(db, STUDY)
    assert result["total_count"] == 5


def test_default_limit(db):
    """Default limit=20 returns all 5 rows when fewer exist."""
    result = get_clinical_data_table(db, STUDY)
    assert len(result["data"]) == 5


def test_limit_restricts_rows(db):
    result = get_clinical_data_table(db, STUDY, limit=2)
    assert len(result["data"]) == 2


def test_offset_skips_rows(db):
    result_all = get_clinical_data_table(db, STUDY)
    result_offset = get_clinical_data_table(db, STUDY, offset=2)
    # offset=2 should skip the first 2 rows
    assert len(result_offset["data"]) == 3
    # The first row of offset result should match the third row of full result
    assert result_offset["data"][0]["SAMPLE_ID"] == result_all["data"][2]["SAMPLE_ID"]


def test_sort_asc(db):
    result = get_clinical_data_table(db, STUDY, sort_col="SAMPLE_ID", sort_dir="asc")
    sample_ids = [row["SAMPLE_ID"] for row in result["data"]]
    assert sample_ids == sorted(sample_ids)


def test_sort_desc(db):
    result = get_clinical_data_table(db, STUDY, sort_col="SAMPLE_ID", sort_dir="desc")
    sample_ids = [row["SAMPLE_ID"] for row in result["data"]]
    assert sample_ids == sorted(sample_ids, reverse=True)


def test_search_filter(db):
    """Search 'S01' should return only the one matching sample."""
    result = get_clinical_data_table(db, STUDY, search="S01")
    assert result["total_count"] == 1
    assert result["data"][0]["SAMPLE_ID"] == "S01"


def test_search_no_match_returns_empty(db):
    result = get_clinical_data_table(db, STUDY, search="ZZZNOMATCH")
    assert result["total_count"] == 0
    assert result["data"] == []


def test_columns_include_cancer_type(db):
    """Columns from clinical_attribute_meta are included with their IDs."""
    result = get_clinical_data_table(db, STUDY)
    col_ids = [c["id"] for c in result["columns"]]
    assert "CANCER_TYPE" in col_ids


def test_offset_and_limit_combined(db):
    result = get_clinical_data_table(db, STUDY, offset=3, limit=10)
    # 5 total - 3 offset = 2 rows
    assert len(result["data"]) == 2
