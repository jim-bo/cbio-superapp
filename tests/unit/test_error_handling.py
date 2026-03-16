"""Error handling tests: malformed filter JSON, missing tables, NULL columns.

These tests verify that repository functions handle unexpected inputs gracefully
rather than propagating DuckDB exceptions to callers.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import (
    get_clinical_counts,
    get_mutated_genes,
    get_cna_genes,
    get_sv_genes,
    get_km_data,
    get_data_types_chart,
)
from cbioportal.web.schemas import DashboardFilters

STUDY = "test_err_study"


@pytest.fixture
def minimal_db():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    conn.execute("""
        CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR,
        PRIMARY KEY (study_id, data_type))
    """)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Malformed filter JSON — repository functions should not crash
# ---------------------------------------------------------------------------

def test_malformed_json_get_mutated_genes(minimal_db):
    """get_mutated_genes should not raise on malformed filter JSON."""
    result = get_mutated_genes(minimal_db, STUDY, "NOT_VALID_JSON{{{")
    assert result == []


def test_malformed_json_get_cna_genes(minimal_db):
    result = get_cna_genes(minimal_db, STUDY, "NOT_VALID_JSON{{{")
    assert result == []


def test_malformed_json_get_clinical_counts(minimal_db):
    # Adds a column for the query to work against
    minimal_db.execute(
        f'ALTER TABLE "{STUDY}_sample" ADD COLUMN CANCER_TYPE VARCHAR'
    )
    result = get_clinical_counts(minimal_db, STUDY, "CANCER_TYPE", "sample", "{bad json}")
    # Should return empty or fallback, not raise
    assert isinstance(result, list)


def test_malformed_json_get_km_data(minimal_db):
    minimal_db.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR,
            patient_attribute BOOLEAN, priority INTEGER,
            PRIMARY KEY (study_id, attr_id)
        )
    """)
    minimal_db.execute(
        "INSERT INTO clinical_attribute_meta VALUES (?,?,?,?,?,?,?)",
        (STUDY, "OS_MONTHS", "OS", "", "NUMBER", True, 1)
    )
    minimal_db.execute(
        "INSERT INTO clinical_attribute_meta VALUES (?,?,?,?,?,?,?)",
        (STUDY, "OS_STATUS", "OS Status", "", "STRING", True, 1)
    )
    minimal_db.execute(f'ALTER TABLE "{STUDY}_patient" ADD COLUMN OS_MONTHS DOUBLE')
    minimal_db.execute(f'ALTER TABLE "{STUDY}_patient" ADD COLUMN OS_STATUS VARCHAR')
    result = get_km_data(minimal_db, STUDY, "{bad json}")
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Missing tables — functions return empty list without raising
# ---------------------------------------------------------------------------

def test_missing_mutations_table_returns_empty():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    result = get_mutated_genes(conn, STUDY)
    assert result == []
    conn.close()


def test_missing_cna_table_returns_empty():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    result = get_cna_genes(conn, STUDY)
    assert result == []
    conn.close()


def test_missing_sv_table_returns_empty():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    result = get_sv_genes(conn, STUDY)
    assert result == []
    conn.close()


# ---------------------------------------------------------------------------
# Pydantic schema: malformed DashboardFilters
# ---------------------------------------------------------------------------

def test_dashboard_filters_valid_empty():
    f = DashboardFilters.model_validate_json('{}')
    assert f.clinicalDataFilters == []
    assert f.mutationFilter.genes == []


def test_dashboard_filters_valid_full():
    json = '''{
        "clinicalDataFilters": [{"attributeId": "GENDER", "values": [{"value": "Male"}]}],
        "mutationFilter": {"genes": ["TP53"]},
        "svFilter": {"genes": ["ALK"]}
    }'''
    f = DashboardFilters.model_validate_json(json)
    assert f.clinicalDataFilters[0].attributeId == "GENDER"
    assert f.mutationFilter.genes == ["TP53"]
    assert f.svFilter.genes == ["ALK"]


def test_dashboard_filters_invalid_json_raises():
    from pydantic import ValidationError
    with pytest.raises((ValidationError, ValueError)):
        DashboardFilters.model_validate_json("NOT_JSON")


def test_dashboard_filters_wrong_type_raises():
    """clinicalDataFilters must be a list, not a string."""
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        DashboardFilters.model_validate_json('{"clinicalDataFilters": "bad"}')
