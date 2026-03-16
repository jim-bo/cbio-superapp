import duckdb
import pytest
from cbioportal.core.study_view_repository import _build_filter_subquery

STUDY = "test_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(
        f'CREATE TABLE "{STUDY}_sample" '
        f'(SAMPLE_ID VARCHAR, CANCER_TYPE VARCHAR, study_id VARCHAR)'
    )
    conn.execute(
        f"INSERT INTO \"{STUDY}_sample\" VALUES "
        f"('S1','BRCA','{STUDY}'), ('S2','LUAD','{STUDY}'), "
        f"('S3','NA','{STUDY}'), ('S4',NULL,'{STUDY}')"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS clinical_attribute_meta "
        "(study_id VARCHAR, attr_id VARCHAR, patient_attribute BOOLEAN, "
        "display_name VARCHAR, description VARCHAR, datatype VARCHAR, priority INTEGER)"
    )
    conn.execute(
        f"INSERT INTO clinical_attribute_meta VALUES "
        f"('{STUDY}','CANCER_TYPE',false,'Cancer Type','','STRING',3000)"
    )
    yield conn
    conn.close()


def test_no_filter_returns_all(db):
    sql, params = _build_filter_subquery(db, STUDY, "{}")
    rows = db.execute(sql, params).fetchall()
    assert len(rows) == 4


def test_partial_filter_excludes_others(db):
    fj = '{"clinicalDataFilters":[{"attributeId":"CANCER_TYPE","values":[{"value":"BRCA"}]}]}'
    sql, params = _build_filter_subquery(db, STUDY, fj)
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["S1"]


def test_na_filter_matches_both_null_and_string(db):
    """'NA' filter must match both samples with string 'NA' and true NULL."""
    fj = '{"clinicalDataFilters":[{"attributeId":"CANCER_TYPE","values":[{"value":"NA"}]}]}'
    sql, params = _build_filter_subquery(db, STUDY, fj)
    rows = db.execute(sql, params).fetchall()
    sample_ids = sorted(r[0] for r in rows)
    assert sample_ids == ["S3", "S4"], f"Expected S3 (NA string) and S4 (NULL), got {sample_ids}"


def test_all_values_returns_all_samples(db):
    """Selecting all values (BRCA, LUAD, NA) must return every sample."""
    fj = (
        '{"clinicalDataFilters":[{"attributeId":"CANCER_TYPE","values":'
        '[{"value":"BRCA"},{"value":"LUAD"},{"value":"NA"}]}]}'
    )
    sql, params = _build_filter_subquery(db, STUDY, fj)
    rows = db.execute(sql, params).fetchall()
    assert len(rows) == 4, f"Expected all 4 samples, got {len(rows)}"
