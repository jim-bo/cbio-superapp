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


@pytest.fixture
def db_with_sv():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, CANCER_TYPE VARCHAR)')
    conn.executemany(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)', [
        ("S1", "P1", "BRCA"),
        ("S2", "P2", "LUAD"),
        ("S3", "P3", "BRCA"),
    ])
    conn.execute(f'CREATE TABLE "{STUDY}_sv" (Sample_Id VARCHAR, Site1_Hugo_Symbol VARCHAR)')
    conn.executemany(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?)', [
        ("S1", "ERBB2"),
        ("S3", "ALK"),
    ])
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


@pytest.fixture
def db_with_cna():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.executemany(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', [
        ("S1", "P1"),
        ("S2", "P2"),
        ("S3", "P3"),
    ])
    conn.execute(f'CREATE TABLE "{STUDY}_cna" (sample_id VARCHAR, hugo_symbol VARCHAR, cna_value FLOAT)')
    conn.executemany(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?)', [
        ("S1", "ERBB2", 2),
        ("S2", "MYC", 2),
        ("S3", "ERBB2", -2),
    ])
    conn.execute(
        "CREATE TABLE IF NOT EXISTS clinical_attribute_meta "
        "(study_id VARCHAR, attr_id VARCHAR, patient_attribute BOOLEAN, "
        "display_name VARCHAR, description VARCHAR, datatype VARCHAR, priority INTEGER)"
    )
    yield conn
    conn.close()


def test_sv_filter_returns_correct_samples(db_with_sv):
    """svFilter on ERBB2 should return only S1 (the sample with ERBB2 SV)."""
    fj = '{"svFilter": {"genes": ["ERBB2"]}}'
    sql, params = _build_filter_subquery(db_with_sv, STUDY, fj)
    rows = db_with_sv.execute(sql, params).fetchall()
    sample_ids = sorted(r[0] for r in rows)
    assert sample_ids == ["S1"], f"Expected ['S1'], got {sample_ids}"


def test_cna_filter_returns_correct_samples(db_with_cna):
    """cnaFilter on ERBB2 should return S1 and S3 (both have ERBB2 CNA)."""
    fj = '{"cnaFilter": {"genes": ["ERBB2"]}}'
    sql, params = _build_filter_subquery(db_with_cna, STUDY, fj)
    rows = db_with_cna.execute(sql, params).fetchall()
    sample_ids = sorted(r[0] for r in rows)
    assert sample_ids == ["S1", "S3"], f"Expected ['S1', 'S3'], got {sample_ids}"


def test_sv_filter_intersects_with_clinical(db_with_sv):
    """svFilter + clinicalDataFilter should return only samples matching both."""
    fj = (
        '{"clinicalDataFilters":[{"attributeId":"CANCER_TYPE","values":[{"value":"BRCA"}]}],'
        '"svFilter":{"genes":["ERBB2"]}}'
    )
    sql, params = _build_filter_subquery(db_with_sv, STUDY, fj)
    rows = db_with_sv.execute(sql, params).fetchall()
    sample_ids = sorted(r[0] for r in rows)
    # S1 has CANCER_TYPE=BRCA and SV in ERBB2; S3 is BRCA but ALK not ERBB2
    assert sample_ids == ["S1"], f"Expected ['S1'], got {sample_ids}"


def test_cna_filter_empty_genes_returns_all(db_with_cna):
    """Empty cnaFilter.genes should return all samples (no filtering)."""
    fj = '{"cnaFilter": {"genes": []}}'
    sql, params = _build_filter_subquery(db_with_cna, STUDY, fj)
    rows = db_with_cna.execute(sql, params).fetchall()
    assert len(rows) == 3, f"Expected 3 samples, got {len(rows)}"
