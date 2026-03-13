"""Unit tests for get_cna_genes() using in-memory DuckDB.

AMP = cna_value 2, HOMDEL = cna_value -2.
Neutral values (±1) are excluded.
CDKN2A isoforms (CDKN2Ap14ARF, CDKN2Ap16INK4A) are filtered out.
Freq = n_samples / n_profiled * 100, rounded to 1 decimal.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_cna_genes

STUDY = "test_cna_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f"""
        CREATE TABLE "{STUDY}_cna" (
            sample_id VARCHAR,
            hugo_symbol VARCHAR,
            cna_value INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE studies (
            study_id VARCHAR,
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
    conn.execute("INSERT INTO studies (study_id, name) VALUES (?, ?)", (STUDY, "Test CNA Study"))
    # No gene_panel table → _get_panel_availability returns False → simple path
    yield conn
    conn.close()


def _add_sample(db, sample_id):
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (sample_id, f"P_{sample_id}"))


def _add_cna(db, sample_id, hugo_symbol, cna_value):
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?)', (sample_id, hugo_symbol, cna_value))


def _by_gene_type(rows, gene, cna_type):
    return next((r for r in rows if r["gene"] == gene and r["cna_type"] == cna_type), None)


def test_amp_counted(db):
    _add_sample(db, "S1")
    _add_cna(db, "S1", "ERBB2", 2)
    result = get_cna_genes(db, STUDY)
    row = _by_gene_type(result, "ERBB2", "AMP")
    assert row is not None
    assert row["n_samples"] == 1


def test_homdel_counted(db):
    _add_sample(db, "S1")
    _add_cna(db, "S1", "CDKN2A", -2)
    result = get_cna_genes(db, STUDY)
    row = _by_gene_type(result, "CDKN2A", "HOMDEL")
    assert row is not None
    assert row["n_samples"] == 1


def test_neutral_values_excluded(db):
    _add_sample(db, "S1")
    _add_cna(db, "S1", "MYC", 1)
    _add_cna(db, "S1", "RB1", -1)
    result = get_cna_genes(db, STUDY)
    assert _by_gene_type(result, "MYC", "AMP") is None
    assert _by_gene_type(result, "RB1", "HOMDEL") is None
    assert len(result) == 0


def test_isoforms_excluded(db):
    _add_sample(db, "S1")
    _add_cna(db, "S1", "CDKN2Ap14ARF", -2)
    _add_cna(db, "S1", "CDKN2Ap16INK4A", -2)
    result = get_cna_genes(db, STUDY)
    assert len(result) == 0


def test_freq_calculation(db):
    """10 samples in study, 4 have ERBB2 AMP → freq = 40.0."""
    for i in range(10):
        _add_sample(db, f"S{i}")
    for i in range(4):
        _add_cna(db, f"S{i}", "ERBB2", 2)
    result = get_cna_genes(db, STUDY)
    row = _by_gene_type(result, "ERBB2", "AMP")
    assert row is not None
    assert row["freq"] == 40.0


def test_amp_and_homdel_separate_rows(db):
    """AMP and HOMDEL for the same gene appear as separate rows."""
    _add_sample(db, "S1")
    _add_sample(db, "S2")
    _add_cna(db, "S1", "MYC", 2)
    _add_cna(db, "S2", "MYC", -2)
    result = get_cna_genes(db, STUDY)
    assert _by_gene_type(result, "MYC", "AMP") is not None
    assert _by_gene_type(result, "MYC", "HOMDEL") is not None


def test_empty_table_returns_empty_list(db):
    result = get_cna_genes(db, STUDY)
    assert result == []


def test_missing_table_returns_empty_list():
    """If the _cna table doesn't exist, get_cna_genes returns [] without raising."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute("""
        CREATE TABLE studies (study_id VARCHAR, name VARCHAR)
    """)
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    result = get_cna_genes(conn, STUDY)
    assert result == []
    conn.close()
