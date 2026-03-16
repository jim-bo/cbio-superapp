"""Unit tests for get_sv_genes() using in-memory DuckDB.

SV frequency = n_samples / n_total_samples * 100 (no gene panel in these tests).
get_sv_genes uses Site1_Hugo_Symbol or Gene1 column for gene names.
Empty gene values are excluded.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_sv_genes

STUDY = "test_sv_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sv" (
            Sample_Id VARCHAR,
            Site1_Hugo_Symbol VARCHAR,
            Site2_Hugo_Symbol VARCHAR,
            SV_Status VARCHAR
        )
    """)
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test SV Study"))
    yield conn
    conn.close()


def _add_sample(db, sample_id):
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (sample_id, f"P_{sample_id}"))


def _add_sv(db, sample_id, gene):
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?, NULL, NULL)', (sample_id, gene))


def test_sv_gene_counted(db):
    _add_sample(db, "S1")
    _add_sv(db, "S1", "ALK")
    result = get_sv_genes(db, STUDY)
    row = next((r for r in result if r["gene"] == "ALK"), None)
    assert row is not None
    assert row["n_samples"] == 1


def test_same_gene_multiple_sv_events_one_sample(db):
    """Multiple SV events in the same gene for the same sample count as n_sv > 1 but n_samples = 1."""
    _add_sample(db, "S1")
    _add_sv(db, "S1", "ALK")
    _add_sv(db, "S1", "ALK")
    result = get_sv_genes(db, STUDY)
    row = next((r for r in result if r["gene"] == "ALK"), None)
    assert row is not None
    assert row["n_samples"] == 1
    assert row["n_sv"] == 2


def test_two_samples_both_counted(db):
    _add_sample(db, "S1")
    _add_sample(db, "S2")
    _add_sv(db, "S1", "RET")
    _add_sv(db, "S2", "RET")
    result = get_sv_genes(db, STUDY)
    row = next((r for r in result if r["gene"] == "RET"), None)
    assert row is not None
    assert row["n_samples"] == 2


def test_freq_calculation(db):
    """5 samples, 2 have ALK SV → freq = 40.0."""
    for i in range(5):
        _add_sample(db, f"S{i}")
    _add_sv(db, "S0", "ALK")
    _add_sv(db, "S1", "ALK")
    result = get_sv_genes(db, STUDY)
    row = next((r for r in result if r["gene"] == "ALK"), None)
    assert row is not None
    assert row["freq"] == 40.0


def test_empty_gene_excluded(db):
    """Rows where Site1_Hugo_Symbol is empty string or NULL are excluded."""
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, NULL, NULL, NULL)', ("S1",))
    db.execute(f"INSERT INTO \"{STUDY}_sv\" VALUES (?, '', NULL, NULL)", ("S1",))
    result = get_sv_genes(db, STUDY)
    assert result == []


def test_empty_table_returns_empty_list(db):
    result = get_sv_genes(db, STUDY)
    assert result == []


def test_missing_table_returns_empty_list():
    """If the _sv table doesn't exist, get_sv_genes returns [] without raising."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    result = get_sv_genes(conn, STUDY)
    assert result == []
    conn.close()


def test_no_data_for_sv_column_returns_empty(db):
    """If the SV table has no recognized gene column, returns empty list."""
    # Create a table with an unrecognized gene column name
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sv" (
            Sample_Id VARCHAR,
            UnknownGeneCol VARCHAR
        )
    """)
    conn.execute("CREATE TABLE studies (study_id VARCHAR, name VARCHAR)")
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test"))
    result = get_sv_genes(conn, STUDY)
    assert result == []
    conn.close()
