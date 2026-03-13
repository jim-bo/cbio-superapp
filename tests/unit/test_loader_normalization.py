"""Unit tests for normalize_hugo_symbols() and VC filtering logic in loader.py."""
import duckdb
import pytest
from cbioportal.core.loader import normalize_hugo_symbols, _EXCLUDED_VCS
from tests.unit.conftest import STUDY


# ---------------------------------------------------------------------------
# Hugo symbol normalization tests
# ---------------------------------------------------------------------------

def test_mll2_normalized_to_kmt2d_via_entrez(db_with_gene_ref):
    conn = db_with_gene_ref
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "MLL2", "8085", "Missense_Mutation", "SOMATIC"),
    )
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    assert row[0] == "KMT2D"


def test_mll3_normalized_to_kmt2c_via_entrez(db_with_gene_ref):
    conn = db_with_gene_ref
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "MLL3", "58508", "Missense_Mutation", "SOMATIC"),
    )
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    assert row[0] == "KMT2C"


def test_mll_normalized_to_kmt2a_via_symbol_map(db_with_gene_ref):
    """MLL with NULL/missing Entrez should fall back to gene_symbol_updates."""
    conn = db_with_gene_ref
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "MLL", None, "Missense_Mutation", "SOMATIC"),
    )
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    assert row[0] == "KMT2A"


def test_mll4_normalized_to_kmt2b_via_symbol_map(db_with_gene_ref):
    """MLL4 with NULL Entrez should fall back to gene_symbol_updates."""
    conn = db_with_gene_ref
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "MLL4", None, "Missense_Mutation", "SOMATIC"),
    )
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    assert row[0] == "KMT2B"


def test_canonical_symbol_unchanged(db_with_gene_ref):
    """KMT2D already canonical — should not be modified."""
    conn = db_with_gene_ref
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KMT2D", "8085", "Missense_Mutation", "SOMATIC"),
    )
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    assert row[0] == "KMT2D"


def test_unknown_symbol_unchanged(db_with_gene_ref):
    """KRAS is in gene_reference but already canonical — should be unchanged."""
    conn = db_with_gene_ref
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "KRAS", "3845", "Missense_Mutation", "SOMATIC"),
    )
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    assert row[0] == "KRAS"


def test_normalization_skipped_without_gene_reference():
    """If gene_reference table is absent, normalize_hugo_symbols returns silently."""
    conn = duckdb.connect(":memory:")
    conn.execute(f"""CREATE TABLE "{STUDY}_mutations" (
        SAMPLE_ID VARCHAR, Hugo_Symbol VARCHAR, Entrez_Gene_Id VARCHAR,
        Variant_Classification VARCHAR, Mutation_Status VARCHAR
    )""")
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)',
        ("S1", "MLL2", "8085", "Missense_Mutation", "SOMATIC"),
    )
    # Should not raise
    normalize_hugo_symbols(conn, STUDY)
    row = conn.execute(f'SELECT Hugo_Symbol FROM "{STUDY}_mutations"').fetchone()
    # Symbol unchanged because no reference tables
    assert row[0] == "MLL2"
    conn.close()


# ---------------------------------------------------------------------------
# Variant Classification filter tests
# ---------------------------------------------------------------------------

def _apply_vc_filter(conn, study_id: str, mutation_file_path: str):
    """Helper that applies the same WHERE clause as load_study() to an in-memory table."""
    _vc_list = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in sorted(_EXCLUDED_VCS))
    return conn.execute(f"""
        SELECT Hugo_Symbol, Variant_Classification
        FROM "{study_id}_mutations"
        WHERE COALESCE(Variant_Classification, '') NOT IN ({_vc_list})
           OR (Hugo_Symbol = 'TERT' AND Variant_Classification = '5''Flank')
    """).fetchall()


@pytest.fixture
def db_vc():
    """In-memory DB with a mutations table pre-populated for VC filter tests."""
    conn = duckdb.connect(":memory:")
    conn.execute(f"""CREATE TABLE "{STUDY}_mutations" (
        SAMPLE_ID VARCHAR, Hugo_Symbol VARCHAR, Entrez_Gene_Id VARCHAR,
        Variant_Classification VARCHAR, Mutation_Status VARCHAR
    )""")
    rows = [
        ("S1", "TERT",  "7015",  "5'Flank",          "SOMATIC"),  # keep
        ("S2", "TERT",  "7015",  "5'UTR",             "SOMATIC"),  # exclude
        ("S3", "KRAS",  "3845",  "Missense_Mutation", "SOMATIC"),  # keep
        ("S4", "KRAS",  "3845",  "Silent",            "SOMATIC"),  # exclude
        ("S5", "TP53",  "7157",  "Intron",            "SOMATIC"),  # exclude
        ("S6", "EGFR",  "1956",  "5'UTR",             "SOMATIC"),  # exclude
        ("S7", "BRAF",  "673",   "Missense_Mutation", "UNCALLED"), # keep (status filtered later)
    ]
    conn.executemany(f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?)', rows)
    yield conn
    conn.close()


def test_tert_5flank_kept(db_vc):
    results = _apply_vc_filter(db_vc, STUDY, "")
    symbols_vcs = {(r[0], r[1]) for r in results}
    assert ("TERT", "5'Flank") in symbols_vcs


def test_tert_5utr_excluded(db_vc):
    results = _apply_vc_filter(db_vc, STUDY, "")
    symbols_vcs = {(r[0], r[1]) for r in results}
    assert ("TERT", "5'UTR") not in symbols_vcs


def test_excluded_vc_removed(db_vc):
    results = _apply_vc_filter(db_vc, STUDY, "")
    symbols_vcs = {(r[0], r[1]) for r in results}
    assert ("KRAS", "Silent") not in symbols_vcs
    assert ("TP53", "Intron") not in symbols_vcs
    assert ("EGFR", "5'UTR") not in symbols_vcs


def test_uncalled_kept_at_load(db_vc):
    """Loader does not filter by Mutation_Status — UNCALLED rows pass the VC filter."""
    results = _apply_vc_filter(db_vc, STUDY, "")
    symbols_vcs = {(r[0], r[1]) for r in results}
    assert ("BRAF", "Missense_Mutation") in symbols_vcs
