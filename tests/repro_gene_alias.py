import duckdb
import pytest
from cbioportal.core.study_view_repository import get_mutated_genes
from cbioportal.core.loader import normalize_hugo_symbols


def _make_conn(study_id):
    """Create an in-memory DuckDB connection with the minimal schema for tests."""
    conn = duckdb.connect(":memory:")

    conn.execute(f'CREATE TABLE "{study_id}_mutations" ('
                 'Hugo_Symbol VARCHAR, '
                 'Entrez_Gene_Id VARCHAR, '
                 'Tumor_Sample_Barcode VARCHAR, '
                 'Mutation_Status VARCHAR)')

    conn.execute(f'CREATE TABLE "{study_id}_sample" (SAMPLE_ID VARCHAR)')

    conn.execute('CREATE TABLE IF NOT EXISTS gene_profiled ('
                 'study_id VARCHAR, '
                 'Hugo_Symbol VARCHAR, '
                 'panel_id VARCHAR, '
                 'n_profiled INTEGER)')
    return conn


def _setup_gene_reference(conn, genes):
    """Populate gene_reference table. genes: list of (entrez_id, hugo_symbol)."""
    conn.execute("DROP TABLE IF EXISTS gene_reference")
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO gene_reference VALUES (?, ?, ?)",
        [(eid, sym, "protein-coding") for eid, sym in genes]
    )


def _setup_gene_symbol_updates(conn, updates):
    """Populate gene_symbol_updates table. updates: list of (old_symbol, new_symbol)."""
    conn.execute("DROP TABLE IF EXISTS gene_symbol_updates")
    conn.execute("""
        CREATE TABLE gene_symbol_updates (
            old_symbol VARCHAR PRIMARY KEY,
            new_symbol VARCHAR
        )
    """)
    conn.executemany("INSERT INTO gene_symbol_updates VALUES (?, ?)", updates)


def test_kmt2d_mll2_alias_resolution():
    """MLL2 (Entrez 8085) should be normalized to KMT2D via gene_reference."""
    conn = _make_conn("repro_study")
    study_id = "repro_study"

    _setup_gene_reference(conn, [(8085, "KMT2D")])

    mutations = [
        ('KMT2D', '8085', 'S1', 'SOMATIC'),
        ('KMT2D', '8085', 'S2', 'SOMATIC'),
        ('MLL2',  '8085', 'S3', 'SOMATIC'),
    ]
    conn.executemany(f'INSERT INTO "{study_id}_mutations" VALUES (?, ?, ?, ?)', mutations)
    conn.execute(f"INSERT INTO \"{study_id}_sample\" VALUES ('S1'), ('S2'), ('S3')")
    conn.execute(f"INSERT INTO gene_profiled VALUES ('{study_id}', 'KMT2D', 'PANEL1', 3)")

    normalize_hugo_symbols(conn, study_id)

    results = get_mutated_genes(conn, study_id, limit=10)

    kmt2d_row = next((r for r in results if r["gene"] == "KMT2D"), None)
    mll2_row = next((r for r in results if r["gene"] == "MLL2"), None)

    assert mll2_row is None, "MLL2 should NOT appear as a separate entry"
    assert kmt2d_row is not None, "KMT2D should be present"
    assert kmt2d_row["n_mut"] == 3, (
        f"KMT2D should have 3 mutations (MLL2 consolidated), got {kmt2d_row['n_mut']}"
    )


def test_symbol_map_consolidation():
    """CDKN2AP16INK4A (Entrez -1) should map to CDKN2A via gene_symbol_updates."""
    conn = _make_conn("s1")
    study_id = "s1"

    _setup_gene_reference(conn, [(1029, "CDKN2A")])
    _setup_gene_symbol_updates(conn, [("CDKN2AP16INK4A", "CDKN2A")])

    mutations = [
        ('CDKN2AP16INK4A', '-1', 'S1', 'SOMATIC'),
        ('CDKN2AP16INK4A', '-1', 'S2', 'SOMATIC'),
    ]
    conn.executemany(f'INSERT INTO "{study_id}_mutations" VALUES (?, ?, ?, ?)', mutations)
    conn.execute(f"INSERT INTO \"{study_id}_sample\" VALUES ('S1'), ('S2')")
    conn.execute(f"INSERT INTO gene_profiled VALUES ('{study_id}', 'CDKN2A', 'PANEL1', 2)")

    normalize_hugo_symbols(conn, study_id)

    results = get_mutated_genes(conn, study_id, limit=10)
    symbols = [r["gene"] for r in results]

    assert "CDKN2AP16INK4A" not in symbols, "CDKN2AP16INK4A should be normalized away"
    assert "CDKN2A" in symbols, "CDKN2A should appear after normalization"


def test_multiple_aliases_same_canonical():
    """CDKN2AP16INK4A and CDKN2AP14ARF both map to CDKN2A → 3 total mutations."""
    conn = _make_conn("s2")
    study_id = "s2"

    _setup_gene_reference(conn, [(1029, "CDKN2A")])
    _setup_gene_symbol_updates(conn, [
        ("CDKN2AP16INK4A", "CDKN2A"),
        ("CDKN2AP14ARF", "CDKN2A"),
    ])

    mutations = [
        ('CDKN2AP16INK4A', '-1', 'S1', 'SOMATIC'),
        ('CDKN2AP14ARF',   '-2', 'S2', 'SOMATIC'),
        ('CDKN2A',         '1029', 'S3', 'SOMATIC'),
    ]
    conn.executemany(f'INSERT INTO "{study_id}_mutations" VALUES (?, ?, ?, ?)', mutations)
    conn.execute(f"INSERT INTO \"{study_id}_sample\" VALUES ('S1'), ('S2'), ('S3')")
    conn.execute(f"INSERT INTO gene_profiled VALUES ('{study_id}', 'CDKN2A', 'PANEL1', 3)")

    normalize_hugo_symbols(conn, study_id)

    results = get_mutated_genes(conn, study_id, limit=10)
    symbols = [r["gene"] for r in results]
    cdkn2a_row = next((r for r in results if r["gene"] == "CDKN2A"), None)

    assert "CDKN2AP16INK4A" not in symbols
    assert "CDKN2AP14ARF" not in symbols
    assert cdkn2a_row is not None, "CDKN2A should appear"
    assert cdkn2a_row["n_mut"] == 3, f"Expected 3 mutations, got {cdkn2a_row['n_mut']}"


def test_n_samples_distinct_after_merge():
    """S1=KMT2D, S2=MLL2, S2 also has KMT2D → n_samples=2, n_mut=3."""
    conn = _make_conn("s3")
    study_id = "s3"

    _setup_gene_reference(conn, [(8085, "KMT2D")])

    mutations = [
        ('KMT2D', '8085', 'S1', 'SOMATIC'),
        ('MLL2',  '8085', 'S2', 'SOMATIC'),
        ('KMT2D', '8085', 'S2', 'SOMATIC'),  # S2 has both names
    ]
    conn.executemany(f'INSERT INTO "{study_id}_mutations" VALUES (?, ?, ?, ?)', mutations)
    conn.execute(f"INSERT INTO \"{study_id}_sample\" VALUES ('S1'), ('S2')")
    conn.execute(f"INSERT INTO gene_profiled VALUES ('{study_id}', 'KMT2D', 'PANEL1', 2)")

    normalize_hugo_symbols(conn, study_id)

    results = get_mutated_genes(conn, study_id, limit=10)
    kmt2d_row = next((r for r in results if r["gene"] == "KMT2D"), None)

    assert kmt2d_row is not None
    assert kmt2d_row["n_mut"] == 3, f"Expected n_mut=3, got {kmt2d_row['n_mut']}"
    assert kmt2d_row["n_samples"] == 2, f"Expected n_samples=2 (distinct), got {kmt2d_row['n_samples']}"


def test_unknown_gene_preserved():
    """Mutations for genes not in any reference table should remain unchanged."""
    conn = _make_conn("s4")
    study_id = "s4"

    _setup_gene_reference(conn, [(8085, "KMT2D")])

    mutations = [
        ('UNKNOWN_GENE', '99999', 'S1', 'SOMATIC'),
    ]
    conn.executemany(f'INSERT INTO "{study_id}_mutations" VALUES (?, ?, ?, ?)', mutations)
    conn.execute(f"INSERT INTO \"{study_id}_sample\" VALUES ('S1')")
    conn.execute(f"INSERT INTO gene_profiled VALUES ('{study_id}', 'UNKNOWN_GENE', 'PANEL1', 1)")

    normalize_hugo_symbols(conn, study_id)

    results = get_mutated_genes(conn, study_id, limit=10)
    symbols = [r["gene"] for r in results]

    assert "UNKNOWN_GENE" in symbols, "Unknown genes should be preserved as-is"


def test_graceful_noop_without_reference():
    """normalize_hugo_symbols should not raise when gene_reference is absent."""
    conn = duckdb.connect(":memory:")
    study_id = "s5"

    conn.execute(f'CREATE TABLE "{study_id}_mutations" ('
                 'Hugo_Symbol VARCHAR, Entrez_Gene_Id VARCHAR, '
                 'Tumor_Sample_Barcode VARCHAR, Mutation_Status VARCHAR)')
    conn.execute(f'INSERT INTO "{study_id}_mutations" VALUES (\'MLL2\', \'8085\', \'S1\', \'SOMATIC\')')

    # No gene_reference table exists — should not raise
    normalize_hugo_symbols(conn, study_id)

    row = conn.execute(f'SELECT Hugo_Symbol FROM "{study_id}_mutations"').fetchone()
    assert row[0] == "MLL2", "Data should be unchanged when reference is absent"


def test_cna_normalization():
    """CNA table with CDKN2AP16INK4A should be normalized to CDKN2A."""
    conn = duckdb.connect(":memory:")
    study_id = "s6"

    conn.execute(f'CREATE TABLE "{study_id}_cna" ('
                 'study_id VARCHAR, hugo_symbol VARCHAR, sample_id VARCHAR, cna_value INTEGER)')
    conn.execute(f"INSERT INTO \"{study_id}_cna\" VALUES "
                 f"('{study_id}', 'CDKN2AP16INK4A', 'S1', -2), "
                 f"('{study_id}', 'CDKN2AP16INK4A', 'S2', -2)")

    _setup_gene_reference(conn, [(1029, "CDKN2A")])
    _setup_gene_symbol_updates(conn, [("CDKN2AP16INK4A", "CDKN2A")])

    normalize_hugo_symbols(conn, study_id)

    symbols = [r[0] for r in conn.execute(f'SELECT DISTINCT hugo_symbol FROM "{study_id}_cna"').fetchall()]
    assert "CDKN2AP16INK4A" not in symbols, "CDKN2AP16INK4A should be normalized in CNA table"
    assert "CDKN2A" in symbols, "CDKN2A should appear in CNA table after normalization"


def test_cna_normalization_via_mutations_bridge():
    """CNA MLL2 should be normalized to KMT2D using mutations table as Entrez ID bridge."""
    conn = duckdb.connect(":memory:")
    study_id = "s8"

    conn.execute(f'CREATE TABLE "{study_id}_mutations" ('
                 'Hugo_Symbol VARCHAR, '
                 'Entrez_Gene_Id VARCHAR, '
                 'Tumor_Sample_Barcode VARCHAR, '
                 'Mutation_Status VARCHAR)')
    conn.execute(f'INSERT INTO "{study_id}_mutations" VALUES (\'MLL2\', \'8085\', \'S1\', \'SOMATIC\')')

    conn.execute(f'CREATE TABLE "{study_id}_cna" ('
                 'study_id VARCHAR, hugo_symbol VARCHAR, sample_id VARCHAR, cna_value INTEGER)')
    conn.execute(f"INSERT INTO \"{study_id}_cna\" VALUES ('{study_id}', 'MLL2', 'S1', -2)")

    _setup_gene_reference(conn, [(8085, "KMT2D")])

    normalize_hugo_symbols(conn, study_id)

    symbols = [r[0] for r in conn.execute(f'SELECT DISTINCT hugo_symbol FROM "{study_id}_cna"').fetchall()]
    assert "MLL2" not in symbols, "MLL2 should be normalized in CNA table via mutations bridge"
    assert "KMT2D" in symbols, "KMT2D should appear in CNA table after bridge normalization"


def test_profiling_join_after_normalization():
    """Panel has KMT2D; mutations had MLL2 (now normalized) → get_mutated_genes works."""
    conn = _make_conn("s7")
    study_id = "s7"

    _setup_gene_reference(conn, [(8085, "KMT2D")])

    mutations = [
        ('MLL2', '8085', 'S1', 'SOMATIC'),
        ('MLL2', '8085', 'S2', 'SOMATIC'),
    ]
    conn.executemany(f'INSERT INTO "{study_id}_mutations" VALUES (?, ?, ?, ?)', mutations)
    conn.execute(f"INSERT INTO \"{study_id}_sample\" VALUES ('S1'), ('S2')")
    # Panel uses the canonical name KMT2D
    conn.execute(f"INSERT INTO gene_profiled VALUES ('{study_id}', 'KMT2D', 'PANEL1', 2)")

    normalize_hugo_symbols(conn, study_id)

    results = get_mutated_genes(conn, study_id, limit=10)
    kmt2d_row = next((r for r in results if r["gene"] == "KMT2D"), None)
    mll2_row = next((r for r in results if r["gene"] == "MLL2"), None)

    assert mll2_row is None, "MLL2 should not appear after normalization"
    assert kmt2d_row is not None, "KMT2D should appear"
    assert kmt2d_row["n_mut"] == 2


if __name__ == "__main__":
    tests = [
        test_kmt2d_mll2_alias_resolution,
        test_symbol_map_consolidation,
        test_multiple_aliases_same_canonical,
        test_n_samples_distinct_after_merge,
        test_unknown_gene_preserved,
        test_graceful_noop_without_reference,
        test_cna_normalization,
        test_cna_normalization_via_mutations_bridge,
        test_profiling_join_after_normalization,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS: {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"FAIL: {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR: {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        raise SystemExit(1)
