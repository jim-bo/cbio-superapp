"""Unit tests for CNA loading — both UNPIVOT and Python row-by-row strategies.

Verifies that:
- Integer values (-2, -1, 1, 2) are preserved as exact floats
- Fractional values (-1.5, 1.5) are preserved without rounding
- NA / empty cells are excluded
- Zero values are excluded
- Both strategies produce identical output
"""
import tempfile
import os
from pathlib import Path

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR
        )
    """)
    conn.execute("INSERT INTO gene_reference VALUES (1956, 'EGFR', 'protein-coding')")
    conn.execute("INSERT INTO gene_reference VALUES (5156, 'PDGFRA', 'protein-coding')")
    return conn


def _write_cna(rows: list[str]) -> Path:
    """Write a CNA file to a temp location and return its path."""
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
    for row in rows:
        f.write(row + "\n")
    f.close()
    return Path(f.name)


def _load_unpivot(conn: duckdb.DuckDBPyConnection, cna_file: Path, study_id: str) -> list[tuple]:
    """Run the UNPIVOT strategy and return sorted (hugo_symbol, sample_id, cna_value) rows."""
    table_name = f'"{study_id}_cna"'
    _NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id"}
    with open(cna_file) as f:
        for line in f:
            if not line.startswith("#"):
                header = line.strip().split("\t")
                break
    _exclude = [c for c in header if c in _NON_SAMPLE_COLS]
    _exclude_clause = f"({', '.join(_exclude)})" if len(_exclude) > 1 else _exclude[0]
    _has_hugo = "Hugo_Symbol" in _exclude
    if _has_hugo:
        hugo_select = "Hugo_Symbol as hugo_symbol,"
        join_clause = ""
    else:
        hugo_select = "gr.hugo_gene_symbol as hugo_symbol,"
        join_clause = "JOIN gene_reference gr ON TRY_CAST(unpivoted.Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id"

    conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM (
            SELECT
                '{study_id}' as study_id,
                {hugo_select}
                sample_id,
                TRY_CAST(cna_value AS DOUBLE) as cna_value
            FROM (
                UNPIVOT (SELECT * FROM read_csv('{cna_file}', delim='\t', header=True, all_varchar=True, ignore_errors=True, null_padding=True))
                ON COLUMNS(* EXCLUDE {_exclude_clause})
                INTO NAME sample_id VALUE cna_value
            ) unpivoted
            {join_clause}
        ) WHERE cna_value IS NOT NULL AND cna_value != 0
    """)
    return conn.execute(
        f'SELECT hugo_symbol, sample_id, cna_value FROM {table_name} ORDER BY hugo_symbol, sample_id'
    ).fetchall()


def _load_python(conn: duckdb.DuckDBPyConnection, cna_file: Path, study_id: str) -> list[tuple]:
    """Run the Python row-by-row strategy and return sorted (hugo_symbol, sample_id, cna_value) rows."""
    table_name = f'"{study_id}_cna"'
    conn.execute(f"""
        CREATE TABLE {table_name} (
            study_id    VARCHAR NOT NULL,
            hugo_symbol VARCHAR,
            sample_id   VARCHAR NOT NULL,
            cna_value   DOUBLE NOT NULL
        )
    """)
    _NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id", "Cytoband"}
    with open(cna_file) as fh:
        for raw in fh:
            if not raw.startswith("#"):
                header = raw.rstrip("\n").split("\t")
                break
        hugo_col = header.index("Hugo_Symbol") if "Hugo_Symbol" in header else None
        entrez_col = header.index("Entrez_Gene_Id") if "Entrez_Gene_Id" in header else None
        sample_indices = [(i, col) for i, col in enumerate(header) if col not in _NON_SAMPLE_COLS]

        entrez_to_hugo: dict[int, str] = {}
        if hugo_col is None and entrez_col is not None:
            rows = conn.execute("SELECT entrez_gene_id, hugo_gene_symbol FROM gene_reference").fetchall()
            entrez_to_hugo = {r[0]: r[1] for r in rows if r[1]}

        batch: list[tuple] = []
        for raw in fh:
            parts = raw.rstrip("\n").split("\t")
            if hugo_col is not None:
                hugo = parts[hugo_col]
            elif entrez_col is not None:
                try:
                    hugo = entrez_to_hugo.get(int(parts[entrez_col]), "")
                except (ValueError, IndexError):
                    hugo = ""
            else:
                hugo = ""
            for idx, sample_id in sample_indices:
                try:
                    raw_val = parts[idx].strip()
                    if raw_val in ("", "NA", "null", "NULL"):
                        continue
                    val = float(raw_val)
                except (ValueError, IndexError):
                    continue
                if val == 0:
                    continue
                batch.append((study_id, hugo, sample_id, val))

        if batch:
            conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)

    return conn.execute(
        f'SELECT hugo_symbol, sample_id, cna_value FROM {table_name} ORDER BY hugo_symbol, sample_id'
    ).fetchall()


# ---------------------------------------------------------------------------
# Tests: integer CNA values are preserved as exact floats
# ---------------------------------------------------------------------------

CNA_INTEGER = [
    "Hugo_Symbol\tS1\tS2\tS3",
    "TP53\t2\t0\t-2",
    "KRAS\t1\t-1\t0",
]


@pytest.mark.parametrize("loader", [_load_unpivot, _load_python])
def test_integer_values_preserved(loader):
    """Integer CNA values (-2, -1, 1, 2) are stored as exact floats, zeros excluded."""
    cna_file = _write_cna(CNA_INTEGER)
    try:
        conn = _base_conn()
        rows = loader(conn, cna_file, "test")
        by_key = {(r[0], r[1]): r[2] for r in rows}

        assert by_key[("TP53", "S1")] == 2.0
        assert by_key[("TP53", "S3")] == -2.0
        assert by_key[("KRAS", "S1")] == 1.0
        assert by_key[("KRAS", "S2")] == -1.0
        # zeros excluded
        assert ("TP53", "S2") not in by_key
        assert ("KRAS", "S3") not in by_key
    finally:
        os.unlink(cna_file)


# ---------------------------------------------------------------------------
# Tests: fractional values are NOT rounded
# ---------------------------------------------------------------------------

CNA_FRACTIONAL = [
    "Hugo_Symbol\tS1\tS2\tS3\tS4",
    "EGFR\t1.5\t-1.5\t2\t0",
    "KRAS\t-1.5\t1.5\t0\t-2",
]


@pytest.mark.parametrize("loader", [_load_unpivot, _load_python])
def test_fractional_values_preserved(loader):
    """-1.5 and 1.5 are stored as-is, not rounded to -2 or 2."""
    cna_file = _write_cna(CNA_FRACTIONAL)
    try:
        conn = _base_conn()
        rows = loader(conn, cna_file, "test")
        by_key = {(r[0], r[1]): r[2] for r in rows}

        assert by_key[("EGFR", "S1")] == 1.5
        assert by_key[("EGFR", "S2")] == -1.5
        assert by_key[("EGFR", "S3")] == 2.0
        assert by_key[("KRAS", "S1")] == -1.5
        assert by_key[("KRAS", "S2")] == 1.5
        assert by_key[("KRAS", "S4")] == -2.0
        # zeros excluded
        assert ("EGFR", "S4") not in by_key
        assert ("KRAS", "S3") not in by_key
    finally:
        os.unlink(cna_file)


# ---------------------------------------------------------------------------
# Tests: NA and empty cells are excluded
# ---------------------------------------------------------------------------

CNA_WITH_NA = [
    "Hugo_Symbol\tS1\tS2\tS3",
    "TP53\tNA\t2\t",
    "KRAS\t-2\tNA\t1",
]


@pytest.mark.parametrize("loader", [_load_unpivot, _load_python])
def test_na_cells_excluded(loader):
    """NA and empty cells produce no rows."""
    cna_file = _write_cna(CNA_WITH_NA)
    try:
        conn = _base_conn()
        rows = loader(conn, cna_file, "test")
        by_key = {(r[0], r[1]): r[2] for r in rows}

        assert ("TP53", "S1") not in by_key   # NA
        assert ("TP53", "S3") not in by_key   # empty
        assert ("KRAS", "S2") not in by_key   # NA
        assert by_key[("TP53", "S2")] == 2.0
        assert by_key[("KRAS", "S1")] == -2.0
        assert by_key[("KRAS", "S3")] == 1.0
    finally:
        os.unlink(cna_file)


# ---------------------------------------------------------------------------
# Tests: Entrez_Gene_Id only file (joins gene_reference)
# ---------------------------------------------------------------------------

CNA_ENTREZ_ONLY = [
    "Entrez_Gene_Id\tS1\tS2",
    "1956\t2\t-1.5",    # EGFR
    "5156\t0\t-2",      # PDGFRA
]


@pytest.mark.parametrize("loader", [_load_unpivot, _load_python])
def test_entrez_only_file(loader):
    """Files with only Entrez_Gene_Id resolve hugo_symbol via gene_reference."""
    cna_file = _write_cna(CNA_ENTREZ_ONLY)
    try:
        conn = _base_conn()
        rows = loader(conn, cna_file, "test")
        by_key = {(r[0], r[1]): r[2] for r in rows}

        assert by_key[("EGFR", "S1")] == 2.0
        assert by_key[("EGFR", "S2")] == -1.5   # preserved, not rounded
        assert by_key[("PDGFRA", "S2")] == -2.0
        assert ("PDGFRA", "S1") not in by_key    # zero excluded
    finally:
        os.unlink(cna_file)


# ---------------------------------------------------------------------------
# Tests: both strategies produce identical output
# ---------------------------------------------------------------------------

CNA_MIXED = [
    "Hugo_Symbol\tEntrez_Gene_Id\tS1\tS2\tS3",
    "EGFR\t1956\t1.5\tNA\t-2",
    "PDGFRA\t5156\t0\t2\t-1.5",
    "KRAS\t0\t-1\t1\t0",
]


def test_strategies_identical_output():
    """UNPIVOT and Python produce the same rows for a file with both Hugo and Entrez columns."""
    cna_file = _write_cna(CNA_MIXED)
    try:
        rows_u = _load_unpivot(_base_conn(), cna_file, "test")
        rows_p = _load_python(_base_conn(), cna_file, "test")
        assert rows_u == rows_p, (
            f"Strategies differ.\n  unpivot: {rows_u}\n  python:  {rows_p}"
        )
    finally:
        os.unlink(cna_file)
