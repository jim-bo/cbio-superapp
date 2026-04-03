"""Performance benchmarks: DuckDB UNPIVOT vs Python-row CNA loading.

These tests are skipped by default. Run with:

    uv run pytest tests/performance/ -v --run-perf

Two strategies are benchmarked against the same real CNA file
(acyc_fmi_2014 — 26 genes × 28 samples, 4.9 KB):

  * unpivot  — current approach: SQL UNPIVOT inside DuckDB
  * python   — new approach: Python reads the file row-by-row, inserts long table

Both must produce identical output. The benchmarks capture wall-clock time and
peak RSS memory so the results can be written to BETA.md.
"""
from __future__ import annotations

import os
import time

import psutil
from pathlib import Path

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Study under test
# ---------------------------------------------------------------------------

STUDY_ID = "acyc_fmi_2014"
CNA_FILE = Path(os.environ.get(
    "CBIO_DOWNLOADS",
    "/Users/jlindsay/Code/cbioportal/cbio-revamp/downloads",
)) / STUDY_ID / "data_cna.txt"

pytestmark = pytest.mark.perf


# ---------------------------------------------------------------------------
# Helper: measure wall time + peak memory for a callable
# ---------------------------------------------------------------------------

def _measure(fn) -> tuple[list[tuple], float, float]:
    """Run fn(conn) and return (rows, elapsed_s, peak_rss_mb).

    Uses psutil RSS to capture both Python heap and DuckDB C-level allocations.
    conn is a fresh in-memory DuckDB pre-seeded with gene_reference rows
    for the genes present in acyc_fmi_2014.
    """
    proc = psutil.Process()
    conn = _make_conn()
    rss_before = proc.memory_info().rss
    peak_rss = rss_before
    t0 = time.perf_counter()
    fn(conn)
    elapsed = time.perf_counter() - t0
    rss_after = proc.memory_info().rss
    peak_rss = max(peak_rss, rss_after)
    peak_mb = (peak_rss - rss_before) / (1024 * 1024)
    rows = conn.execute(
        f'SELECT hugo_symbol, sample_id, cna_value FROM "{STUDY_ID}_cna" ORDER BY hugo_symbol, sample_id'
    ).fetchall()
    conn.close()
    return rows, elapsed, peak_mb


def _make_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with a gene_reference table seeded from the CNA file headers.

    We only need Entrez→Hugo mappings for the genes in this file; we build them
    directly from the file so the test has no external DB dependency.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR
        )
    """)
    # Seed from the file itself — every row has Hugo_Symbol and Entrez_Gene_Id
    with open(CNA_FILE) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            header = line.strip().split("\t")
            break
    hugo_col = header.index("Hugo_Symbol") if "Hugo_Symbol" in header else None
    entrez_col = header.index("Entrez_Gene_Id") if "Entrez_Gene_Id" in header else None

    if hugo_col is not None and entrez_col is not None:
        with open(CNA_FILE) as fh:
            for line in fh:
                if line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if parts[0] == "Hugo_Symbol":
                    continue
                try:
                    entrez = int(parts[entrez_col])
                    hugo = parts[hugo_col]
                    conn.execute(
                        "INSERT OR IGNORE INTO gene_reference VALUES (?, ?, NULL)",
                        (entrez, hugo),
                    )
                except (ValueError, IndexError):
                    pass
    return conn


# ---------------------------------------------------------------------------
# Strategy A: DuckDB UNPIVOT (current implementation)
# ---------------------------------------------------------------------------

def _load_cna_unpivot(conn: duckdb.DuckDBPyConnection) -> None:
    """Current approach — lifted verbatim from loader/__init__.py."""
    cna_file = CNA_FILE
    raw_study_id = STUDY_ID
    table_name = f'"{raw_study_id}_cna"'

    _NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id"}
    with open(cna_file) as _f:
        for _line in _f:
            if not _line.startswith("#"):
                _header_cols = _line.strip().split("\t")
                break

    _exclude = [c for c in _header_cols if c in _NON_SAMPLE_COLS]
    _exclude_clause = (
        f"({', '.join(_exclude)})" if len(_exclude) > 1 else _exclude[0]
    )
    _has_hugo = "Hugo_Symbol" in _exclude
    if _has_hugo:
        _hugo_select = "Hugo_Symbol as hugo_symbol,"
        _join_clause = ""
    else:
        _hugo_select = "gr.hugo_gene_symbol as hugo_symbol,"
        _join_clause = "JOIN gene_reference gr ON TRY_CAST(unpivoted.Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id"

    sql = f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM (
            SELECT
                '{raw_study_id}' as study_id,
                {_hugo_select}
                sample_id,
                TRY_CAST(cna_value AS FLOAT) as cna_value
            FROM (
                UNPIVOT (SELECT * FROM read_csv('{cna_file}', delim='\t', header=True, all_varchar=True, ignore_errors=True, null_padding=True))
                ON COLUMNS(* EXCLUDE {_exclude_clause})
                INTO
                    NAME sample_id
                    VALUE cna_value
            ) unpivoted
            {_join_clause}
        ) WHERE cna_value IS NOT NULL AND cna_value != 0
    """
    conn.execute(sql)


# ---------------------------------------------------------------------------
# Strategy B: Python row-by-row (new approach)
# ---------------------------------------------------------------------------

def _load_cna_python(conn: duckdb.DuckDBPyConnection) -> None:
    """New approach — Python reads the wide matrix row-by-row and inserts long rows.

    Memory footprint is O(1 row) regardless of how wide the matrix is.
    """
    cna_file = CNA_FILE
    raw_study_id = STUDY_ID
    table_name = f'"{raw_study_id}_cna"'

    conn.execute(f"""
        CREATE TABLE {table_name} (
            study_id  VARCHAR NOT NULL,
            hugo_symbol VARCHAR,
            sample_id VARCHAR NOT NULL,
            cna_value FLOAT NOT NULL
        )
    """)

    _NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id", "Cytoband"}

    with open(cna_file) as fh:
        # Skip comment lines, read header
        for raw in fh:
            if not raw.startswith("#"):
                header = raw.rstrip("\n").split("\t")
                break

        hugo_col = header.index("Hugo_Symbol") if "Hugo_Symbol" in header else None
        entrez_col = header.index("Entrez_Gene_Id") if "Entrez_Gene_Id" in header else None

        # Sample columns are everything that isn't a known non-sample column
        sample_indices = [
            (i, col) for i, col in enumerate(header) if col not in _NON_SAMPLE_COLS
        ]

        # Build entrez→hugo lookup if needed
        entrez_to_hugo: dict[int, str] = {}
        if hugo_col is None and entrez_col is not None:
            rows = conn.execute(
                "SELECT entrez_gene_id, hugo_gene_symbol FROM gene_reference"
            ).fetchall()
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
                    val = float(raw_val)  # preserve fractional values like -1.5
                except (ValueError, IndexError):
                    continue
                if val == 0:
                    continue
                batch.append((raw_study_id, hugo, sample_id, val))

            if len(batch) >= 10_000:
                conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)
                batch.clear()

        if batch:
            conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)


# ---------------------------------------------------------------------------
# Correctness test: both strategies produce identical output
# ---------------------------------------------------------------------------

def test_cna_strategies_produce_identical_output():
    """Both strategies must return the same long-format rows for acyc_fmi_2014."""
    if not CNA_FILE.exists():
        pytest.skip(f"CNA file not found: {CNA_FILE}")

    rows_unpivot, _, _ = _measure(_load_cna_unpivot)
    rows_python, _, _ = _measure(_load_cna_python)

    assert rows_unpivot == rows_python, (
        f"Strategy outputs differ.\n"
        f"  unpivot rows: {len(rows_unpivot)}\n"
        f"  python  rows: {len(rows_python)}\n"
        f"  first unpivot: {rows_unpivot[:3]}\n"
        f"  first python:  {rows_python[:3]}"
    )
    assert len(rows_unpivot) > 0, "Expected non-zero CNA rows for acyc_fmi_2014"


# ---------------------------------------------------------------------------
# Benchmark: UNPIVOT
# ---------------------------------------------------------------------------

def test_benchmark_unpivot():
    """Benchmark DuckDB UNPIVOT strategy."""
    if not CNA_FILE.exists():
        pytest.skip(f"CNA file not found: {CNA_FILE}")

    rows, elapsed, peak_mb = _measure(_load_cna_unpivot)
    print(f"\n  [UNPIVOT] rows={len(rows)}  elapsed={elapsed:.4f}s  peak_mem={peak_mb:.2f}MB")
    assert len(rows) > 0


# ---------------------------------------------------------------------------
# Benchmark: Python row-by-row
# ---------------------------------------------------------------------------

def test_benchmark_python():
    """Benchmark Python row-by-row strategy."""
    if not CNA_FILE.exists():
        pytest.skip(f"CNA file not found: {CNA_FILE}")

    rows, elapsed, peak_mb = _measure(_load_cna_python)
    print(f"\n  [PYTHON]  rows={len(rows)}  elapsed={elapsed:.4f}s  peak_mem={peak_mb:.2f}MB")
    assert len(rows) > 0


# ---------------------------------------------------------------------------
# Combined: print side-by-side comparison (small file)
# ---------------------------------------------------------------------------

def test_benchmark_comparison():
    """Run both strategies and print a side-by-side comparison table."""
    if not CNA_FILE.exists():
        pytest.skip(f"CNA file not found: {CNA_FILE}")

    rows_u, elapsed_u, peak_u = _measure(_load_cna_unpivot)
    rows_p, elapsed_p, peak_p = _measure(_load_cna_python)

    print(f"""
CNA load benchmark — {STUDY_ID}  ({CNA_FILE.stat().st_size / 1024:.1f} KB, {len(rows_u)} non-zero rows)

Strategy   | Wall time (s) | Peak memory (MB)
-----------|--------------|------------------
UNPIVOT    | {elapsed_u:>12.4f} | {peak_u:>16.2f}
Python     | {elapsed_p:>12.4f} | {peak_p:>16.2f}
Ratio      | {elapsed_u/elapsed_p if elapsed_p else float('inf'):>12.2f}x | {peak_u/peak_p if peak_p else float('inf'):>15.2f}x
""")

    # Correctness
    assert rows_u == rows_p, "Strategies produced different output"


# ---------------------------------------------------------------------------
# Medium-file benchmark: hcc_msk_2024  (541 genes × 1,371 samples, 1.4 MB)
# ---------------------------------------------------------------------------

MEDIUM_STUDY_ID = "hcc_msk_2024"
MEDIUM_CNA_FILE = Path(os.environ.get(
    "CBIO_DOWNLOADS",
    "/Users/jlindsay/Code/cbioportal/cbio-revamp/downloads",
)) / MEDIUM_STUDY_ID / "data_cna.txt"


def _measure_for(fn, cna_file: Path, study_id: str) -> tuple[int, float, float]:
    """Run fn(conn, cna_file, study_id) and return (row_count, elapsed_s, peak_rss_mb).

    Uses psutil RSS so DuckDB C-level allocations are included in the measurement.
    """
    proc = psutil.Process()
    conn = _make_conn_for(cna_file, study_id)
    rss_before = proc.memory_info().rss
    t0 = time.perf_counter()
    row_count = fn(cna_file, study_id, conn)
    elapsed = time.perf_counter() - t0
    rss_after = proc.memory_info().rss
    peak_mb = (rss_after - rss_before) / (1024 * 1024)
    conn.close()
    return row_count, elapsed, peak_mb


def _load_cna_unpivot_for(cna_file: Path, study_id: str, conn: duckdb.DuckDBPyConnection) -> int:
    """UNPIVOT strategy, parameterised over file and study."""
    table_name = f'"{study_id}_cna"'
    _NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id"}
    with open(cna_file) as _f:
        for _line in _f:
            if not _line.startswith("#"):
                _header_cols = _line.strip().split("\t")
                break
    _exclude = [c for c in _header_cols if c in _NON_SAMPLE_COLS]
    _exclude_clause = (
        f"({', '.join(_exclude)})" if len(_exclude) > 1 else _exclude[0]
    )
    _has_hugo = "Hugo_Symbol" in _exclude
    if _has_hugo:
        _hugo_select = "Hugo_Symbol as hugo_symbol,"
        _join_clause = ""
    else:
        _hugo_select = "gr.hugo_gene_symbol as hugo_symbol,"
        _join_clause = "JOIN gene_reference gr ON TRY_CAST(unpivoted.Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id"
    sql = f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM (
            SELECT
                '{study_id}' as study_id,
                {_hugo_select}
                sample_id,
                TRY_CAST(cna_value AS FLOAT) as cna_value
            FROM (
                UNPIVOT (SELECT * FROM read_csv('{cna_file}', delim='\t', header=True, all_varchar=True, ignore_errors=True, null_padding=True))
                ON COLUMNS(* EXCLUDE {_exclude_clause})
                INTO NAME sample_id VALUE cna_value
            ) unpivoted
            {_join_clause}
        ) WHERE cna_value IS NOT NULL AND cna_value != 0
    """
    conn.execute(sql)
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def test_benchmark_medium_comparison():
    """Side-by-side benchmark on hcc_msk_2024 (541 genes × 1,371 samples, 1.4 MB)."""
    if not MEDIUM_CNA_FILE.exists():
        pytest.skip(f"Medium CNA file not found: {MEDIUM_CNA_FILE}")

    rows_u, elapsed_u, peak_u = _measure_for(_load_cna_unpivot_for, MEDIUM_CNA_FILE, MEDIUM_STUDY_ID)
    rows_p, elapsed_p, peak_p = _measure_for(_load_cna_python_for, MEDIUM_CNA_FILE, MEDIUM_STUDY_ID)
    file_mb = MEDIUM_CNA_FILE.stat().st_size / (1024 * 1024)

    print(f"""
CNA load benchmark — {MEDIUM_STUDY_ID}  ({file_mb:.1f} MB, {rows_u} non-zero rows)

Strategy   | Wall time (s) | Peak memory (MB)
-----------|--------------|------------------
UNPIVOT    | {elapsed_u:>12.4f} | {peak_u:>16.2f}
Python     | {elapsed_p:>12.4f} | {peak_p:>16.2f}
Ratio      | {elapsed_u/elapsed_p if elapsed_p else 0:>12.2f}x | {peak_u/peak_p if peak_p else 0:>15.2f}x
""")
    assert rows_u == rows_p, f"Output mismatch: unpivot={rows_u} python={rows_p}"


# ---------------------------------------------------------------------------
# Medium-large benchmark: msk_met_2021  (541 genes × 25,776 samples, 27 MB)
# ---------------------------------------------------------------------------

MEDLARGE_STUDY_ID = "msk_met_2021"
MEDLARGE_CNA_FILE = Path(os.environ.get(
    "CBIO_DOWNLOADS",
    "/Users/jlindsay/Code/cbioportal/cbio-revamp/downloads",
)) / MEDLARGE_STUDY_ID / "data_cna.txt"

TIMEOUT_S = 60  # fail the strategy if it exceeds this


def _measure_for_with_timeout(fn, cna_file: Path, study_id: str) -> tuple[int, float, float] | None:
    """Like _measure_for but returns None if the function exceeds TIMEOUT_S.

    Uses psutil RSS for memory so DuckDB C-level allocations are captured.
    Note: SIGALRM cannot interrupt C-extension calls, so timeout is best-effort.
    """
    import signal

    def _handler(signum, frame):
        raise TimeoutError(f"Exceeded {TIMEOUT_S}s timeout")

    proc = psutil.Process()
    conn = _make_conn_for(cna_file, study_id)
    rss_before = proc.memory_info().rss
    t0 = time.perf_counter()
    try:
        signal.signal(signal.SIGALRM, _handler)
        signal.alarm(TIMEOUT_S)
        row_count = fn(cna_file, study_id, conn)
        signal.alarm(0)
    except TimeoutError:
        conn.close()
        return None
    elapsed = time.perf_counter() - t0
    rss_after = proc.memory_info().rss
    peak_mb = (rss_after - rss_before) / (1024 * 1024)
    conn.close()
    return row_count, elapsed, peak_mb


def test_benchmark_medlarge_comparison():
    """Side-by-side benchmark on msk_met_2021 (541 genes × 25,776 samples, 27 MB).

    This is the inflection point: large enough that UNPIVOT may OOM or timeout
    while the Python approach stays bounded. Times out after 60 seconds per strategy.
    """
    if not MEDLARGE_CNA_FILE.exists():
        pytest.skip(f"File not found: {MEDLARGE_CNA_FILE}")

    file_mb = MEDLARGE_CNA_FILE.stat().st_size / (1024 * 1024)

    result_u = _measure_for_with_timeout(_load_cna_unpivot_for, MEDLARGE_CNA_FILE, MEDLARGE_STUDY_ID)
    result_p = _measure_for_with_timeout(_load_cna_python_for, MEDLARGE_CNA_FILE, MEDLARGE_STUDY_ID)

    def fmt(result):
        if result is None:
            return f"{'TIMEOUT':>12} | {'TIMEOUT':>16}"
        rows, elapsed, peak = result
        return f"{elapsed:>12.2f} | {peak:>16.2f}"

    rows_u = result_u[0] if result_u else "TIMEOUT"
    rows_p = result_p[0] if result_p else "TIMEOUT"

    print(f"""
CNA load benchmark — {MEDLARGE_STUDY_ID}  ({file_mb:.1f} MB, rows: unpivot={rows_u} python={rows_p})

Strategy   | Wall time (s) | Peak memory (MB)
-----------|--------------|------------------
UNPIVOT    | {fmt(result_u)}
Python     | {fmt(result_p)}
""")

    # At least the Python strategy must succeed
    assert result_p is not None, "Python strategy timed out"
    assert result_p[0] > 0


# ---------------------------------------------------------------------------
# Large-file benchmark: msk_impact_50k_2026  (the one that OOM'd)
# Python-only — UNPIVOT is known to OOM at 38 GB on this file.
# ---------------------------------------------------------------------------

LARGE_STUDY_ID = "msk_impact_50k_2026"
LARGE_CNA_FILE = Path(os.environ.get(
    "CBIO_DOWNLOADS",
    "/Users/jlindsay/Code/cbioportal/cbio-revamp/downloads",
)) / LARGE_STUDY_ID / "data_cna.txt"


def _make_conn_for(cna_file: Path, study_id: str) -> duckdb.DuckDBPyConnection:
    """Generic version of _make_conn for any CNA file with Hugo_Symbol column."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR
        )
    """)
    return conn


def _load_cna_python_for(cna_file: Path, study_id: str, conn: duckdb.DuckDBPyConnection) -> int:
    """Python row-by-row loader, parameterised over file and study."""
    table_name = f'"{study_id}_cna"'
    conn.execute(f"""
        CREATE TABLE {table_name} (
            study_id  VARCHAR NOT NULL,
            hugo_symbol VARCHAR,
            sample_id VARCHAR NOT NULL,
            cna_value FLOAT NOT NULL
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
        sample_indices = [
            (i, col) for i, col in enumerate(header) if col not in _NON_SAMPLE_COLS
        ]
        batch: list[tuple] = []
        for raw in fh:
            parts = raw.rstrip("\n").split("\t")
            hugo = parts[hugo_col] if hugo_col is not None else ""
            for idx, sample_id in sample_indices:
                try:
                    raw_val = parts[idx].strip()
                    if raw_val in ("", "NA", "null", "NULL"):
                        continue
                    val = float(raw_val)  # preserve fractional values like -1.5
                except (ValueError, IndexError):
                    continue
                if val == 0:
                    continue
                batch.append((study_id, hugo, sample_id, val))
            if len(batch) >= 10_000:
                conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)
                batch.clear()
        if batch:
            conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def test_benchmark_large_file_python():
    """Python row-by-row strategy on msk_impact_50k_2026 (54k samples, 57 MB).

    UNPIVOT is intentionally not run here — it OOMs at 38.3 GB on this file.
    This test documents that the Python approach completes successfully.
    """
    if not LARGE_CNA_FILE.exists():
        pytest.skip(f"File not found: {LARGE_CNA_FILE}")

    proc = psutil.Process()
    conn = _make_conn_for(LARGE_CNA_FILE, LARGE_STUDY_ID)
    rss_before = proc.memory_info().rss
    t0 = time.perf_counter()
    row_count = _load_cna_python_for(LARGE_CNA_FILE, LARGE_STUDY_ID, conn)
    elapsed = time.perf_counter() - t0
    rss_after = proc.memory_info().rss
    peak_mb = (rss_after - rss_before) / (1024 * 1024)
    file_mb = LARGE_CNA_FILE.stat().st_size / (1024 * 1024)
    conn.close()

    print(f"""
CNA load benchmark — {LARGE_STUDY_ID}  ({file_mb:.1f} MB source file)

Strategy   | Wall time (s) | Peak RSS delta (MB) | Rows loaded
-----------|--------------|---------------------|------------
Python     | {elapsed:>12.2f} | {peak_mb:>19.2f} | {row_count:>11,}
UNPIVOT    |          n/a |         OOM @ 38 GB |         n/a
""")
    assert row_count > 0
