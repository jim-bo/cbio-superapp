"""Unit tests for _inject_fga_from_seg() in loader.py."""
import duckdb
import pytest
import tempfile
from pathlib import Path

from cbioportal.core.loader import _inject_fga_from_seg

STUDY = "test_study"
SEG_THRESHOLD = 0.2


def _make_db(samples: list[str]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    for sid in samples:
        conn.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (sid, f"P-{sid}"))
    return conn


def _write_seg(path: Path, rows: list[tuple]) -> None:
    """Write a minimal .seg file. rows = (ID, chrom, start, end, num_mark, seg_mean)."""
    with open(path, "w") as f:
        f.write("ID\tchrom\tloc.start\tloc.end\tnum.mark\tseg.mean\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_fga_injected_as_column(tmp_path):
    """Column FRACTION_GENOME_ALTERED is added to the sample table."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100, 0.5),
    ])
    conn = _make_db(["S1"])
    result = _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    assert result is True
    cols = {c[0] for c in conn.execute(f'DESCRIBE "{STUDY}_sample"').fetchall()}
    assert "FRACTION_GENOME_ALTERED" in cols


def test_fga_fully_altered(tmp_path):
    """Sample with all segments altered => FGA = 1.0."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100,  0.8),
        ("S1", "chr2", 0, 2_000_000, 200, -0.5),
    ])
    conn = _make_db(["S1"])
    _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    fga = conn.execute(f'SELECT FRACTION_GENOME_ALTERED FROM "{STUDY}_sample" WHERE SAMPLE_ID = ?', ("S1",)).fetchone()[0]
    assert fga == 1.0


def test_fga_partially_altered(tmp_path):
    """Sample with half of bases altered => FGA = 0.5."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100,  0.5),   # altered
        ("S1", "chr2", 0, 1_000_000, 100,  0.05),  # below threshold
    ])
    conn = _make_db(["S1"])
    _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    fga = conn.execute(f'SELECT FRACTION_GENOME_ALTERED FROM "{STUDY}_sample" WHERE SAMPLE_ID = ?', ("S1",)).fetchone()[0]
    assert fga == pytest.approx(0.5, abs=1e-4)


def test_fga_no_altered_segments(tmp_path):
    """Sample with all seg.mean below threshold => FGA = 0.0."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100, 0.1),
        ("S1", "chr2", 0, 1_000_000, 100, -0.1),
    ])
    conn = _make_db(["S1"])
    _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    fga = conn.execute(f'SELECT FRACTION_GENOME_ALTERED FROM "{STUDY}_sample" WHERE SAMPLE_ID = ?', ("S1",)).fetchone()[0]
    assert fga == 0.0


def test_fga_threshold_boundary(tmp_path):
    """Segments at exactly 0.2 are counted as altered."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100,  0.2),   # at threshold => altered
        ("S1", "chr2", 0, 1_000_000, 100, -0.2),   # at negative threshold => altered
        ("S1", "chr3", 0, 1_000_000, 100,  0.19),  # just below => not altered
    ])
    conn = _make_db(["S1"])
    _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    fga = conn.execute(f'SELECT FRACTION_GENOME_ALTERED FROM "{STUDY}_sample" WHERE SAMPLE_ID = ?', ("S1",)).fetchone()[0]
    assert fga == pytest.approx(2 / 3, abs=1e-4)


def test_fga_multiple_samples(tmp_path):
    """FGA computed independently per sample."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100, 0.5),   # S1: 100% altered
        ("S2", "chr1", 0, 1_000_000, 100, 0.05),  # S2: 0% altered
    ])
    conn = _make_db(["S1", "S2"])
    _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    rows = {r[0]: r[1] for r in conn.execute(
        f'SELECT SAMPLE_ID, FRACTION_GENOME_ALTERED FROM "{STUDY}_sample"'
    ).fetchall()}
    assert rows["S1"] == 1.0
    assert rows["S2"] == 0.0


def test_fga_sample_not_in_seg_gets_null(tmp_path):
    """Samples absent from the seg file get NULL FGA (not an error)."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [
        ("S1", "chr1", 0, 1_000_000, 100, 0.5),
    ])
    conn = _make_db(["S1", "S2"])  # S2 has no seg data
    _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    fga_s2 = conn.execute(
        f'SELECT FRACTION_GENOME_ALTERED FROM "{STUDY}_sample" WHERE SAMPLE_ID = ?', ("S2",)
    ).fetchone()[0]
    assert fga_s2 is None


def test_fga_uses_hg38_seg_fallback(tmp_path):
    """Falls back to data_cna_hg38.seg when data_cna_hg19.seg is absent."""
    _write_seg(tmp_path / "data_cna_hg38.seg", [
        ("S1", "chr1", 0, 1_000_000, 100, 0.5),
    ])
    conn = _make_db(["S1"])
    result = _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    assert result is True


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

def test_fga_skipped_when_column_exists(tmp_path):
    """Returns False without error if FRACTION_GENOME_ALTERED already in table."""
    _write_seg(tmp_path / "data_cna_hg19.seg", [("S1", "chr1", 0, 1_000_000, 100, 0.5)])
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, FRACTION_GENOME_ALTERED DOUBLE)')
    conn.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', ("S1", 0.99))
    result = _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    assert result is False
    # Original value must be unchanged
    fga = conn.execute(f'SELECT FRACTION_GENOME_ALTERED FROM "{STUDY}_sample"').fetchone()[0]
    assert fga == 0.99


def test_fga_skipped_when_no_seg_file(tmp_path):
    """Returns False if no .seg file exists."""
    conn = _make_db(["S1"])
    result = _inject_fga_from_seg(conn, f'"{STUDY}_sample"', tmp_path)
    assert result is False
