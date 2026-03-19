"""Unit tests for get_numeric_histogram — equal-width histogram binning."""
import duckdb
import pytest

from cbioportal.core.study_view.clinical import get_numeric_histogram

STUDY = "test_study"


def _make_db(values: list, attr: str = "SCORE", source: str = "sample"):
    """Create in-memory DB with one numeric clinical attribute."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, {attr} DOUBLE)')
    if source == "patient":
        conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR, {attr} DOUBLE)')
    for i, v in enumerate(values):
        if source == "sample":
            if v is None:
                conn.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, NULL)', (f"S{i}", f"P{i}"))
            else:
                conn.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)', (f"S{i}", f"P{i}", v))
    return conn


def test_float_bins_auto_size():
    """Values 0–100 should produce ~10–20 bins with auto bin_size."""
    values = list(range(0, 101))  # 0, 1, 2, ..., 100
    conn = _make_db(values)
    result = get_numeric_histogram(conn, STUDY, "SCORE")
    conn.close()
    bins = [r for r in result if r["x"] != "NA"]
    assert 10 <= len(bins) <= 20, f"Expected 10–20 bins, got {len(bins)}: {bins}"
    # All counts should be positive integers
    for b in bins:
        assert b["y"] > 0
        assert "-" in b["x"]


def test_integer_bins():
    """Explicit bin_size=5 with values 0–50 should produce 10 bins of 5 each."""
    values = list(range(0, 50))  # 0..49
    conn = _make_db(values)
    result = get_numeric_histogram(conn, STUDY, "SCORE", bin_size=5)
    conn.close()
    bins = [r for r in result if r["x"] != "NA"]
    assert len(bins) == 10, f"Expected 10 bins, got {len(bins)}: {bins}"
    for b in bins:
        assert b["y"] == 5, f"Each bin should have 5 samples, got {b['y']} for {b['x']}"
    # First bin should be 0-5
    assert bins[0]["x"] == "0-5", f"First bin should be '0-5', got '{bins[0]['x']}'"


def test_na_excluded_from_bins():
    """NULL values should not appear in the main bins but be counted in the NA entry."""
    values = [0.0, 10.0, 20.0, None, None]
    conn = _make_db(values)
    result = get_numeric_histogram(conn, STUDY, "SCORE")
    conn.close()
    bins = [r for r in result if r["x"] != "NA"]
    na_entries = [r for r in result if r["x"] == "NA"]
    # Non-NA bins should only count non-null values
    total_bin_count = sum(b["y"] for b in bins)
    assert total_bin_count == 3, f"Expected 3 non-NA values in bins, got {total_bin_count}"
    # NA entry should exist and count 2
    assert len(na_entries) == 1, f"Expected 1 NA entry, got {na_entries}"
    assert na_entries[0]["y"] == 2, f"Expected na_count=2, got {na_entries[0]['y']}"


def test_single_value_attr():
    """All samples with the same value should produce exactly 1 bin."""
    values = [42.0, 42.0, 42.0, 42.0]
    conn = _make_db(values)
    result = get_numeric_histogram(conn, STUDY, "SCORE")
    conn.close()
    bins = [r for r in result if r["x"] != "NA"]
    assert len(bins) == 1, f"Expected 1 bin for single value, got {len(bins)}: {bins}"
    assert bins[0]["y"] == 4, f"Expected count=4, got {bins[0]['y']}"


def test_all_null_returns_empty():
    """All-NULL attribute should return empty list (no min/max computable)."""
    values = [None, None, None]
    conn = _make_db(values)
    result = get_numeric_histogram(conn, STUDY, "SCORE")
    conn.close()
    assert result == [], f"Expected empty list for all-NULL, got {result}"


def test_na_not_in_result_when_count_is_zero():
    """When there are no NULLs, no NA entry should appear in the result."""
    values = [1.0, 2.0, 3.0]
    conn = _make_db(values)
    result = get_numeric_histogram(conn, STUDY, "SCORE")
    conn.close()
    na_entries = [r for r in result if r["x"] == "NA"]
    assert na_entries == [], f"Expected no NA entry when no NULLs, got {na_entries}"
