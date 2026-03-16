"""Unit tests for get_tmb_fga_scatter() using in-memory DuckDB.

Returns binned density data + Pearson/Spearman correlations.
Only samples with non-NULL FGA AND mutation_count > 0 (from mutations table) are included.
GERMLINE and Fusion mutations are excluded from the mutation count.
Returns _EMPTY_SCATTER sentinel when no eligible samples exist.
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_tmb_fga_scatter

STUDY = "test_scatter_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sample" (
            SAMPLE_ID VARCHAR,
            PATIENT_ID VARCHAR,
            FGA DOUBLE
        )
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_mutations" (
            SAMPLE_ID VARCHAR,
            Hugo_Symbol VARCHAR,
            Chromosome VARCHAR,
            Start_Position INTEGER,
            End_Position INTEGER,
            Reference_Allele VARCHAR,
            Tumor_Seq_Allele1 VARCHAR,
            Mutation_Status VARCHAR,
            Variant_Classification VARCHAR
        )
    """)
    yield conn
    conn.close()


def _add_sample(db, sample_id, fga):
    db.execute(
        f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)',
        (sample_id, f"P_{sample_id}", fga)
    )


def _add_mutation(db, sample_id, chrom="17", start=7674220, end=7674221,
                  mutation_status=None, variant_class=None):
    db.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (sample_id, "TP53", chrom, start, end, "C", "T", mutation_status, variant_class)
    )


def test_empty_returns_sentinel(db):
    """No samples → returns the _EMPTY_SCATTER dict (bins=[], counts=0)."""
    result = get_tmb_fga_scatter(db, STUDY)
    assert result["bins"] == []
    assert result["count_max"] == 0


def test_null_fga_excluded(db):
    """Samples with NULL FGA are excluded."""
    _add_sample(db, "S1", None)
    _add_mutation(db, "S1")
    result = get_tmb_fga_scatter(db, STUDY)
    assert result["bins"] == []


def test_no_mutations_excluded(db):
    """Samples with no mutations (mutation_count=0) are excluded."""
    _add_sample(db, "S1", 0.5)
    # No mutations added for S1
    result = get_tmb_fga_scatter(db, STUDY)
    assert result["bins"] == []


def test_single_eligible_sample_produces_bin(db):
    """One sample with FGA=0.5 and 1 mutation produces exactly 1 bin."""
    _add_sample(db, "S1", 0.5)
    _add_mutation(db, "S1")
    result = get_tmb_fga_scatter(db, STUDY)
    assert len(result["bins"]) == 1
    assert result["bins"][0]["count"] == 1


def test_positive_correlation_sign(db):
    """With co-varying FGA and mutation counts, Pearson/Spearman should be positive."""
    for i in range(1, 6):
        _add_sample(db, f"S{i}", i * 0.15)
        for j in range(i * 2):
            # Different positions per mutation so COUNT DISTINCT works
            _add_mutation(db, f"S{i}", chrom="17", start=7674220 + j, end=7674221 + j)
    result = get_tmb_fga_scatter(db, STUDY)
    assert result["pearson_corr"] > 0
    assert result["spearman_corr"] > 0


def test_germline_excluded_from_mutation_count(db):
    """GERMLINE mutations are excluded from the mutation count."""
    _add_sample(db, "S1", 0.3)
    # Only GERMLINE mutation → mutation_count = 0 → excluded from scatter
    _add_mutation(db, "S1", mutation_status="GERMLINE")
    result = get_tmb_fga_scatter(db, STUDY)
    assert result["bins"] == []


def test_required_keys_present(db):
    """All expected keys are present in the response."""
    _add_sample(db, "S1", 0.3)
    _add_mutation(db, "S1")
    result = get_tmb_fga_scatter(db, STUDY)
    for key in ("bins", "x_bin_size", "y_bin_size", "count_min", "count_max",
                "pearson_corr", "pearson_pval", "spearman_corr", "spearman_pval"):
        assert key in result, f"Missing key: {key}"
