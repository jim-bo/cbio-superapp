"""Unit tests for annotation annotators — in-memory DuckDB + temp cache DB."""
from __future__ import annotations

import duckdb
import pytest

from cbioportal.core.annotation.annotators.mutations import (
    _resolve_mutation_effect,
    annotate_mutations,
)
from cbioportal.core.annotation.annotators.cna import annotate_cna
from cbioportal.core.annotation.annotators.sv import annotate_sv
from cbioportal.core.annotation.writer import write_variant_annotations

STUDY = "test_annotation"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def study_db():
    """In-memory study DuckDB with mutations, cna, and sv tables."""
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE "{STUDY}_mutations" (
            Hugo_Symbol VARCHAR,
            Tumor_Sample_Barcode VARCHAR,
            HGVSp_Short VARCHAR,
            Variant_Classification VARCHAR,
            Chromosome VARCHAR,
            Start_Position VARCHAR,
            Reference_Allele VARCHAR,
            Tumor_Seq_Allele2 VARCHAR,
            Mutation_Status VARCHAR
        )
    """)
    conn.execute(f"""
        INSERT INTO "{STUDY}_mutations" VALUES
        ('KRAS', 'S001', 'p.G12D', 'Missense_Mutation', '12', '25398284', 'C', 'A', 'Somatic'),
        ('TP53', 'S001', 'p.R175H', 'Missense_Mutation', '17', '7674220', 'G', 'A', 'Somatic'),
        ('BRAF', 'S002', 'p.V600E', 'Missense_Mutation', '7', '140453136', 'A', 'T', 'Somatic')
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_cna" (
            hugo_symbol VARCHAR,
            sample_id VARCHAR,
            cna_value FLOAT
        )
    """)
    conn.execute(f"""
        INSERT INTO "{STUDY}_cna" VALUES
        ('ERBB2', 'S001', 2),
        ('CDKN2A', 'S002', -2),
        ('MYC', 'S001', 1)
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sv" (
            Tumor_Sample_Barcode VARCHAR,
            Site1_Hugo_Symbol VARCHAR,
            Site2_Hugo_Symbol VARCHAR,
            Class VARCHAR
        )
    """)
    conn.execute(f"""
        INSERT INTO "{STUDY}_sv" VALUES
        ('S001', 'ALK', 'EML4', 'FUSION'),
        ('S002', 'BCR', 'ABL1', 'FUSION')
    """)
    yield conn
    conn.close()


@pytest.fixture
def cache_db(tmp_path):
    """Temp-file cache DuckDB seeded with reference tables."""
    cache_path = str(tmp_path / "cache.duckdb")
    conn = duckdb.connect(cache_path)
    # MOAlmanac features
    conn.execute("""
        CREATE TABLE moalmanac_features_bulk (
            gene VARCHAR, alteration VARCHAR, feature_id INTEGER,
            feature_type VARCHAR, alt_type VARCHAR, payload JSON
        )
    """)
    conn.executemany(
        "INSERT INTO moalmanac_features_bulk VALUES (?, ?, ?, ?, ?, '{}')",
        [
            ("KRAS", "G12D", 1, "somatic_variant", None),
            ("ERBB2", None, 2, "copy_number", "Amplification"),
            ("ALK", None, 3, "fusion", None),
        ],
    )
    # MOAlmanac assertions
    conn.execute("""
        CREATE TABLE moalmanac_assertions_bulk (
            feature_id INTEGER, clinical_significance VARCHAR, drug VARCHAR,
            disease VARCHAR, score_bin VARCHAR, oncogenic VARCHAR, payload JSON
        )
    """)
    conn.executemany(
        "INSERT INTO moalmanac_assertions_bulk VALUES (?, ?, ?, ?, ?, ?, '{}')",
        [
            (1, "FDA-Approved", "Sotorasib", "LUAD", "Putatively Actionable", "Oncogenic"),
            (2, "Guideline", "Trastuzumab", "BRCA", "Putatively Actionable", "Oncogenic"),
            (3, "Clinical trial", "Crizotinib", "LUAD", "Putatively Actionable", "Oncogenic"),
        ],
    )
    # CIViC
    conn.execute("""
        CREATE TABLE civic_evidence (
            evidence_id INTEGER, gene VARCHAR, variant_name VARCHAR,
            hgvsp_short VARCHAR, evidence_type VARCHAR, clinical_significance VARCHAR,
            evidence_level VARCHAR, drugs VARCHAR, disease VARCHAR, oncotree_code VARCHAR,
            fetched_at TIMESTAMP
        )
    """)
    conn.executemany(
        "INSERT INTO civic_evidence VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
        [
            (101, "TP53", "R175H", "p.R175H", "Functional", "Loss-of-function", "B", "", "Pan-cancer", ""),
        ],
    )
    # IntOGen
    conn.execute("""
        CREATE TABLE intogen_drivers (
            symbol VARCHAR, tumor_type VARCHAR, oncotree_code VARCHAR,
            role VARCHAR, methods VARCHAR, qvalue_combination DOUBLE, fetched_at TIMESTAMP
        )
    """)
    conn.executemany(
        "INSERT INTO intogen_drivers VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
        [
            ("KRAS", "LUAD", "LUAD", "Act", "OncodriveFML", 0.001),
            ("TP53", "LUAD", "LUAD", "LoF", "OncodriveFML", 0.001),
        ],
    )
    conn.close()
    return cache_path


# ── mutation_effect resolution ────────────────────────────────────────────────

@pytest.mark.parametrize("civic,intogen,expected_effect,expected_src", [
    ("Gain-of-function",  None,  "Gain-of-function", "civic"),
    ("Loss-of-function",  None,  "Loss-of-function", "civic"),
    ("Activating",        None,  "Gain-of-function", "civic"),
    ("Dominant Negative", None,  "Loss-of-function", "civic"),
    ("Neomorphic",        None,  "Loss-of-function", "civic"),
    (None,                "Act", "Gain-of-function", "intogen"),
    (None,                "LoF", "Loss-of-function", "intogen"),
    (None,                "Amb", "Unknown",           "intogen"),
    (None,                None,  "Unknown",           "unknown"),
    ("Loss-of-function",  "Act", "Loss-of-function", "civic"),  # CIViC takes precedence
])
def test_resolve_mutation_effect(civic, intogen, expected_effect, expected_src):
    effect, src = _resolve_mutation_effect(civic, intogen)
    assert effect == expected_effect
    assert src == expected_src


# ── annotate_mutations ────────────────────────────────────────────────────────

def test_annotate_mutations_happy_path(study_db, cache_db):
    rows = annotate_mutations(study_db, STUDY, cache_db, vep_lookup=None)
    assert len(rows) >= 3  # at least one row per mutation

    kras_rows = [r for r in rows if r["hugo_symbol"] == "KRAS" and r["hgvsp_short"] == "p.G12D"]
    assert kras_rows, "Expected KRAS G12D row"
    kras = kras_rows[0]
    assert kras["alteration_type"] == "MUTATION"
    assert kras["moalmanac_drug"] == "Sotorasib"
    # mutation_effect from IntOGen since CIViC has no Functional entry for KRAS
    assert kras["mutation_effect"] == "Gain-of-function"
    assert kras["mutation_effect_source"] == "intogen"

    tp53_rows = [r for r in rows if r["hugo_symbol"] == "TP53" and r["hgvsp_short"] == "p.R175H"]
    assert tp53_rows
    tp53 = tp53_rows[0]
    # CIViC Functional entry for TP53 R175H → civic source
    assert tp53["mutation_effect"] == "Loss-of-function"
    assert tp53["mutation_effect_source"] == "civic"
    assert tp53["civic_evidence_id"] == 101


def test_annotate_mutations_with_vep_lookup(study_db, cache_db):
    vep_lookup = {
        ("KRAS", "12", "25398284", "C", "A"): {
            "vep_impact": "MODERATE",        # derived from consequence
            "vep_consequence": "missense_variant",
            "vep_transcript_id": "ENST00000311936",
            "vep_exon_number": None,          # not in vibe-vep output
            "am_score": 0.92,
            "am_class": "likely_pathogenic",
            "hotspot_type": "single_residue",
        }
    }
    rows = annotate_mutations(study_db, STUDY, cache_db, vep_lookup=vep_lookup)
    kras = next((r for r in rows if r["hugo_symbol"] == "KRAS"), None)
    assert kras is not None
    assert kras["vep_impact"] == "MODERATE"
    assert kras["hotspot_type"] == "single_residue"
    assert kras["am_score"] == 0.92


def test_annotate_mutations_missing_table(cache_db):
    conn = duckdb.connect(":memory:")
    try:
        rows = annotate_mutations(conn, "nonexistent_study", cache_db)
        assert rows == []
    finally:
        conn.close()


def test_annotate_mutations_vep_null_when_no_lookup(study_db, cache_db):
    rows = annotate_mutations(study_db, STUDY, cache_db, vep_lookup=None)
    for row in rows:
        assert row["vep_impact"] is None
        assert row["hotspot_type"] is None


# ── annotate_cna ──────────────────────────────────────────────────────────────

def test_annotate_cna_happy_path(study_db, cache_db):
    rows = annotate_cna(study_db, STUDY, cache_db)
    # Only ±2 values (MYC = 1 excluded)
    symbols = {r["hugo_symbol"] for r in rows}
    assert "ERBB2" in symbols
    assert "CDKN2A" in symbols
    assert "MYC" not in symbols

    erbb2 = next(r for r in rows if r["hugo_symbol"] == "ERBB2")
    assert erbb2["alteration_type"] == "CNA"
    assert erbb2["cna_value"] == 2
    assert erbb2["moalmanac_drug"] == "Trastuzumab"


def test_annotate_cna_missing_table(cache_db):
    conn = duckdb.connect(":memory:")
    try:
        rows = annotate_cna(conn, "nonexistent_study", cache_db)
        assert rows == []
    finally:
        conn.close()


# ── annotate_sv ───────────────────────────────────────────────────────────────

def test_annotate_sv_happy_path(study_db, cache_db):
    rows = annotate_sv(study_db, STUDY, cache_db)
    symbols = {r["hugo_symbol"] for r in rows}
    assert "ALK" in symbols
    assert "EML4" in symbols

    alk = next(r for r in rows if r["hugo_symbol"] == "ALK")
    assert alk["alteration_type"] == "SV"
    assert alk["moalmanac_drug"] == "Crizotinib"
    assert alk["sv_partner_gene"] == "EML4"


def test_annotate_sv_missing_table(cache_db):
    conn = duckdb.connect(":memory:")
    try:
        rows = annotate_sv(conn, "nonexistent_study", cache_db)
        assert rows == []
    finally:
        conn.close()


# ── write_variant_annotations ────────────────────────────────────────────────

def test_writer_creates_table_and_inserts(study_db, cache_db):
    rows = annotate_mutations(study_db, STUDY, cache_db, vep_lookup=None)
    count = write_variant_annotations(study_db, STUDY, rows)
    assert count == len(rows)

    # Table exists and has correct schema
    cols = {
        row[0]
        for row in study_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ?",
            (f"{STUDY}_variant_annotations",),
        ).fetchall()
    }
    assert "hugo_symbol" in cols
    assert "moalmanac_drug" in cols
    assert "oncokb_oncogenic" in cols
    assert "annotated_at" in cols


def test_writer_rebuilds_on_second_call(study_db, cache_db):
    rows = annotate_mutations(study_db, STUDY, cache_db, vep_lookup=None)
    write_variant_annotations(study_db, STUDY, rows)
    # Second write should drop and rebuild
    write_variant_annotations(study_db, STUDY, rows[:1])
    count = study_db.execute(
        f'SELECT COUNT(*) FROM "{STUDY}_variant_annotations"'
    ).fetchone()[0]
    assert count == 1


def test_writer_handles_empty_rows(study_db):
    count = write_variant_annotations(study_db, STUDY, [])
    assert count == 0
