"""Unit tests for cbioportal.cli.tools — the cli-textual agent tools."""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from cbioportal.cli.tools import (
    describe_study,
    gene_mutation_frequency,
    list_studies,
    validate_study_folder,
)
from cli_textual.tools.base import ToolResult


# ── helpers ─────────────────────────────────────────────────────────────────


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


@pytest.fixture
def seeded_db():
    """In-memory DuckDB with the minimal schema the cli tools touch.

    Provides one study (`tiny_study`) with two samples and three mutations
    (TP53 in both samples, KRAS in one sample).
    """
    conn = duckdb.connect(":memory:")
    sid = "tiny_study"
    conn.execute(
        """
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
        """
    )
    conn.execute(
        "INSERT INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (sid, "lung", "Tiny Lung Study", "desc", "tiny", True, None, None, None, "Lung"),
    )
    conn.execute(
        "CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR)"
    )
    conn.execute(
        "INSERT INTO study_data_types VALUES (?, 'mutation'), (?, 'cna')",
        (sid, sid),
    )
    conn.execute(f'CREATE TABLE "{sid}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(
        f'INSERT INTO "{sid}_sample" VALUES (?, ?), (?, ?)',
        ("S1", "P1", "S2", "P2"),
    )
    # Pre-computed derived table the gene-frequency tools read from
    conn.execute(
        f"""
        CREATE TABLE "{sid}_genomic_event_derived" (
            sample_id VARCHAR,
            hugo_symbol VARCHAR,
            variant_type VARCHAR,
            cna_type VARCHAR
        )
        """
    )
    conn.execute(
        f'INSERT INTO "{sid}_genomic_event_derived" VALUES '
        "(?, 'TP53', 'mutation', NULL),"
        "(?, 'TP53', 'mutation', NULL),"
        "(?, 'KRAS', 'mutation', NULL)",
        ("S1", "S2", "S1"),
    )
    conn.execute(
        f"""
        CREATE TABLE "{sid}_profiled_counts" (
            hugo_symbol VARCHAR,
            variant_type VARCHAR,
            n_profiled INTEGER
        )
        """
    )
    conn.execute(
        f'INSERT INTO "{sid}_profiled_counts" VALUES '
        "('TP53', 'mutation', 2),"
        "('KRAS', 'mutation', 2)"
    )
    # clinical_sample union view used by get_study_catalog
    conn.execute(
        f"""
        CREATE VIEW clinical_sample AS
        SELECT '{sid}' AS study_id, SAMPLE_ID FROM "{sid}_sample"
        """
    )
    yield conn
    conn.close()


@pytest.fixture
def patch_open_conn(seeded_db, monkeypatch):
    """Make tools.open_conn yield the seeded in-memory connection."""
    from contextlib import contextmanager

    from cbioportal.cli.tools import _db as db_helper

    @contextmanager
    def fake_open_conn(read_only: bool = True):
        yield seeded_db

    monkeypatch.setattr(db_helper, "open_conn", fake_open_conn)
    # Also patch the names already imported into the leaf modules
    import cbioportal.cli.tools.studies as studies_mod
    import cbioportal.cli.tools.gene_frequency as freq_mod

    monkeypatch.setattr(studies_mod, "open_conn", fake_open_conn)
    monkeypatch.setattr(freq_mod, "open_conn", fake_open_conn)
    yield


# ── list_studies ────────────────────────────────────────────────────────────


def test_list_studies_returns_table(patch_open_conn):
    result = _run(list_studies())
    assert isinstance(result, ToolResult)
    assert not result.is_error
    assert "tiny_study" in result.output
    assert "Tiny Lung Study" in result.output
    assert "1 studies." in result.output or "1 of 1" in result.output


def test_list_studies_filtered_by_data_type(patch_open_conn):
    result = _run(list_studies(data_type="mutation"))
    assert "tiny_study" in result.output

    result_none = _run(list_studies(data_type="mrna"))
    assert "tiny_study" not in result_none.output


# ── describe_study ──────────────────────────────────────────────────────────


def test_describe_study_known(patch_open_conn):
    result = _run(describe_study("tiny_study"))
    assert not result.is_error
    assert "Tiny Lung Study" in result.output
    assert "samples**: 2" in result.output


def test_describe_study_unknown(patch_open_conn):
    result = _run(describe_study("does_not_exist"))
    assert result.is_error


# ── gene_mutation_frequency (panel-aware) ───────────────────────────────────


def test_gene_mutation_frequency_uses_profiled_denominator(patch_open_conn):
    result = _run(gene_mutation_frequency("tiny_study"))
    assert not result.is_error
    # TP53 hit in 2/2 profiled samples → 100.0%
    assert "TP53" in result.output
    assert "100.0" in result.output
    # KRAS hit in 1/2 profiled samples → 50.0%
    assert "KRAS" in result.output
    assert "50.0" in result.output


def test_gene_mutation_frequency_filters_to_named_genes(patch_open_conn):
    result = _run(gene_mutation_frequency("tiny_study", genes="TP53"))
    assert "TP53" in result.output
    assert "KRAS" not in result.output


# ── validate_study_folder ───────────────────────────────────────────────────


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def test_validate_clean_minimal_study(tmp_path):
    study = tmp_path / "tiny"
    _write(
        study / "meta_study.txt",
        "type_of_cancer: lung\n"
        "cancer_study_identifier: tiny\n"
        "name: Tiny\n"
        "description: a tiny study\n",
    )
    _write(
        study / "data_clinical_sample.txt",
        "PATIENT_ID\tSAMPLE_ID\nP1\tS1\n",
    )
    _write(
        study / "data_clinical_patient.txt",
        "PATIENT_ID\nP1\n",
    )
    result = _run(validate_study_folder(str(study)))
    assert not result.is_error, result.output
    assert "0 error" in result.output


def test_validate_missing_meta_study(tmp_path):
    study = tmp_path / "broken"
    study.mkdir()
    result = _run(validate_study_folder(str(study)))
    assert result.is_error
    assert "meta_study.txt" in result.output


def test_validate_meta_missing_required_keys(tmp_path):
    study = tmp_path / "halfmeta"
    _write(
        study / "meta_study.txt",
        "type_of_cancer: lung\nname: Half\n",  # missing identifier + description
    )
    result = _run(validate_study_folder(str(study)))
    assert result.is_error
    assert "cancer_study_identifier" in result.output
    assert "description" in result.output


def test_validate_data_file_missing_required_column(tmp_path):
    study = tmp_path / "badmaf"
    _write(
        study / "meta_study.txt",
        "type_of_cancer: lung\n"
        "cancer_study_identifier: badmaf\n"
        "name: Bad MAF\n"
        "description: x\n",
    )
    # Missing Variant_Classification
    _write(
        study / "data_mutations.txt",
        "Hugo_Symbol\tTumor_Sample_Barcode\nTP53\tS1\n",
    )
    result = _run(validate_study_folder(str(study)))
    assert result.is_error
    assert "Variant_Classification" in result.output


def test_validate_path_does_not_exist(tmp_path):
    result = _run(validate_study_folder(str(tmp_path / "nope")))
    assert result.is_error
    assert "does not exist" in result.output
