"""Unit tests for annotation/vep — mocked subprocess."""
from __future__ import annotations

import csv
import io
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from cbioportal.core.annotation.vep.maf_io import parse_vep_output
from cbioportal.core.annotation.vep.runner import (
    VepNotAvailableError,
    VepRuntimeError,
    is_vep_available,
    run_vep,
)


# ── runner tests ──────────────────────────────────────────────────────────────

def test_is_vep_available_false(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _: None)
    assert is_vep_available() is False


def test_is_vep_available_true(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _: "/usr/local/bin/vibe-vep")
    assert is_vep_available() is True


def test_run_vep_raises_when_not_available(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _: None)
    with pytest.raises(VepNotAvailableError):
        run_vep(tmp_path / "in.maf", tmp_path / "out.maf")


def test_run_vep_raises_on_nonzero_exit(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _: "/usr/local/bin/vibe-vep")
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error: transcripts not downloaded"
    with patch("subprocess.run", return_value=mock_result):
        with pytest.raises(VepRuntimeError):
            run_vep(tmp_path / "in.maf", tmp_path / "out.maf")


def test_run_vep_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _: "/usr/local/bin/vibe-vep")
    mock_result = MagicMock()
    mock_result.returncode = 0
    with patch("subprocess.run", return_value=mock_result):
        run_vep(tmp_path / "in.maf", tmp_path / "out.maf")  # no exception


# ── parse_vep_output tests ────────────────────────────────────────────────────

def _write_vep_maf(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def test_parse_vep_output_basic(tmp_path):
    """vibe-vep appends vibe.* columns; IMPACT is derived from consequence."""
    maf_path = tmp_path / "out.maf"
    _write_vep_maf(maf_path, [
        {
            "Hugo_Symbol": "KRAS",
            "Chromosome": "12",
            "Start_Position": "25398284",
            "Reference_Allele": "C",
            "Tumor_Seq_Allele2": "A",
            "vibe.consequence": "missense_variant",
            "vibe.transcript_id": "ENST00000311936",
            "vibe.am_score": "0.85",
            "vibe.am_class": "likely_pathogenic",
            "vibe.hotspot_type": "single_residue",
        }
    ])

    lookup = parse_vep_output(maf_path)
    key = ("KRAS", "12", "25398284", "C", "A")
    assert key in lookup
    ann = lookup[key]
    assert ann["vep_consequence"] == "missense_variant"
    assert ann["vep_impact"] == "MODERATE"   # derived from missense_variant
    assert ann["vep_transcript_id"] == "ENST00000311936"
    assert ann["am_score"] == 0.85
    assert ann["hotspot_type"] == "single_residue"


def test_parse_vep_output_empty_file(tmp_path):
    maf_path = tmp_path / "empty.maf"
    maf_path.write_text("")
    lookup = parse_vep_output(maf_path)
    assert lookup == {}


def test_parse_vep_output_null_values_skipped(tmp_path):
    maf_path = tmp_path / "out.maf"
    _write_vep_maf(maf_path, [
        {
            "Hugo_Symbol": "TP53",
            "Chromosome": "17",
            "Start_Position": "7674220",
            "Reference_Allele": "G",
            "Tumor_Seq_Allele2": "A",
            "vibe.consequence": "missense_variant",
            "vibe.transcript_id": ".",
            "vibe.am_score": "",
            "vibe.am_class": "N/A",
            "vibe.hotspot_type": "",
        }
    ])
    lookup = parse_vep_output(maf_path)
    ann = lookup[("TP53", "17", "7674220", "G", "A")]
    assert ann["vep_transcript_id"] is None
    assert ann["am_score"] is None
    assert ann["am_class"] is None
    assert ann["hotspot_type"] is None
    # consequence + derived impact should still be set
    assert ann["vep_consequence"] == "missense_variant"
    assert ann["vep_impact"] == "MODERATE"
