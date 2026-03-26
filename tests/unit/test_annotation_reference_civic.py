"""Unit tests for annotation/reference/civic.py — mocked HTTP."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from cbioportal.core.annotation.reference.civic import (
    _normalize_hgvsp,
    ensure_civic,
    refresh_civic,
)

SAMPLE_TSV = """\
molecular_profile\tmolecular_profile_id\tdisease\tdoid\ttherapies\tevidence_type\tevidence_level\tsignificance\tevidence_id
BRAF V600E\t1\tMelanoma\t8923\tVemurafenib\tPredictive\tA\tSensitivity/Response\t1
KRAS Gly12Asp\t2\tLUAD\t3908\tErlotinib\tPredictive\tB\tResistance\t2
TP53 p.R175H\t3\tPan-cancer\t\t\tFunctional\tC\tLoss-of-function\t3
"""


def _mock_response(text):
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.text = text
    return m


@pytest.fixture
def cache_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


# ── _normalize_hgvsp tests ────────────────────────────────────────────────────

@pytest.mark.parametrize("inp,expected", [
    ("V600E",     "p.V600E"),
    ("Val600Glu", "p.V600E"),
    ("p.V600E",   "p.V600E"),
    ("Gly12Asp",  "p.G12D"),
    ("G12D",      "p.G12D"),
    ("p.R175H",   "p.R175H"),
    ("",          None),
    (None,        None),
    ("foobar",    None),
])
def test_normalize_hgvsp(inp, expected):
    assert _normalize_hgvsp(inp) == expected


def test_refresh_civic_loads_records(cache_db):
    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.return_value = _mock_response(SAMPLE_TSV)
        refresh_civic(cache_db)

    rows = cache_db.execute(
        "SELECT evidence_id, gene, hgvsp_short FROM civic_evidence ORDER BY evidence_id"
    ).fetchall()
    assert len(rows) == 3

    # BRAF V600E normalized
    assert rows[0] == (1, "BRAF", "p.V600E")
    # KRAS Gly12Asp → p.G12D
    assert rows[1] == (2, "KRAS", "p.G12D")
    # TP53 p.R175H
    assert rows[2] == (3, "TP53", "p.R175H")


def test_parse_molecular_profile():
    from cbioportal.core.annotation.reference.civic import _parse_molecular_profile
    assert _parse_molecular_profile("BRAF V600E") == ("BRAF", "p.V600E")
    assert _parse_molecular_profile("JAK2 V617F") == ("JAK2", "p.V617F")
    assert _parse_molecular_profile("TP53 R175H") == ("TP53", "p.R175H")
    assert _parse_molecular_profile("MYC AMPLIFICATION") == ("MYC", None)
    assert _parse_molecular_profile("BCR::ABL1 e13a2") == ("", None)  # fusion
    assert _parse_molecular_profile("") == ("", None)


def test_ensure_civic_skips_if_fresh(cache_db):
    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.return_value = _mock_response(SAMPLE_TSV)
        refresh_civic(cache_db)

    with patch("httpx.Client") as MockClient2:
        ensure_civic(cache_db)
        MockClient2.assert_not_called()


def test_refresh_civic_handles_empty_tsv(cache_db):
    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.return_value = _mock_response("molecular_profile\tevidence_id\n")
        refresh_civic(cache_db)

    count = cache_db.execute("SELECT COUNT(*) FROM civic_evidence").fetchone()[0]
    assert count == 0
