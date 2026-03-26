"""Unit tests for annotation/reference/intogen.py — mocked HTTP."""
from __future__ import annotations

import io
import zipfile
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from cbioportal.core.annotation.reference.intogen import (
    _map_tumor_type,
    ensure_intogen,
    refresh_intogen,
)

SAMPLE_TSV = (
    "SYMBOL\tTUMOR_TYPE\tROLE\tMETHODS\tQVALUE_COMBINATION\n"
    "KRAS\tLUAD\tAct\tOncodriveFML,MutPanning\t0.001\n"
    "TP53\tLUAD\tLoF\tOncodriveFML\t0.0001\n"
    "EGFR\tLAML\tAct\tOncodriveFML\t0.01\n"
)


def _make_zip(tsv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("intogen_drivers-2023.1/Compendium_Cancer_Genes.tsv", tsv_content)
    return buf.getvalue()


def _mock_response(zip_bytes):
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.content = zip_bytes
    return m


@pytest.fixture
def cache_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.mark.parametrize("intogen_type,expected", [
    ("LAML", "AML"),
    ("LIHC", "HCC"),
    ("DLBC", "DLBCL"),
    ("LUAD", "LUAD"),  # identity mapping
    ("UNKNOWN_TYPE", "UNKNOWN_TYPE"),  # passthrough
])
def test_map_tumor_type(intogen_type, expected):
    assert _map_tumor_type(intogen_type) == expected


def test_refresh_intogen_loads_drivers(cache_db):
    zip_bytes = _make_zip(SAMPLE_TSV)

    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.return_value = _mock_response(zip_bytes)
        refresh_intogen(cache_db)

    rows = cache_db.execute(
        "SELECT symbol, tumor_type, oncotree_code, role FROM intogen_drivers ORDER BY symbol"
    ).fetchall()
    assert len(rows) == 3

    egfr = next(r for r in rows if r[0] == "EGFR")
    assert egfr[2] == "AML"   # LAML → AML
    assert egfr[3] == "Act"

    tp53 = next(r for r in rows if r[0] == "TP53")
    assert tp53[3] == "LoF"


def test_ensure_intogen_skips_if_fresh(cache_db):
    zip_bytes = _make_zip(SAMPLE_TSV)

    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.return_value = _mock_response(zip_bytes)
        refresh_intogen(cache_db)

    with patch("httpx.Client") as MockClient2:
        ensure_intogen(cache_db)
        MockClient2.assert_not_called()
