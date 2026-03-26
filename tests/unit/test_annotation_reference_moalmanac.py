"""Unit tests for annotation/reference/moalmanac.py — mocked HTTP."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from cbioportal.core.annotation.reference.moalmanac import (
    ensure_moalmanac,
    refresh_moalmanac,
)

SAMPLE_FEATURES = [
    {
        "feature_id": 1,
        "feature_type": "somatic_variant",
        "attributes": [{"gene": "KRAS", "protein_change": "p.G12D"}],
    },
    {
        "feature_id": 2,
        "feature_type": "Copy Number Alteration",
        "attributes": [{"gene": "ERBB2", "direction": "Amplification"}],
    },
    {
        "feature_id": 3,
        "feature_type": "fusion",
        "attributes": [{"gene": "ALK", "partner_gene": "EML4"}],
    },
]

SAMPLE_ASSERTIONS = [
    {
        "features": [{"feature_id": 1}],
        "predictive_implication": "FDA-Approved",
        "therapy_name": "Sotorasib",
        "oncotree_term": "LUAD",
        "score_bin": "Putatively Actionable",
        "oncogenic": "Oncogenic",
    }
]


def _mock_response(data):
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.json.return_value = data
    return m


@pytest.fixture
def cache_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_refresh_loads_features_and_assertions(cache_db):
    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.side_effect = [
            _mock_response(SAMPLE_FEATURES),
            _mock_response(SAMPLE_ASSERTIONS),
        ]
        refresh_moalmanac(cache_db)

    features = cache_db.execute(
        "SELECT gene, alteration, feature_type, alt_type FROM moalmanac_features_bulk"
    ).fetchall()
    assert len(features) == 3

    # KRAS G12D — somatic variant, alteration stripped of p.
    kras = next(f for f in features if f[0] == "KRAS")
    assert kras[1] == "G12D"
    assert kras[2] == "somatic_variant"

    # ERBB2 amplification — copy_number
    erbb2 = next(f for f in features if f[0] == "ERBB2")
    assert erbb2[2] == "copy_number"
    assert erbb2[3] == "Amplification"

    # ALK fusion
    alk = next(f for f in features if f[0] == "ALK")
    assert alk[2] == "fusion"

    # Assertions loaded
    assertions = cache_db.execute(
        "SELECT feature_id, clinical_significance, drug FROM moalmanac_assertions_bulk"
    ).fetchall()
    assert len(assertions) == 1
    assert assertions[0][2] == "Sotorasib"


def test_ensure_moalmanac_skips_if_fresh(cache_db):
    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.side_effect = [
            _mock_response(SAMPLE_FEATURES),
            _mock_response(SAMPLE_ASSERTIONS),
        ]
        refresh_moalmanac(cache_db)

    # Second call should not trigger HTTP again
    with patch("httpx.Client") as MockClient2:
        ensure_moalmanac(cache_db)
        MockClient2.assert_not_called()


def test_schema_migration_adds_columns_to_existing_table(cache_db):
    """Existing table without feature_type/alt_type gets migrated non-destructively."""
    cache_db.execute(
        "CREATE TABLE moalmanac_features_bulk (gene VARCHAR, alteration VARCHAR, feature_id INTEGER, payload JSON)"
    )
    cache_db.execute("INSERT INTO moalmanac_features_bulk VALUES ('BRAF', 'V600E', 99, '{}')")

    with patch("httpx.Client") as MockClient:
        client_instance = MockClient.return_value.__enter__.return_value
        client_instance.get.side_effect = [
            _mock_response([]),
            _mock_response([]),
        ]
        refresh_moalmanac(cache_db)

    cols = {
        row[0]
        for row in cache_db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'moalmanac_features_bulk'"
        ).fetchall()
    }
    assert "feature_type" in cols
    assert "alt_type" in cols
