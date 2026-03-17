"""Unit tests for core/syncer.py using a mock CbioPortalClient and in-memory cache."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers to build an in-memory cache DB with the full sync schema
# ---------------------------------------------------------------------------

def _make_cache_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE cache_manifest (
            study_id VARCHAR,
            data_type VARCHAR,
            molecular_profile_id VARCHAR,
            fetched_at TIMESTAMP,
            PRIMARY KEY (study_id, data_type)
        );
        CREATE TABLE moalmanac_cache (
            variant_hash VARCHAR PRIMARY KEY,
            payload JSON,
            fetched_at TIMESTAMP
        );
        CREATE TABLE studies (
            study_id VARCHAR PRIMARY KEY,
            type_of_cancer VARCHAR,
            name VARCHAR,
            description VARCHAR,
            short_name VARCHAR,
            all_sample_count INTEGER,
            synced_at TIMESTAMP
        );
        CREATE TABLE clinical_attributes (
            study_id VARCHAR,
            attr_id VARCHAR,
            display_name VARCHAR,
            description VARCHAR,
            datatype VARCHAR,
            patient_attribute BOOLEAN,
            priority INTEGER,
            PRIMARY KEY (study_id, attr_id)
        );
        CREATE TABLE clinical_data (
            study_id VARCHAR,
            sample_id VARCHAR,
            patient_id VARCHAR,
            attr_id VARCHAR,
            value VARCHAR,
            patient_attribute BOOLEAN
        );
    """)
    return conn


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FAKE_STUDY = MagicMock()
FAKE_STUDY.studyId = "test_study"
FAKE_STUDY.model_dump.return_value = {
    "studyId": "test_study",
    "name": "Test Study",
    "description": "A test study",
    "cancerType": {"cancerTypeId": "brca", "name": "Breast Cancer"},
    "allSampleCount": 42,
    "shortName": "TS",
}

FAKE_ATTRS = [
    {
        "clinicalAttributeId": "AGE",
        "displayName": "Age",
        "description": "Age at diagnosis",
        "datatype": "NUMBER",
        "patientAttribute": True,
        "priority": "1",
    },
    {
        "clinicalAttributeId": "CANCER_TYPE",
        "displayName": "Cancer Type",
        "description": "Type of cancer",
        "datatype": "STRING",
        "patientAttribute": False,
        "priority": "3000",
    },
]

FAKE_CLINICAL_ROWS = [
    {
        "studyId": "test_study",
        "sampleId": "S001",
        "patientId": "P001",
        "clinicalAttributeId": "CANCER_TYPE",
        "value": "Breast",
        "clinicalDataType": "SAMPLE",
    },
    {
        "studyId": "test_study",
        "sampleId": None,
        "patientId": "P001",
        "clinicalAttributeId": "AGE",
        "value": "52",
        "clinicalDataType": "PATIENT",
    },
]


# ---------------------------------------------------------------------------
# Tests for cache helper functions
# ---------------------------------------------------------------------------

def test_upsert_studies_inserts_rows():
    from cbioportal.core.cache import upsert_studies

    conn = _make_cache_conn()
    upsert_studies(conn, [FAKE_STUDY.model_dump()])

    rows = conn.execute("SELECT study_id, type_of_cancer, all_sample_count FROM studies").fetchall()
    assert len(rows) == 1
    study_id, cancer, count = rows[0]
    assert study_id == "test_study"
    assert cancer == "brca"
    assert count == 42


def test_upsert_studies_updates_on_conflict():
    from cbioportal.core.cache import upsert_studies

    conn = _make_cache_conn()
    upsert_studies(conn, [FAKE_STUDY.model_dump()])

    updated = {**FAKE_STUDY.model_dump(), "allSampleCount": 99}
    upsert_studies(conn, [updated])

    count = conn.execute("SELECT all_sample_count FROM studies WHERE study_id = 'test_study'").fetchone()[0]
    assert count == 99


def test_upsert_clinical_attributes():
    from cbioportal.core.cache import upsert_clinical_attributes

    conn = _make_cache_conn()
    upsert_clinical_attributes(conn, "test_study", FAKE_ATTRS)

    rows = conn.execute(
        "SELECT attr_id, patient_attribute, priority FROM clinical_attributes WHERE study_id = 'test_study'"
    ).fetchall()
    assert len(rows) == 2
    by_id = {r[0]: r for r in rows}
    assert by_id["AGE"][1] is True
    assert by_id["AGE"][2] == 1
    assert by_id["CANCER_TYPE"][1] is False
    assert by_id["CANCER_TYPE"][2] == 3000


def test_upsert_clinical_data_inserts_and_replaces():
    from cbioportal.core.cache import upsert_clinical_data

    conn = _make_cache_conn()
    upsert_clinical_data(conn, "test_study", FAKE_CLINICAL_ROWS)

    count = conn.execute("SELECT count(*) FROM clinical_data WHERE study_id = 'test_study'").fetchone()[0]
    assert count == 2

    # Replace with a single row — old rows should be gone
    upsert_clinical_data(conn, "test_study", [FAKE_CLINICAL_ROWS[0]])
    count = conn.execute("SELECT count(*) FROM clinical_data WHERE study_id = 'test_study'").fetchone()[0]
    assert count == 1


def test_upsert_clinical_data_patient_attribute_flag():
    from cbioportal.core.cache import upsert_clinical_data

    conn = _make_cache_conn()
    upsert_clinical_data(conn, "test_study", FAKE_CLINICAL_ROWS)

    rows = conn.execute(
        "SELECT attr_id, patient_attribute FROM clinical_data WHERE study_id = 'test_study' ORDER BY attr_id"
    ).fetchall()
    by_attr = {r[0]: r[1] for r in rows}
    assert by_attr["AGE"] is True
    assert by_attr["CANCER_TYPE"] is False


# ---------------------------------------------------------------------------
# Tests for sync_all (mocked HTTP)
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


def test_sync_all_populates_cache(tmp_path, monkeypatch):
    """sync_all should insert studies and clinical data into the cache DB."""
    from cbioportal.core import syncer, cache

    # Point cache DB at a temp file
    cache_db = tmp_path / "cache.duckdb"
    monkeypatch.setattr(cache, "CACHE_DB_PATH", cache_db)
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)

    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: mock_client
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.fetch_all_studies.return_value = [FAKE_STUDY]
    mock_client.get_clinical_attributes.return_value = FAKE_ATTRS
    mock_client.get_clinical_data.return_value = FAKE_CLINICAL_ROWS

    messages = []

    with patch("cbioportal.core.api.client.CbioPortalClient", return_value=mock_client):
        stats = _run(syncer.sync_all(messages.append))

    assert stats["studies"] == 1
    assert stats["clinical_rows"] == 2

    conn = duckdb.connect(str(cache_db), read_only=True)
    study_count = conn.execute("SELECT count(*) FROM studies").fetchone()[0]
    attr_count = conn.execute("SELECT count(*) FROM clinical_attributes").fetchone()[0]
    data_count = conn.execute("SELECT count(*) FROM clinical_data").fetchone()[0]
    manifest = conn.execute(
        "SELECT study_id FROM cache_manifest WHERE data_type = 'clinical'"
    ).fetchone()
    conn.close()

    assert study_count == 1
    assert attr_count == 2
    assert data_count == 2
    assert manifest is not None
    assert manifest[0] == "test_study"


def test_sync_all_skips_fresh_cache(tmp_path, monkeypatch):
    """Studies already in the manifest within TTL should be skipped."""
    from cbioportal.core import syncer, cache

    cache_db = tmp_path / "cache.duckdb"
    monkeypatch.setattr(cache, "CACHE_DB_PATH", cache_db)
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)

    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: mock_client
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.fetch_all_studies.return_value = [FAKE_STUDY]
    mock_client.get_clinical_attributes.return_value = FAKE_ATTRS
    mock_client.get_clinical_data.return_value = FAKE_CLINICAL_ROWS

    with patch("cbioportal.core.api.client.CbioPortalClient", return_value=mock_client):
        # First sync — populates cache
        _run(syncer.sync_all(lambda _: None))
        # Second sync — should skip
        stats = _run(syncer.sync_all(lambda _: None))

    assert stats["studies"] == 1
    assert stats["clinical_rows"] == 0  # nothing fetched on second run
    assert mock_client.get_clinical_data.call_count == 1  # called only once
