"""Unit tests for session_repository — uses in-memory SQLite, no file I/O."""
import hashlib
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cbioportal.core.session_repository import (
    Base,
    SessionRecord,
    _hash_token,
    create_session,
    delete_session,
    fetch_settings,
    get_session,
    list_sessions,
    upsert_settings,
)


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


TOKEN_A = "token-aaa"
TOKEN_B = "token-bbb"


# ---------------------------------------------------------------------------
# create + get
# ---------------------------------------------------------------------------


def test_create_and_retrieve(db):
    payload = {"name": "my study", "samples": ["s1", "s2"]}
    record = create_session(db, "virtual_study", payload, TOKEN_A)

    assert record.id is not None
    assert len(record.id) == 36  # UUID v4

    fetched = get_session(db, record.id)
    assert fetched is not None
    assert fetched.data["name"] == "my study"
    assert fetched.data["samples"] == ["s1", "s2"]


def test_get_nonexistent_returns_none(db):
    assert get_session(db, "00000000-0000-0000-0000-000000000000") is None


def test_create_sets_type(db):
    record = create_session(db, "custom_gene_list", {"geneList": ["TP53"]}, TOKEN_A)
    assert record.type == "custom_gene_list"


# ---------------------------------------------------------------------------
# owner_token storage
# ---------------------------------------------------------------------------


def test_owner_token_is_stored_as_hash(db):
    raw = "my-secret-token"
    record = create_session(db, "main_session", {}, raw)
    expected_hash = hashlib.sha256(raw.encode()).hexdigest()
    assert record.owner_token == expected_hash
    assert record.owner_token != raw


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_scoped_to_owner(db):
    create_session(db, "virtual_study", {"name": "A"}, TOKEN_A)
    create_session(db, "virtual_study", {"name": "B"}, TOKEN_A)
    create_session(db, "virtual_study", {"name": "C"}, TOKEN_B)

    results_a = list_sessions(db, "virtual_study", TOKEN_A)
    results_b = list_sessions(db, "virtual_study", TOKEN_B)

    assert len(results_a) == 2
    assert len(results_b) == 1
    assert results_b[0].data["name"] == "C"


def test_list_empty_returns_empty_list(db):
    assert list_sessions(db, "virtual_study", "unknown-token") == []


def test_list_scoped_to_type(db):
    create_session(db, "virtual_study", {}, TOKEN_A)
    create_session(db, "main_session", {}, TOKEN_A)

    vs = list_sessions(db, "virtual_study", TOKEN_A)
    ms = list_sessions(db, "main_session", TOKEN_A)

    assert len(vs) == 1
    assert len(ms) == 1


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_own_session(db):
    record = create_session(db, "virtual_study", {}, TOKEN_A)
    assert delete_session(db, record.id, TOKEN_A) is True
    assert get_session(db, record.id) is None


def test_delete_foreign_session_returns_false(db):
    record = create_session(db, "virtual_study", {}, TOKEN_A)
    assert delete_session(db, record.id, TOKEN_B) is False
    assert get_session(db, record.id) is not None


def test_delete_nonexistent_returns_false(db):
    assert delete_session(db, "no-such-id", TOKEN_A) is False


# ---------------------------------------------------------------------------
# checksum
# ---------------------------------------------------------------------------


def test_checksum_is_deterministic(db):
    data = {"x": 1, "y": [2, 3]}
    r1 = create_session(db, "main_session", data, TOKEN_A)
    r2 = create_session(db, "main_session", data, TOKEN_B)
    assert r1.checksum == r2.checksum


def test_checksum_changes_on_update(db):
    record = create_session(db, "settings", {"filters": {}}, TOKEN_A)
    original_checksum = record.checksum

    updated = upsert_settings(
        db, "study_view", ["msk_chord_2024"], {"filters": {"mutationFilter": {"genes": ["TP53"]}}}, TOKEN_A
    )
    assert updated.checksum != original_checksum


# ---------------------------------------------------------------------------
# upsert_settings
# ---------------------------------------------------------------------------


def test_upsert_settings_creates_new_row(db):
    record = upsert_settings(
        db, "study_view", ["msk_chord_2024"], {"filters": {}}, TOKEN_A
    )
    assert record.id is not None
    assert record.type == "settings"
    assert list_sessions(db, "settings", TOKEN_A) != []


def test_upsert_settings_updates_existing_row(db):
    upsert_settings(db, "study_view", ["msk_chord_2024"], {"filters": {}}, TOKEN_A)
    upsert_settings(db, "study_view", ["msk_chord_2024"], {"filters": {"x": 1}}, TOKEN_A)

    # Should still be one row
    rows = list_sessions(db, "settings", TOKEN_A)
    assert len(rows) == 1
    assert rows[0].data["filters"] == {"x": 1}


def test_upsert_different_origins_create_separate_rows(db):
    upsert_settings(db, "study_view", ["study_a"], {"filters": {}}, TOKEN_A)
    upsert_settings(db, "study_view", ["study_b"], {"filters": {}}, TOKEN_A)

    rows = list_sessions(db, "settings", TOKEN_A)
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# fetch_settings
# ---------------------------------------------------------------------------


def test_fetch_settings_returns_correct_row(db):
    upsert_settings(db, "study_view", ["msk_chord_2024"], {"filters": {"x": 99}}, TOKEN_A)
    record = fetch_settings(db, "study_view", ["msk_chord_2024"], TOKEN_A)
    assert record is not None
    assert record.data["filters"] == {"x": 99}


def test_fetch_settings_origin_order_independent(db):
    upsert_settings(db, "study_view", ["study_a", "study_b"], {"filters": {}}, TOKEN_A)

    # Query with reversed order should find the same row
    record = fetch_settings(db, "study_view", ["study_b", "study_a"], TOKEN_A)
    assert record is not None


def test_fetch_settings_wrong_owner_returns_none(db):
    upsert_settings(db, "study_view", ["msk_chord_2024"], {"filters": {}}, TOKEN_A)
    assert fetch_settings(db, "study_view", ["msk_chord_2024"], TOKEN_B) is None


def test_fetch_settings_missing_returns_none(db):
    assert fetch_settings(db, "study_view", ["nonexistent"], TOKEN_A) is None
