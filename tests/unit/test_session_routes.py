"""Route-level tests for the session service API.

Uses FastAPI TestClient with an in-memory SQLite sessions DB. The DuckDB
lifespan is patched so tests don't need a real data file.
"""
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cbioportal.core.session_repository import Base
from cbioportal.web.app import create_app


@pytest.fixture
def client():
    # StaticPool ensures all connections share the same in-memory SQLite DB.
    # Without it each new connection gets a fresh (empty) in-memory database.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    # Patch the DuckDB parts of lifespan so TestClient starts cleanly
    mock_conn = MagicMock()
    with (
        patch("cbioportal.web.app.get_connection", return_value=mock_conn),
        patch("cbioportal.web.app.load_study_names", return_value={}),
        # Use in-memory sessions DB instead of file-based SQLite
        patch("cbioportal.web.app.make_engine", return_value=engine),
        patch("cbioportal.web.app.sessionmaker", return_value=factory),
    ):
        app = create_app()
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


def test_create_session_returns_id_and_checksum(client):
    resp = client.post(
        "/api/session/main_session",
        json={"query": "TP53", "studyIds": ["msk_chord_2024"]},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert "id" in body
    assert "checksum" in body
    assert len(body["id"]) == 36  # UUID


def test_create_unknown_type_returns_400(client):
    resp = client.post("/api/session/banana", json={})
    assert resp.status_code == 400


def test_create_sets_cookie(client):
    resp = client.post("/api/session/main_session", json={})
    assert resp.status_code == 201
    assert "cbio_session_token" in resp.cookies


def test_cookie_reused_across_requests(client):
    r1 = client.post("/api/session/virtual_study", json={"name": "s1"})
    r2 = client.post("/api/session/virtual_study", json={"name": "s2"})
    # Both use the same cookie jar — both should list under that token
    listing = client.get("/api/session/virtual_study")
    assert listing.status_code == 200
    names = [item["data"]["name"] for item in listing.json()]
    assert "s1" in names and "s2" in names


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


def test_get_session_roundtrip(client):
    payload = {"genes": ["TP53", "KRAS"]}
    create_resp = client.post("/api/session/main_session", json=payload)
    session_id = create_resp.json()["id"]

    get_resp = client.get(f"/api/session/main_session/{session_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["data"] == payload


def test_get_nonexistent_session_returns_404(client):
    resp = client.get("/api/session/main_session/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


def test_get_wrong_type_returns_404(client):
    create_resp = client.post("/api/session/main_session", json={})
    sid = create_resp.json()["id"]
    resp = client.get(f"/api/session/virtual_study/{sid}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


def test_list_sessions(client):
    client.post("/api/session/virtual_study", json={"name": "a"})
    client.post("/api/session/virtual_study", json={"name": "b"})
    resp = client.get("/api/session/virtual_study")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


def test_list_unknown_type_returns_400(client):
    resp = client.get("/api/session/banana")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


def test_delete_own_session(client):
    create_resp = client.post("/api/session/virtual_study", json={})
    sid = create_resp.json()["id"]

    del_resp = client.delete(f"/api/session/virtual_study/{sid}")
    assert del_resp.status_code == 204

    get_resp = client.get(f"/api/session/virtual_study/{sid}")
    assert get_resp.status_code == 404


def test_delete_foreign_session_returns_404(client):
    # Create with first client (gets a cookie)
    create_resp = client.post("/api/session/virtual_study", json={})
    sid = create_resp.json()["id"]

    # Delete with a different cookie (simulate different user)
    resp = client.delete(
        f"/api/session/virtual_study/{sid}",
        cookies={"cbio_session_token": "different-token"},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Settings upsert / fetch
# ---------------------------------------------------------------------------


def test_settings_save_and_fetch(client):
    save_resp = client.post(
        "/api/session/settings",
        json={
            "page": "study_view",
            "origin": ["msk_chord_2024"],
            "filters": {"clinicalDataFilters": [], "mutationFilter": {"genes": ["TP53"]}},
        },
    )
    assert save_resp.status_code == 201
    sid = save_resp.json()["id"]

    fetch_resp = client.post(
        "/api/session/settings/fetch",
        json={"page": "study_view", "origin": ["msk_chord_2024"]},
    )
    assert fetch_resp.status_code == 200
    data = fetch_resp.json()["data"]
    assert data["filters"]["mutationFilter"]["genes"] == ["TP53"]


def test_settings_upsert_is_idempotent(client):
    for i in range(3):
        client.post(
            "/api/session/settings",
            json={"page": "study_view", "origin": ["msk_chord_2024"], "filters": {"v": i}},
        )
    # Should still be one settings row
    listing = client.get("/api/session/settings")
    assert len(listing.json()) == 1
    assert listing.json()[0]["data"]["filters"]["v"] == 2


def test_settings_fetch_missing_returns_404(client):
    resp = client.post(
        "/api/session/settings/fetch",
        json={"page": "study_view", "origin": ["no-such-study"]},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Virtual study shortcuts
# ---------------------------------------------------------------------------


def test_virtual_study_save_and_list(client):
    save_resp = client.post(
        "/api/session/virtual_study/save",
        json={"name": "My Cohort", "studies": ["msk_chord_2024"], "origin": ["msk_chord_2024"]},
    )
    assert save_resp.status_code == 201

    list_resp = client.get("/api/session/virtual_study")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1
    assert list_resp.json()[0]["data"]["name"] == "My Cohort"


# ---------------------------------------------------------------------------
# Share redirect
# ---------------------------------------------------------------------------


def test_share_redirect_settings(client):
    save_resp = client.post(
        "/api/session/settings",
        json={"page": "study_view", "origin": ["msk_chord_2024"]},
    )
    sid = save_resp.json()["id"]

    resp = client.get(f"/api/session/share/{sid}", follow_redirects=False)
    assert resp.status_code in (301, 302, 307, 308)
    assert "msk_chord_2024" in resp.headers["location"]
    assert sid in resp.headers["location"]


def test_share_redirect_nonexistent_returns_404(client):
    resp = client.get("/api/session/share/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404
