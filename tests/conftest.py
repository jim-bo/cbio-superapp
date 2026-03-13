"""pytest fixtures shared across the test suite."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

FIXTURES_DIR = Path(__file__).parent / "fixtures"
STUDY_ID = "msk_chord_2024"

# Maps clinicalAttributeId → display name for all 18 clinical charts under test.
# "Data Types" is excluded — it's not a standard clinical attribute.
CLINICAL_CHARTS: dict[str, str] = {
    "CANCER_TYPE": "Cancer Type",
    "CANCER_TYPE_DETAILED": "Cancer Type Detailed",
    "CLINICAL_GROUP": "Clinical Group",
    "CLINICAL_SUMMARY": "Clinical Summary",
    "DIAGNOSIS_DESCRIPTION": "Diagnosis Description",
    "ICD_O_HISTOLOGY_DESCRIPTION": "ICD-O Histology Description",
    "PATHOLOGICAL_GROUP": "Pathological Group",
    "OS_STATUS": "Overall Survival Status",
    "SAMPLE_TYPE": "Sample Type",
    "RACE": "Race",
    "GENDER": "Sex",
    "STAGE_HIGHEST_RECORDED": "Stage (Highest Recorded)",
    "ETHNICITY": "Ethnicity",
    "MSI_TYPE": "MSI Type",
    "GENE_PANEL": "Gene Panel",
    "SMOKING_PREDICTIONS_3_CLASSES": "Smoking History (NLP)",
    "SOMATIC_STATUS": "Somatic Status",
    "PRIOR_MED_TO_MSK": "Prior Treatment to MSK (NLP)",
}


# ---------------------------------------------------------------------------
# App / HTTP client
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client():
    """FastAPI TestClient wired to the real DuckDB database."""
    # Allow overriding DB path via environment variable (same as the app does)
    db_path = os.environ.get(
        "CBIO_DB_PATH",
        str(Path(__file__).parent.parent / "data" / "cbioportal.duckdb"),
    )
    os.environ.setdefault("CBIO_DB_PATH", db_path)

    from cbioportal.web.app import create_app
    app = create_app()
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Golden fixtures
# ---------------------------------------------------------------------------

def _load_fixture(name: str) -> dict:
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture not found: {path}. Run tests/capture_golden.py first.")
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def fixture_baseline() -> dict:
    return _load_fixture(f"{STUDY_ID}_baseline.json")


@pytest.fixture(scope="session")
def fixture_cancer_type() -> dict:
    return _load_fixture(f"{STUDY_ID}_cancer_type.json")


@pytest.fixture(scope="session")
def fixture_tp53_filter() -> dict:
    return _load_fixture(f"{STUDY_ID}_tp53_filter.json")


# ---------------------------------------------------------------------------
# Helper: post to a chart endpoint
# ---------------------------------------------------------------------------

def post_chart(client: TestClient, endpoint: str, study_id: str = STUDY_ID, filter_json: dict | None = None) -> list | dict:
    """POST to /study/summary/chart/<endpoint>?format=json and return parsed body."""
    data: dict = {"study_id": study_id}
    if filter_json is not None:
        data["filter_json"] = json.dumps(filter_json)
    resp = client.post(f"/study/summary/chart/{endpoint}", data=data, params={"format": "json"})
    resp.raise_for_status()
    return resp.json()
