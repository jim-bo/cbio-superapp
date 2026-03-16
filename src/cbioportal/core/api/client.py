"""CbioPortalClient — httpx-based client for cBioPortal REST API (stubbed methods)."""
from __future__ import annotations

import httpx

from cbioportal.core.cbio_config import get_config
from cbioportal.core.api.models import ClinicalRow, CnaSegment, Mutation, Study


class CbioPortalClient:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
    ) -> None:
        cfg = get_config()
        self.base_url = (base_url or cfg["portal"]["url"]).rstrip("/")
        self.token = token or cfg["portal"].get("token") or None
        self.http = httpx.Client(
            headers=self._build_headers(),
            timeout=30,
        )

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def close(self) -> None:
        self.http.close()

    def __enter__(self) -> "CbioPortalClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Stubbed API methods
    # ------------------------------------------------------------------

    def search_studies(
        self,
        query: str,
        cancer_type: str | None = None,
        min_samples: int | None = None,
    ) -> list[Study]:
        """Search cBioPortal studies by keyword."""
        raise NotImplementedError

    def get_study(self, study_id: str) -> Study:
        """Fetch a single study by ID."""
        raise NotImplementedError

    def get_mutations(
        self,
        study_id: str,
        sample_ids: list[str] | None = None,
    ) -> list[Mutation]:
        """Fetch all mutations for a study, optionally filtered by sample IDs."""
        raise NotImplementedError

    def get_cna_segments(self, study_id: str) -> list[CnaSegment]:
        """Fetch CNA segments for a study."""
        raise NotImplementedError

    def get_clinical_data(self, study_id: str) -> list[ClinicalRow]:
        """Fetch clinical data for a study."""
        raise NotImplementedError
