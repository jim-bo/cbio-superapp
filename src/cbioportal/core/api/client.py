"""CbioPortalClient — httpx-based client for cBioPortal REST API (stubbed methods)."""
from __future__ import annotations

import httpx

from cbioportal.core.cbio_config import get_config
from cbioportal.core.api.models import ClinicalRow, CnaSegment, Mutation, Study
from cbioportal.core.api import study_cache


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

    def fetch_all_studies(self) -> list[Study]:
        """Fetch the complete study list from the portal (for cache population)."""
        r = self.http.get(
            f"{self.base_url}/api/studies",
            params={"pageSize": 500, "sortBy": "studyId", "direction": "ASC", "projection": "DETAILED"},
        )
        r.raise_for_status()
        return [Study.model_validate(s) for s in r.json()]

    def search_studies(
        self,
        query: str,
        cancer_type: str | None = None,
        min_samples: int | None = None,
    ) -> list[Study]:
        """Search studies via local cache (fetches from API on cache miss/expiry)."""
        cfg = get_config()
        ttl = int(cfg.get("cache", {}).get("ttl_days", 180))
        studies = study_cache.load(self.base_url, ttl)
        if studies is None:
            studies = self.fetch_all_studies()
            study_cache.save(self.base_url, studies)
        return study_cache.search(studies, query, cancer_type, min_samples)

    def get_study(self, study_id: str) -> Study:
        """Fetch a single study by ID."""
        r = self.http.get(f"{self.base_url}/api/studies/{study_id}")
        r.raise_for_status()
        return Study.model_validate(r.json())

    def get_mutation_profile_id(self, study_id: str) -> str | None:
        """Find the molecular profile ID for extended mutations for a study."""
        r = self.http.get(f"{self.base_url}/api/studies/{study_id}/molecular-profiles")
        r.raise_for_status()
        profiles = r.json()
        for p in profiles:
            if p.get("molecularAlterationType") == "MUTATION_EXTENDED":
                return p.get("molecularProfileId")
        return None

    def get_default_sample_list_id(self, study_id: str) -> str | None:
        """Find the 'all samples' list ID for a study."""
        r = self.http.get(f"{self.base_url}/api/studies/{study_id}/sample-lists")
        r.raise_for_status()
        lists = r.json()
        for l in lists:
            if l.get("category") == "all_cases_in_study":
                return l.get("sampleListId")
        # Fallback to the first list if none match exactly
        return lists[0].get("sampleListId") if lists else None

    def get_mutations_raw(self, molecular_profile_id: str, sample_list_id: str) -> list[dict]:
        """Fetch all raw mutations for a given molecular profile and sample list with pagination."""
        all_mutations = []
        page_number = 0
        page_size = 20000  # Safer limit for cBioPortal API
        
        while True:
            r = self.http.post(
                f"{self.base_url}/api/molecular-profiles/{molecular_profile_id}/mutations/fetch",
                params={
                    "pageNumber": page_number,
                    "pageSize": page_size,
                    "projection": "DETAILED"
                },
                json={"sampleListId": sample_list_id}
            )
            r.raise_for_status()
            data = r.json()
            
            if not data:
                break
                
            all_mutations.extend(data)
            
            # If we got less than page_size, we reached the end
            if len(data) < page_size:
                break
                
            page_number += 1
            
        return all_mutations

    def get_clinical_data_raw(self, study_id: str, attribute_id: str, data_type: str = "SAMPLE") -> list[dict]:
        """Fetch clinical data for a specific attribute (e.g., ONCOTREE_CODE) across the entire study."""
        all_data = []
        page_number = 0
        page_size = 20000
        
        while True:
            r = self.http.get(
                f"{self.base_url}/api/studies/{study_id}/clinical-data",
                params={
                    "clinicalDataType": data_type,
                    "attributeId": attribute_id,
                    "pageNumber": page_number,
                    "pageSize": page_size,
                }
            )
            r.raise_for_status()
            data = r.json()
            
            if not data:
                break
                
            all_data.extend(data)
            
            if len(data) < page_size:
                break
                
            page_number += 1
            
        return all_data

    def get_clinical_attributes(self, study_id: str) -> list[dict]:
        """Fetch clinical attribute definitions for a study."""
        r = self.http.get(
            f"{self.base_url}/api/studies/{study_id}/clinical-attributes",
            params={"projection": "SUMMARY", "pageSize": 1000},
        )
        r.raise_for_status()
        return r.json()

    def get_clinical_data(self, study_id: str) -> list[dict]:
        """Fetch all clinical data (SAMPLE + PATIENT) for a study."""
        all_rows: list[dict] = []
        for data_type in ("SAMPLE", "PATIENT"):
            page = 0
            while True:
                r = self.http.get(
                    f"{self.base_url}/api/studies/{study_id}/clinical-data",
                    params={
                        "clinicalDataType": data_type,
                        "projection": "SUMMARY",
                        "pageSize": 50000,
                        "pageNumber": page,
                    },
                )
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                all_rows.extend(data)
                if len(data) < 50000:
                    break
                page += 1
        return all_rows

    def get_cna_segments(self, study_id: str) -> list[CnaSegment]:
        """Fetch CNA segments for a study."""
        raise NotImplementedError
