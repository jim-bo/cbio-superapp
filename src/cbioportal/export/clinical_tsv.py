"""Clinical TSV formatter (stub)."""
from __future__ import annotations

from cbioportal.core.api.models import ClinicalRow

# cBioPortal 5-row header format:
#   #display_names, #descriptions, #datatypes, #priority, col_ids


def to_clinical_tsv(rows: list[ClinicalRow]) -> bytes:
    """Convert clinical rows to cBioPortal 5-row-header TSV format bytes."""
    raise NotImplementedError
