"""OncoKB annotation client (stub)."""
from __future__ import annotations

from cbioportal.core.api.models import Mutation

ONCOKB_API = "https://www.oncokb.org/api/v1"


def annotate_mutations(mutations: list[Mutation], token: str) -> list[Mutation]:
    """POST to OncoKB; adds oncogenic, mutation_effect, highest_level fields."""
    raise NotImplementedError
