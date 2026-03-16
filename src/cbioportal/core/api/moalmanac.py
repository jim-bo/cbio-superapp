"""MOAlmanac annotation client (stub)."""
from __future__ import annotations

from cbioportal.core.api.models import Mutation

MOALMANAC_API = "https://moalmanac.org/api"


def annotate_mutations(mutations: list[Mutation]) -> list[Mutation]:
    """POST to MOAlmanac; adds therapeutic_sensitivity/resistance fields."""
    raise NotImplementedError
