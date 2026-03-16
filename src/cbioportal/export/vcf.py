"""VCF formatter (stub)."""
from __future__ import annotations

from cbioportal.core.api.models import Mutation

# ##fileformat=VCFv4.1 + ##INFO lines + #CHROM POS ID REF ALT QUAL FILTER INFO


def to_vcf(mutations: list[Mutation]) -> bytes:
    """Convert mutations to VCFv4.1 format bytes."""
    raise NotImplementedError
