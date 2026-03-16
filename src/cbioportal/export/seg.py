"""SEG formatter (stub)."""
from __future__ import annotations

from cbioportal.core.api.models import CnaSegment

# UCSC segmentation format: ID, chrom, loc.start, loc.end, num.mark, seg.mean
SEG_COLUMNS = ["ID", "chrom", "loc.start", "loc.end", "num.mark", "seg.mean"]


def to_seg(segments: list[CnaSegment]) -> bytes:
    """Convert CNA segments to UCSC SEG format bytes."""
    raise NotImplementedError
