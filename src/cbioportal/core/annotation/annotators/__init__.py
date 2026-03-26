"""Annotator functions for each alteration type."""
from .cna import annotate_cna
from .mutations import annotate_mutations
from .sv import annotate_sv

__all__ = ["annotate_mutations", "annotate_cna", "annotate_sv"]
