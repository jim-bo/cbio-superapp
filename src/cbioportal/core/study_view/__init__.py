"""Study View repository — public API.

Re-exports all public query functions so callers can import from the package root
exactly as they did from the old study_view_repository.py module:

    from cbioportal.core.study_view import get_study_metadata, get_mutated_genes, ...

Internal modules:
    filters    — _build_filter_subquery(), get_clinical_attributes()
    colors     — get_value_color(), CBIOPORTAL_D3_COLORS, RESERVED_COLORS
    clinical   — get_clinical_counts(), get_clinical_data_table()
    genomic    — get_mutated_genes(), get_cna_genes(), get_sv_genes(), get_age_histogram()
    survival   — get_km_data(), compute_km_curve(), get_tmb_fga_scatter()
    treatments — get_patient_treatment_counts(), get_sample_treatment_counts()
    meta       — get_study_metadata(), get_charts_meta(), get_data_types_chart()
"""
from .filters import _build_filter_subquery, get_clinical_attributes
from .colors import get_value_color, CBIOPORTAL_D3_COLORS, RESERVED_COLORS
from .clinical import get_clinical_counts, get_all_clinical_counts, get_clinical_data_table, get_numeric_histogram
from .genomic import (
    get_mutated_genes,
    get_cna_genes,
    get_sv_genes,
    get_data_types,
    get_age_histogram,
    _get_panel_availability,
)
from .survival import get_km_data, compute_km_curve, get_tmb_fga_scatter
from .treatments import get_patient_treatment_counts, get_sample_treatment_counts
from .meta import (
    get_study_metadata,
    get_charts_meta,
    get_data_types_chart,
    build_filtered_sample_ids,
)

__all__ = [
    "_build_filter_subquery", "get_clinical_attributes",
    "get_value_color", "CBIOPORTAL_D3_COLORS", "RESERVED_COLORS",
    "get_clinical_counts", "get_all_clinical_counts", "get_clinical_data_table", "get_numeric_histogram",
    "get_mutated_genes", "get_cna_genes", "get_sv_genes", "get_data_types",
    "get_age_histogram", "_get_panel_availability",
    "get_km_data", "compute_km_curve", "get_tmb_fga_scatter",
    "get_patient_treatment_counts", "get_sample_treatment_counts",
    "get_study_metadata", "get_charts_meta", "get_data_types_chart",
    "build_filtered_sample_ids",
]
