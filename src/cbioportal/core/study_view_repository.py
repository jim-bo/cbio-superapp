"""Backward-compatibility shim — study view queries now live in core/study_view/.

This file is kept so that existing code importing from
``cbioportal.core.study_view_repository`` continues to work unchanged.
New code should import directly from ``cbioportal.core.study_view``.
"""
from cbioportal.core.study_view import (  # noqa: F401
    _build_filter_subquery,
    get_clinical_attributes,
    get_value_color,
    CBIOPORTAL_D3_COLORS,
    RESERVED_COLORS,
    get_clinical_counts,
    get_all_clinical_counts,
    get_clinical_data_table,
    get_numeric_histogram,
    get_mutated_genes,
    get_cna_genes,
    get_sv_genes,
    get_data_types,
    get_age_histogram,
    _get_panel_availability,
    get_km_data,
    compute_km_curve,
    get_tmb_fga_scatter,
    get_patient_treatment_counts,
    get_sample_treatment_counts,
    get_study_metadata,
    get_charts_meta,
    get_data_types_chart,
    build_filtered_sample_ids,
)
