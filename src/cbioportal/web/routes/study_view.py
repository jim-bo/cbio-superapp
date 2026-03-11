"""Study View route handlers — /study/summary?id=..."""
from __future__ import annotations

import json
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from typing import Annotated

from cbioportal.core.study_view_repository import (
    get_study_metadata,
    get_all_clinical_counts,
    get_clinical_counts,
    get_clinical_attributes,
    get_data_types,
    get_mutated_genes,
    get_sv_genes,
    get_cna_genes,
    get_age_histogram,
    get_km_data,
    get_tmb_fga_scatter,
    build_filtered_sample_ids,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# Chart type config — maps attribute IDs to chart type ("pie" | "table")
# ---------------------------------------------------------------------------

_PIE_ATTRIBUTES = {
    "CLINICAL_SUMMARY", "DIAGNOSIS_DESCRIPTION", "OS_STATUS", "SAMPLE_TYPE",
    "RACE", "SEX", "GENDER", "STAGE_HIGHEST", "ETHNICITY", "MSI_TYPE", "GENE_PANEL",
    "SOMATIC_STATUS", "PRIOR_TREATMENT_TO_MSK_NLP", "SMOKING_HISTORY_NLP",
    "PRIOR_TREATMENT", "METASTATIC_SITE", "CANCER_STATUS",
    "DFS_STATUS", "PFS_STATUS",
}

_TABLE_ATTRIBUTES = {
    "CANCER_TYPE", "CLINICAL_GROUP", "ICD_O_HISTOLOGY_DESCRIPTION",
    "PATHOLOGICAL_GROUP", "CANCER_TYPE_DETAILED",
}

_SKIP_ATTRIBUTES = {
    "OS_MONTHS", "DFS_MONTHS", "PFS_MONTHS", "FRACTION_GENOME_ALTERED",
    "MUTATION_COUNT", "TMB_NONSYNONYMOUS", "MSI_SCORE",
    "AGE", "DIAGNOSIS_AGE", "AGE_AT_SEQ_REPORT", "AGE_AT_DIAGNOSIS",
}

_CHART_TITLES = {
    "CANCER_TYPE": "Cancer Type",
    "CLINICAL_GROUP": "Clinical Group",
    "CLINICAL_SUMMARY": "Clinical Summary",
    "DIAGNOSIS_DESCRIPTION": "Diagnosis Description",
    "OS_STATUS": "Overall Survival Status",
    "SAMPLE_TYPE": "Sample Type",
    "RACE": "Race",
    "SEX": "Sex",
    "STAGE_HIGHEST": "Stage (Highest Recorded)",
    "ETHNICITY": "Ethnicity",
    "ICD_O_HISTOLOGY_DESCRIPTION": "ICD-O Histology Description",
    "PATHOLOGICAL_GROUP": "Pathological Group",
    "CANCER_TYPE_DETAILED": "Cancer Type Detailed",
    "MSI_TYPE": "MSI Type",
    "GENE_PANEL": "Gene Panel",
    "SOMATIC_STATUS": "Somatic Status",
    "PRIOR_TREATMENT_TO_MSK_NLP": "Prior Treatment to MSK (NLP)",
    "SMOKING_HISTORY_NLP": "Smoking History NLP",
}


def _infer_chart_type(attr_id: str, values: list[dict]) -> str:
    """Determine whether to show pie or table for an attribute."""
    if attr_id in _PIE_ATTRIBUTES:
        return "pie"
    if attr_id in _TABLE_ATTRIBUTES:
        return "table"
    # Heuristic: low cardinality → pie, high → table
    return "pie" if len(values) <= 8 else "table"


def _build_clinical_charts(
    conn,
    study_id: str,
    filtered_sample_ids: list[str] | None,
    attrs: dict[str, str],
) -> list[dict]:
    """Build list of chart dicts for clinical attributes, skipping internal cols."""
    charts = []
    # Preferred order for display
    preferred_order = [
        "CANCER_TYPE", "CLINICAL_GROUP", "CLINICAL_SUMMARY", "DIAGNOSIS_DESCRIPTION",
        "OS_STATUS", "SAMPLE_TYPE", "RACE", "SEX", "STAGE_HIGHEST", "ETHNICITY",
        "ICD_O_HISTOLOGY_DESCRIPTION", "PATHOLOGICAL_GROUP", "CANCER_TYPE_DETAILED",
        "MSI_TYPE", "GENE_PANEL", "SOMATIC_STATUS", "PRIOR_TREATMENT_TO_MSK_NLP",
        "SMOKING_HISTORY_NLP",
    ]
    # Build in preferred order, then remaining attrs
    ordered = [a for a in preferred_order if a in attrs]
    remaining = [a for a in attrs if a not in set(preferred_order) and a not in _SKIP_ATTRIBUTES]
    for attr_id in ordered + remaining:
        if attr_id in _SKIP_ATTRIBUTES:
            continue
        source = attrs[attr_id]
        data = get_clinical_counts(conn, study_id, attr_id, source, filtered_sample_ids)
        if not data:
            continue
        chart_type = _infer_chart_type(attr_id, data)
        charts.append({
            "chart_id": attr_id,
            "chart_title": _CHART_TITLES.get(attr_id, attr_id.replace("_", " ").title()),
            "chart_type": chart_type,
            "data": data,
        })
    return charts


def _build_page_context(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> dict:
    """Build the full template context for the study summary page."""
    meta = get_study_metadata(conn, study_id)
    if not meta:
        return {}

    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    attrs = get_clinical_attributes(conn, study_id)

    # Genomic widgets
    mutated_genes = get_mutated_genes(conn, study_id, filtered_ids)
    sv_genes = get_sv_genes(conn, study_id, filtered_ids)
    cna_genes = get_cna_genes(conn, study_id, filtered_ids)

    # Complex charts
    age_histogram = get_age_histogram(conn, study_id, filtered_ids)
    km_curve = get_km_data(conn, study_id, filtered_ids)
    scatter_data = get_tmb_fga_scatter(conn, study_id, filtered_ids)

    # Clinical charts
    clinical_charts = _build_clinical_charts(conn, study_id, filtered_ids, attrs)

    # Data types
    data_types = get_data_types(conn, study_id)

    # Cohort counts
    if filtered_ids is not None:
        n_samples_filtered = len(filtered_ids)
    else:
        n_samples_filtered = meta["n_samples"]

    active_filters: list[dict] = []
    if filter_json:
        try:
            f = json.loads(filter_json)
            for cf in f.get("clinicalDataFilters", []):
                attr_id = cf.get("attributeId", "")
                vals = [v.get("value", "") for v in cf.get("values", [])]
                active_filters.append({"attribute": attr_id, "values": vals})
            for gene in f.get("mutationFilter", {}).get("genes", []):
                active_filters.append({"attribute": "Mutation", "values": [gene]})
        except Exception:
            pass

    return {
        "meta": meta,
        "study_id": study_id,
        "clinical_charts": clinical_charts,
        "data_types": data_types,
        "mutated_genes": mutated_genes,
        "sv_genes": sv_genes,
        "cna_genes": cna_genes,
        "age_histogram": age_histogram,
        "km_curve": km_curve,
        "scatter_data": scatter_data,
        "n_samples_filtered": n_samples_filtered,
        "n_patients_filtered": meta["n_patients"],
        "filter_json": filter_json or "{}",
        "active_filters": active_filters,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/study/summary", response_class=HTMLResponse)
async def study_summary(request: Request, id: str = ""):
    if not id:
        raise HTTPException(status_code=400, detail="Study ID is required")

    conn = request.app.state.db_conn
    ctx = _build_page_context(conn, id)
    if not ctx:
        raise HTTPException(status_code=404, detail=f"Study '{id}' not found")

    return request.app.state.templates.TemplateResponse(
        "study_view/page.html", {"request": request, **ctx}
    )


@router.get("/study/vanilla", response_class=HTMLResponse)
async def study_vanilla(request: Request):
    return request.app.state.templates.TemplateResponse(
        "study_view/vanilla_dashboard.html", {"request": request}
    )


@router.post("/study/summary/chart/clinical")
async def chart_clinical(
    request: Request,
    study_id: Annotated[str, Form()],
    attribute_id: Annotated[str, Form()],
    chart_type: Annotated[str, Form()] = "pie",
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    attrs = get_clinical_attributes(conn, study_id)
    source = attrs.get(attribute_id, "sample")
    data = get_clinical_counts(conn, study_id, attribute_id, source, filtered_ids)
    inferred_type = _infer_chart_type(attribute_id, data)

    if format == "json":
        return {"data": data, "chart_type": inferred_type}

    template = "study_view/partials/charts/pie_chart.html" if inferred_type == "pie" else "study_view/partials/charts/table_chart.html"
    return request.app.state.templates.TemplateResponse(template, {
        "request": request,
        "chart_id": attribute_id,
        "chart_title": _CHART_TITLES.get(attribute_id, attribute_id.replace("_", " ").title()),
        "chart_type": inferred_type,
        "data": data,
        "study_id": study_id,
        "filter_json": filter_json,
    })


@router.post("/study/summary/chart/mutated-genes")
async def chart_mutated_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    data = get_mutated_genes(conn, study_id, filtered_ids)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/mutated_genes.html",
        {"request": request, "mutated_genes": data, "study_id": study_id, "filter_json": filter_json},
    )


@router.post("/study/summary/chart/sv-genes")
async def chart_sv_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    data = get_sv_genes(conn, study_id, filtered_ids)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/sv_genes.html",
        {"request": request, "sv_genes": data, "study_id": study_id, "filter_json": filter_json},
    )


@router.post("/study/summary/chart/cna-genes")
async def chart_cna_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    data = get_cna_genes(conn, study_id, filtered_ids)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/cna_genes.html",
        {"request": request, "cna_genes": data, "study_id": study_id, "filter_json": filter_json},
    )


@router.post("/study/summary/chart/age")
async def chart_age(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    data = get_age_histogram(conn, study_id, filter_json)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/histogram.html",
        {"request": request, "age_histogram": data, "study_id": study_id, "filter_json": filter_json},
    )


@router.post("/study/summary/chart/scatter")
async def chart_scatter(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    data = get_tmb_fga_scatter(conn, study_id, filtered_ids)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/scatter_tmb_fga.html",
        {"request": request, "scatter_data": data, "study_id": study_id, "filter_json": filter_json},
    )


@router.post("/study/summary/chart/km")
async def chart_km(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    filtered_ids = build_filtered_sample_ids(conn, study_id, filter_json)
    data = get_km_data(conn, study_id, filtered_ids)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/km_plot.html",
        {"request": request, "km_curve": data, "study_id": study_id, "filter_json": filter_json},
    )
