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
    get_mutated_genes,
    get_sv_genes,
    get_cna_genes,
    get_age_histogram,
    get_km_data,
    get_tmb_fga_scatter,
    get_clinical_attributes,
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
    "DFS_STATUS", "PFS_STATUS", "NUM_ICDO_DX",
}

_TABLE_ATTRIBUTES = {
    "CANCER_TYPE", "CANCER_TYPE_DETAILED", "CLINICAL_GROUP", "PATHOLOGICAL_GROUP",
    "ICD_O_HISTOLOGY_DESCRIPTION"
}

_SKIP_ATTRIBUTES = {"study_id", "PATIENT_ID", "SAMPLE_ID"}

_CHART_TITLES = {
    "CANCER_TYPE": "Cancer Type",
    "GENDER": "Sex",
    "SEX": "Sex",
    "AGE": "Diagnosis Age",
    "OS_STATUS": "Overall Survival Status",
}


def _infer_chart_type(attr_id: str, data: list[dict]) -> str:
    if attr_id in _PIE_ATTRIBUTES:
        return "pie"
    if attr_id in _TABLE_ATTRIBUTES:
        return "table"
    # Fallback heuristic: low cardinality = pie
    return "pie" if len(data) <= 8 else "table"


def _build_page_context(conn, study_id: str) -> dict | None:
    meta = get_study_metadata(conn, study_id)
    if not meta:
        return None

    # Determine which charts to show by default
    attrs = get_clinical_attributes(conn, study_id)
    preferred_order = ["CANCER_TYPE", "GENDER", "SEX", "AGE", "OS_STATUS"]
    
    charts = []
    # Build in preferred order, then remaining attrs
    ordered = [a for a in preferred_order if a in attrs]
    remaining = [a for a in attrs if a not in set(preferred_order) and a not in _SKIP_ATTRIBUTES]
    for attr_id in ordered + remaining:
        source = attrs[attr_id]
        data = get_clinical_counts(conn, study_id, attr_id, source)
        if not data:
            continue
        charts.append({
            "id": attr_id,
            "title": _CHART_TITLES.get(attr_id, attr_id.replace("_", " ").title()),
            "type": _infer_chart_type(attr_id, data),
            "data": data,
        })

    return {
        "meta": meta,
        "charts": charts,
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
async def study_vanilla(request: Request, id: str):
    conn = request.app.state.db_conn
    meta = get_study_metadata(conn, id)
    if not meta:
        raise HTTPException(status_code=404, detail="Study not found")
    return request.app.state.templates.TemplateResponse(
        "study_view/vanilla_dashboard.html", {"request": request, "meta": meta}
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
    attrs = get_clinical_attributes(conn, study_id)
    source = attrs.get(attribute_id, "sample")
    data = get_clinical_counts(conn, study_id, attribute_id, source, filter_json)
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
    data = get_mutated_genes(conn, study_id, filter_json)
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
    data = get_sv_genes(conn, study_id, filter_json)
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
    data = get_cna_genes(conn, study_id, filter_json)
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
    data = get_tmb_fga_scatter(conn, study_id, filter_json)
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
    data = get_km_data(conn, study_id, filter_json)
    if format == "json":
        return data
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/charts/km_plot.html",
        {"request": request, "km_curve": data, "study_id": study_id, "filter_json": filter_json},
    )
