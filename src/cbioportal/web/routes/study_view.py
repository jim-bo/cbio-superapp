"""Study View route handlers — /study/summary?id=..."""
from __future__ import annotations

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from typing import Annotated

from cbioportal.core.study_view_repository import (
    get_study_metadata,
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


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/study/summary", response_class=HTMLResponse)
async def study_summary(request: Request, id: str):
    conn = request.app.state.db_conn
    meta = get_study_metadata(conn, id)
    if not meta:
        raise HTTPException(status_code=404, detail="Study not found")
    return request.app.state.templates.TemplateResponse(
        "study_view/page.html", {"request": request, "meta": meta}
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
    return {"data": data, "chart_type": inferred_type}


@router.post("/study/summary/chart/mutated-genes")
async def chart_mutated_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    return get_mutated_genes(conn, study_id, filter_json)


@router.post("/study/summary/chart/sv-genes")
async def chart_sv_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    return get_sv_genes(conn, study_id, filter_json)


@router.post("/study/summary/chart/cna-genes")
async def chart_cna_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    return get_cna_genes(conn, study_id, filter_json)


@router.post("/study/summary/chart/age")
async def chart_age(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    all_bins = get_age_histogram(conn, study_id, filter_json)
    na_count = next((r["y"] for r in all_bins if r["x"] == "NA"), 0)
    bins = [r for r in all_bins if r["x"] != "NA"]
    return {"data": bins, "na_count": na_count}


@router.post("/study/summary/chart/scatter")
async def chart_scatter(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    return get_tmb_fga_scatter(conn, study_id, filter_json)


@router.post("/study/summary/chart/km")
async def chart_km(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    conn = request.app.state.db_conn
    return get_km_data(conn, study_id, filter_json)
