"""Study View route handlers — /study/summary?id=..."""
from __future__ import annotations

import json

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import ValidationError
from typing import Annotated

from cbioportal.core.session_repository import fetch_settings, get_session
from cbioportal.core.study_view_repository import (
    get_study_metadata,
    get_clinical_counts,
    get_mutated_genes,
    get_sv_genes,
    get_cna_genes,
    get_age_histogram,
    get_numeric_histogram,
    get_km_data,
    get_tmb_fga_scatter,
    get_clinical_attributes,
    get_charts_meta,
    get_data_types_chart,
    get_patient_treatment_counts,
    get_sample_treatment_counts,
)
from cbioportal.web.schemas import (
    DashboardFilters,
    ClinicalChartResponse,
    MutatedGeneRow,
    CnaGeneRow,
    SvGeneRow,
    AgeResponse,
    ScatterResponse,
    KmPoint,
    DataTypeRow,
    ChartMetaRow,
    NavbarCounts,
)

router = APIRouter()


def _parse_filters(filter_json: str) -> DashboardFilters:
    """Validate and parse filter_json; raises HTTP 400 on malformed input."""
    try:
        return DashboardFilters.model_validate_json(filter_json)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filter_json: {exc}")


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

@router.get("/study/summary/charts-meta", response_model=list[ChartMetaRow])
async def charts_meta_endpoint(request: Request, id: str):
    """Return ordered chart descriptors used to build the dashboard layout."""
    conn = request.app.state.db_conn
    return get_charts_meta(conn, id)


@router.get("/study/summary", response_class=HTMLResponse)
async def study_summary(request: Request, id: str, session_id: str | None = None):
    """Render the Study View summary (dashboard) page.

    Restores saved filter state server-side so the page loads with the right
    filters already applied — no client-side async fetch needed.
    """
    conn = request.app.state.db_conn
    meta = get_study_metadata(conn, id)
    if not meta:
        raise HTTPException(status_code=404, detail="Study not found")

    raw_token = request.cookies.get("cbio_session_token")
    restored_filters: dict = {}
    resolved_session_id: str = session_id or ""

    try:
        db = request.app.state.session_factory()
        try:
            if session_id:
                # Explicit session_id in URL — shared link or bookmark.
                record = get_session(db, session_id)
                if record and record.type == "settings":
                    restored_filters = record.data.get("filters") or {}
            elif raw_token:
                # No explicit session — restore auto-saved settings for this study.
                record = fetch_settings(db, "study_view", [id], raw_token)
                if record:
                    restored_filters = record.data.get("filters") or {}
                    resolved_session_id = record.id
        finally:
            db.close()
    except Exception:
        # Session lookup must never break the page render.
        pass

    return request.app.state.templates.TemplateResponse(
        "study_view/page.html",
        {
            "request": request,
            "meta": meta,
            "active_tab": "summary",
            "restored_filters": json.dumps(restored_filters),
            "session_id": resolved_session_id,
        },
    )


@router.get("/study/clinicalData", response_class=HTMLResponse)
async def study_clinical_data(request: Request, id: str):
    """Render the Study View clinical data tab page."""
    conn = request.app.state.db_conn
    meta = get_study_metadata(conn, id)
    if not meta:
        raise HTTPException(status_code=404, detail="Study not found")
    return request.app.state.templates.TemplateResponse(
        "study_view/page.html", {"request": request, "meta": meta, "active_tab": "clinicalData"}
    )


@router.post("/study/clinicalData/table", response_class=HTMLResponse)
async def study_clinical_data_table(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    search: Annotated[str, Form()] = "",
    sort_col: Annotated[str, Form()] = "SAMPLE_ID",
    sort_dir: Annotated[str, Form()] = "asc",
    offset: Annotated[int, Form()] = 0,
    limit: Annotated[int, Form()] = 20,
):
    """Return a paginated HTMX partial for the clinical data tab table."""
    conn = request.app.state.db_conn
    from cbioportal.core.study_view_repository import get_clinical_data_table
    
    result = get_clinical_data_table(
        conn, study_id, filter_json, search, sort_col, sort_dir, offset, limit
    )
    
    return request.app.state.templates.TemplateResponse(
        "study_view/partials/clinical_data_table.html",
        {
            "request": request,
            "study_id": study_id,
            "data": result["data"],
            "columns": result["columns"],
            "total_count": result["total_count"],
            "offset": result["offset"],
            "limit": result["limit"],
            "sort_col": sort_col,
            "sort_dir": sort_dir,
            "search": search,
        }
    )


@router.post("/study/summary/navbar-counts", response_model=NavbarCounts)
async def navbar_counts(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
):
    """Return filtered patient and sample counts for the navbar selection indicator."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    from cbioportal.core.study_view_repository import _build_filter_subquery

    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        n_samples = conn.execute(
            f"SELECT COUNT(DISTINCT SAMPLE_ID) FROM ({filter_sql})", params
        ).fetchone()[0]

        n_patients = conn.execute(
            f"SELECT COUNT(DISTINCT PATIENT_ID) FROM \"{study_id}_sample\" WHERE SAMPLE_ID IN ({filter_sql})",
            params
        ).fetchone()[0]
    except Exception:
        n_samples = 0
        n_patients = 0

    return {"n_patients": n_patients, "n_samples": n_samples}


@router.post("/study/summary/chart/clinical", response_model=ClinicalChartResponse)
async def chart_clinical(
    request: Request,
    study_id: Annotated[str, Form()],
    attribute_id: Annotated[str, Form()],
    chart_type: Annotated[str, Form()] = "pie",
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return frequency counts for one clinical attribute (pie or table chart data)."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    attrs = get_clinical_attributes(conn, study_id)
    source = attrs.get(attribute_id, "sample")
    data = get_clinical_counts(conn, study_id, attribute_id, source, filter_json)
    inferred_type = _infer_chart_type(attribute_id, data)
    return {"data": data, "chart_type": inferred_type}


@router.post("/study/summary/chart/mutated-genes", response_model=list[MutatedGeneRow])
async def chart_mutated_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return panel-aware mutation frequencies for the Mutated Genes table."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    return get_mutated_genes(conn, study_id, filter_json)


@router.post("/study/summary/chart/sv-genes", response_model=list[SvGeneRow])
async def chart_sv_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return panel-aware SV frequencies for the Structural Variant Genes table."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    return get_sv_genes(conn, study_id, filter_json)


@router.post("/study/summary/chart/cna-genes", response_model=list[CnaGeneRow])
async def chart_cna_genes(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return panel-aware CNA frequencies for the CNA Genes table."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    return get_cna_genes(conn, study_id, filter_json)


@router.post("/study/summary/chart/age", response_model=AgeResponse)
async def chart_age(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return 5-year age histogram bins and NA count for the age distribution chart."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    all_bins = get_age_histogram(conn, study_id, filter_json)
    na_count = next((r["y"] for r in all_bins if r["x"] == "NA"), 0)
    bins = [r for r in all_bins if r["x"] != "NA"]
    return {"data": bins, "na_count": na_count}


@router.post("/study/summary/chart/scatter", response_model=ScatterResponse)
async def chart_scatter(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return binned density data and Pearson/Spearman correlations for the TMB vs FGA scatter."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    return get_tmb_fga_scatter(conn, study_id, filter_json)


@router.post("/study/summary/chart/km", response_model=list[KmPoint])
async def chart_km(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return Kaplan-Meier step-function points for the Overall Survival chart."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    return get_km_data(conn, study_id, filter_json)


@router.post("/study/summary/chart/numeric", response_model=AgeResponse)
async def chart_numeric(
    request: Request,
    study_id: Annotated[str, Form()],
    attribute_id: Annotated[str, Form()],
    bin_size: Annotated[float | None, Form()] = None,
    clip_min: Annotated[float | None, Form()] = None,
    clip_max: Annotated[float | None, Form()] = None,
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return equal-width histogram bins for any numeric clinical attribute."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    all_bins = get_numeric_histogram(
        conn, study_id, attribute_id, filter_json, bin_size, clip_min, clip_max
    )
    na_count = next((r["y"] for r in all_bins if r["x"] == "NA"), 0)
    bins = [r for r in all_bins if r["x"] != "NA"]
    return {"data": bins, "na_count": na_count}


@router.post("/study/summary/chart/data-types", response_model=list[DataTypeRow])
async def chart_data_types(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return available molecular data types and their profiled sample counts."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    return get_data_types_chart(conn, study_id, filter_json)


@router.post("/study/summary/chart/patient-treatments")
async def chart_patient_treatments(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return distinct patient counts per treatment agent."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    from fastapi.responses import JSONResponse
    data = get_patient_treatment_counts(conn, study_id, filter_json)
    return JSONResponse({"rows": data})


@router.post("/study/summary/chart/sample-treatments")
async def chart_sample_treatments(
    request: Request,
    study_id: Annotated[str, Form()],
    filter_json: Annotated[str, Form()] = "{}",
    format: str | None = None,
):
    """Return sample counts by treatment agent and pre/post timing."""
    _parse_filters(filter_json)
    conn = request.app.state.db_conn
    from fastapi.responses import JSONResponse
    data = get_sample_treatment_counts(conn, study_id, filter_json)
    return JSONResponse({"rows": data})
