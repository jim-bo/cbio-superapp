"""Results View route handlers — /results/oncoprint?cancer_study_list=...&gene_list=..."""
from __future__ import annotations

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Annotated

from cbioportal.core.oncoprint_repository import (
    get_oncoprint_data,
    get_clinical_track_options,
    get_clinical_track_data,
    get_lollipop_data,
    get_mutation_summary,
    get_mutations_table,
)

router = APIRouter()


def _get_study_meta(conn, study_id: str) -> dict:
    """Return basic study metadata or raise 404."""
    try:
        row = conn.execute(
            "SELECT study_id, name, description FROM studies WHERE study_id = ?",
            [study_id],
        ).fetchone()
    except Exception:
        row = None
    if not row:
        raise HTTPException(status_code=404, detail=f"Study '{study_id}' not found")
    study_id_val, name, description = row
    try:
        n_samples = conn.execute(
            f'SELECT COUNT(*) FROM "{study_id}_sample"'
        ).fetchone()[0]
    except Exception:
        n_samples = 0
    try:
        n_patients = conn.execute(
            f'SELECT COUNT(DISTINCT PATIENT_ID) FROM "{study_id}_sample"'
        ).fetchone()[0]
    except Exception:
        n_patients = 0
    return {
        "study_id": study_id_val,
        "name": name or study_id,
        "description": description or "",
        "n_samples": n_samples,
        "n_patients": n_patients,
    }


@router.get("/results/oncoprint", response_class=HTMLResponse)
async def oncoprint_page(
    request: Request,
    cancer_study_list: str = "",
    gene_list: str = "",
    case_set_id: str = "",
    profileFilter: str = "",
    tab: str = "oncoprint",
):
    """Render the Results View page (OncoPrint + Mutations tabs)."""
    study_id = cancer_study_list.split(",")[0].strip() if cancer_study_list else ""
    # Support space- or %20-separated gene list; keep all genes for multi-gene OncoPrint
    genes = [g.strip() for g in gene_list.replace(",", " ").split() if g.strip()]

    conn = request.app.state.db_conn
    meta = _get_study_meta(conn, study_id) if study_id else {}

    return request.app.state.templates.TemplateResponse(
        "results_view/page.html",
        {
            "request": request,
            "study_id": study_id,
            "genes": genes,
            "gene": genes[0] if genes else "",   # first gene (for Mutations tab default)
            "meta": meta,
            "active_tab": tab,
        },
    )


@router.post("/results/oncoprint/genetic-data")
async def oncoprint_genetic_data(
    request: Request,
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
):
    """Return GeneticTrackDatum[] for one gene in a study."""
    conn = request.app.state.db_conn
    data = get_oncoprint_data(conn, study_id, gene)
    return JSONResponse(data)


@router.post("/results/oncoprint/clinical-options")
async def oncoprint_clinical_options(
    request: Request,
    study_id: Annotated[str, Form()],
):
    """Return [{attr_id, display_name, freq, datatype}] sorted by completeness."""
    conn = request.app.state.db_conn
    options = get_clinical_track_options(conn, study_id)
    return JSONResponse(options)


@router.post("/results/oncoprint/clinical-data")
async def oncoprint_clinical_data(
    request: Request,
    study_id: Annotated[str, Form()],
    attr_ids: Annotated[str, Form()] = "",
):
    """Return {sampleId: {attrId: value}} for the requested attributes."""
    conn = request.app.state.db_conn
    ids = [a.strip() for a in attr_ids.split(",") if a.strip()]
    data = get_clinical_track_data(conn, study_id, ids)
    return JSONResponse(data)


# ── Mutations Tab ─────────────────────────────────────────────────────────────

@router.post("/results/oncoprint/lollipop")
async def mutations_lollipop(
    request: Request,
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
):
    """Return lollipop plot data: aggregated positions + protein length."""
    conn = request.app.state.db_conn
    data = get_lollipop_data(conn, study_id, gene)
    return JSONResponse(data)


@router.post("/results/oncoprint/mutation-summary")
async def mutations_summary(
    request: Request,
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
):
    """Return per-type mutation summary for the right panel."""
    conn = request.app.state.db_conn
    data = get_mutation_summary(conn, study_id, gene)
    return JSONResponse(data)


@router.post("/results/oncoprint/mutations-table")
async def mutations_table_data(
    request: Request,
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
    page: Annotated[int, Form()] = 1,
    page_size: Annotated[int, Form()] = 25,
    sort_col: Annotated[str, Form()] = "Protein_position",
    sort_dir: Annotated[str, Form()] = "ASC",
):
    """Return paginated mutation rows for the mutations table."""
    conn = request.app.state.db_conn
    data = get_mutations_table(conn, study_id, gene, page, page_size, sort_col, sort_dir)
    return JSONResponse(data)
