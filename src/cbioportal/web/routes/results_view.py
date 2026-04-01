"""Results View route handlers — /results/oncoprint?cancer_study_list=...&gene_list=..."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Annotated

import json

from cbioportal.core.database import get_db
from cbioportal.core.oncoprint_repository import (
    get_oncoprint_data,
    get_clinical_track_options,
    get_clinical_track_data,
    get_lollipop_data,
    get_mutation_summary,
    get_mutations_table,
)
from cbioportal.core.plots_repository import (
    get_cancer_types_summary,
    get_clinical_attribute_options,
    get_color_data,
    get_generic_assay_entities,
    get_molecular_profiles,
    get_plots_data,
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
def oncoprint_page(
    request: Request,
    conn=Depends(get_db),
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

    meta = _get_study_meta(conn, study_id) if study_id else {}
    profiles = get_molecular_profiles(conn, study_id) if study_id else []

    return request.app.state.templates.TemplateResponse(
        "results_view/page.html",
        {
            "request": request,
            "study_id": study_id,
            "genes": genes,
            "gene": genes[0] if genes else "",   # first gene (for Mutations tab default)
            "meta": meta,
            "active_tab": tab,
            "molecular_profiles": profiles,
        },
    )


@router.post("/results/oncoprint/genetic-data")
def oncoprint_genetic_data(
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
    conn=Depends(get_db),
):
    """Return GeneticTrackDatum[] for one gene in a study."""
    data = get_oncoprint_data(conn, study_id, gene)
    return JSONResponse(data)


@router.post("/results/oncoprint/clinical-options")
def oncoprint_clinical_options(
    study_id: Annotated[str, Form()],
    conn=Depends(get_db),
):
    """Return [{attr_id, display_name, freq, datatype}] sorted by completeness."""
    options = get_clinical_track_options(conn, study_id)
    return JSONResponse(options)


@router.post("/results/oncoprint/clinical-data")
def oncoprint_clinical_data(
    study_id: Annotated[str, Form()],
    conn=Depends(get_db),
    attr_ids: Annotated[str, Form()] = "",
):
    """Return {sampleId: {attrId: value}} for the requested attributes."""
    ids = [a.strip() for a in attr_ids.split(",") if a.strip()]
    data = get_clinical_track_data(conn, study_id, ids)
    return JSONResponse(data)


# ── Mutations Tab ─────────────────────────────────────────────────────────────

@router.post("/results/oncoprint/lollipop")
def mutations_lollipop(
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
    conn=Depends(get_db),
):
    """Return lollipop plot data: aggregated positions + protein length."""
    data = get_lollipop_data(conn, study_id, gene)
    return JSONResponse(data)


@router.post("/results/oncoprint/mutation-summary")
def mutations_summary(
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
    conn=Depends(get_db),
):
    """Return per-type mutation summary for the right panel."""
    data = get_mutation_summary(conn, study_id, gene)
    return JSONResponse(data)


@router.post("/results/oncoprint/mutations-table")
def mutations_table_data(
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
    conn=Depends(get_db),
    page: Annotated[int, Form()] = 1,
    page_size: Annotated[int, Form()] = 25,
    sort_col: Annotated[str, Form()] = "Protein_position",
    sort_dir: Annotated[str, Form()] = "ASC",
):
    """Return paginated mutation rows for the mutations table."""
    data = get_mutations_table(conn, study_id, gene, page, page_size, sort_col, sort_dir)
    return JSONResponse(data)


# ── Cancer Types Summary Tab ─────────────────────────────────────────────────

@router.post("/results/oncoprint/cancer-types-summary")
def cancer_types_summary(
    study_id: Annotated[str, Form()],
    gene: Annotated[str, Form()],
    conn=Depends(get_db),
    group_by: Annotated[str, Form()] = "CANCER_TYPE",
    count_by: Annotated[str, Form()] = "patients",
):
    """Return alteration breakdown per cancer type for one gene."""
    data = get_cancer_types_summary(conn, study_id, gene, group_by, count_by)
    return JSONResponse(data)


# ── Plots Tab ────────────────────────────────────────────────────────────────

@router.post("/results/oncoprint/plots-data")
def plots_data(
    study_id: Annotated[str, Form()],
    conn=Depends(get_db),
    h_config: Annotated[str, Form()] = "{}",
    v_config: Annotated[str, Form()] = "{}",
):
    """Return cross-tabulated data for the plots chart."""
    h = json.loads(h_config)
    v = json.loads(v_config)
    data = get_plots_data(conn, study_id, h, v)
    return JSONResponse(data)


@router.post("/results/oncoprint/plots-clinical-options")
def plots_clinical_options(
    study_id: Annotated[str, Form()],
    conn=Depends(get_db),
):
    """Return available clinical attributes for axis dropdowns."""
    data = get_clinical_attribute_options(conn, study_id)
    return JSONResponse(data)


@router.post("/results/oncoprint/plots-generic-assay-entities")
def plots_generic_assay_entities(
    study_id: Annotated[str, Form()],
    stable_id: Annotated[str, Form()],
    conn=Depends(get_db),
):
    """Return sorted entity IDs (e.g. drug names) for a generic assay profile."""
    entities = get_generic_assay_entities(conn, study_id, stable_id)
    return JSONResponse({"entities": entities})


@router.post("/results/oncoprint/plots-color-data")
def plots_color_data(
    study_id: Annotated[str, Form()],
    conn=Depends(get_db),
    color_config: Annotated[str, Form()] = "{}",
):
    """Return per-sample color overlay data for scatter/box coloring."""
    config = json.loads(color_config)
    data = get_color_data(conn, study_id, config)
    return JSONResponse(data)
