"""Homepage route handlers."""
from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Annotated

from cbioportal.core.database import get_db
from cbioportal.core.study_repository import (
    DATA_TYPE_OPTIONS,
    SPECIAL_COLLECTIONS,
    get_study_catalog,
    get_cancer_type_counts,
    get_query_form_context,
    validate_genes,
)

router = APIRouter()


def _build_context(conn, study_names, cancer_type="All", data_types=None):
    """Build template context for homepage renders."""
    if data_types is None:
        data_types = []

    studies = get_study_catalog(
        conn,
        study_names,
        cancer_type=cancer_type if cancer_type != "All" else None,
        data_types=data_types if data_types else None,
    )

    cancer_type_counts, special_counts = get_cancer_type_counts(
        conn,
        data_types=data_types if data_types else None,
    )

    # Group studies by cancer_type for display
    grouped: dict[str, list] = {}
    for s in studies:
        ct = s["cancer_type"]
        pmid = s["pmid"].split(",")[0].strip() if s["pmid"] else None
        grouped.setdefault(ct, []).append({
            "id": s["id"],
            "name": s["name"],
            "samples": s["sample_count"],
            "description": s["description"],
            "pubmed_url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None,
            "cbio_url": f"/study/summary?id={s['id']}",
        })

    # cancer_types should be the sorted list of Organ Systems
    sorted_organ_systems = sorted(cancer_type_counts.keys())

    total_studies = sum(cancer_type_counts.values()) + sum(special_counts.values())
    total_samples = sum(s["sample_count"] for s in studies)

    grouped_sorted = {}
    for key, _label in SPECIAL_COLLECTIONS:
        if key in grouped:
            grouped_sorted[key] = grouped[key]
    for ct in sorted_organ_systems:
        if ct in grouped:
            grouped_sorted[ct] = grouped[ct]

    return {
        "grouped_studies": grouped_sorted,
        "cancer_types": sorted_organ_systems,
        "cancer_type_counts": cancer_type_counts,
        "special_collections": [(key, label, special_counts.get(key, 0)) for key, label in SPECIAL_COLLECTIONS],
        "selected_cancer_type": cancer_type,
        "selected_data_types": data_types,
        "data_type_options": DATA_TYPE_OPTIONS,
        "total_studies": total_studies,
        "total_samples": total_samples,
    }


@router.get("/", response_class=HTMLResponse)
def home(request: Request, conn=Depends(get_db)):
    study_names = request.app.state.study_names
    ctx = _build_context(conn, study_names)
    return request.app.state.templates.TemplateResponse(
        "home/page.html", {"request": request, **ctx}
    )


@router.post("/studies", response_class=HTMLResponse)
def filter_studies(
    request: Request,
    conn=Depends(get_db),
    cancer_type: Annotated[str, Form()] = "All",
    data_types: Annotated[list[str], Form()] = [],
):
    study_names = request.app.state.study_names
    ctx = _build_context(conn, study_names, cancer_type=cancer_type, data_types=data_types)
    return request.app.state.templates.TemplateResponse(
        "home/partials/cancer_study_list.html", {"request": request, **ctx}
    )


@router.get("/query", response_class=HTMLResponse)
def query_page(
    request: Request,
    conn=Depends(get_db),
    study_ids: str = "",
):
    """Render the query-by-gene form as a standalone page."""
    ids = [s.strip() for s in study_ids.split(",") if s.strip()]
    if not ids:
        return HTMLResponse("No studies selected. <a href='/'>Go back</a>", status_code=400)
    ctx = get_query_form_context(conn, ids)
    return request.app.state.templates.TemplateResponse(
        "query/page.html", {"request": request, **ctx}
    )


@router.post("/validate-genes")
def validate_genes_endpoint(
    genes: Annotated[str, Form()] = "",
    conn=Depends(get_db),
):
    """Validate gene symbols against the gene_reference table."""
    result = validate_genes(conn, genes)
    return JSONResponse(result)
