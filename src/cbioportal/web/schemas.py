"""Pydantic request/response schemas for Study View API endpoints.

All chart endpoints share the same filter input shape (DashboardFilters).
Response models are used as FastAPI response_model= to enforce output contracts
and generate OpenAPI documentation.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Filter / Request models
# ---------------------------------------------------------------------------

class ClinicalFilterValue(BaseModel):
    """A single string-valued category filter (e.g. value="Male")."""
    value: str


class NumericFilterValue(BaseModel):
    """An open-ended numeric range filter (e.g. Age between 40 and 60)."""
    start: float | None = None
    end: float | None = None


class ClinicalDataFilter(BaseModel):
    """One filter applied to a clinical attribute."""
    attributeId: str
    values: list[ClinicalFilterValue | NumericFilterValue]


class MutationFilter(BaseModel):
    """Gene-level mutation filter (include only samples mutated in these genes)."""
    genes: list[str] = []


class SvFilter(BaseModel):
    """Gene-level structural variant filter."""
    genes: list[str] = []


class CnaFilter(BaseModel):
    """Gene-level copy number alteration filter."""
    genes: list[str] = []


class DashboardFilters(BaseModel):
    """Complete filter state serialized by the study view dashboard.

    This is the single filter envelope accepted by all chart POST endpoints.
    The JS client serializes DashboardState.filters → JSON → form field filter_json.
    """
    clinicalDataFilters: list[ClinicalDataFilter] = []
    mutationFilter: MutationFilter = MutationFilter()
    svFilter: SvFilter = SvFilter()
    cnaFilter: CnaFilter = CnaFilter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class ClinicalCountRow(BaseModel):
    """One row in a clinical attribute frequency table (pie or table chart)."""
    model_config = ConfigDict(extra="allow")

    value: str
    count: int
    pct: float
    color: str


class ClinicalChartResponse(BaseModel):
    """Response from /chart/clinical — a list of category rows plus inferred chart type."""
    data: list[ClinicalCountRow]
    chart_type: str


class MutatedGeneRow(BaseModel):
    """One row in the Mutated Genes table."""
    gene: str
    n_mut: int
    n_samples: int
    freq: float


class CnaGeneRow(BaseModel):
    """One row in the CNA Genes table."""
    gene: str
    cna_type: str
    n_samples: int
    freq: float


class SvGeneRow(BaseModel):
    """One row in the Structural Variant Genes table."""
    gene: str
    n_sv: int
    n_samples: int
    freq: float


class AgeBin(BaseModel):
    """One histogram bin for the age distribution chart."""
    x: str
    y: int


class AgeResponse(BaseModel):
    """Response from /chart/age — bins plus NA count."""
    data: list[AgeBin]
    na_count: int


class ScatterBin(BaseModel):
    """One 2-D density bin in the TMB vs FGA scatter chart."""
    bin_x: float
    bin_y: float
    count: int


class ScatterResponse(BaseModel):
    """Response from /chart/scatter — binned density plus correlation statistics."""
    model_config = ConfigDict(extra="allow")

    bins: list[ScatterBin]
    x_bin_size: float
    y_bin_size: float
    count_min: int
    count_max: int
    pearson_corr: float
    pearson_pval: float
    spearman_corr: float
    spearman_pval: float


class KmPoint(BaseModel):
    """One step in the Kaplan-Meier survival curve."""
    time: float
    survival: float


class DataTypeRow(BaseModel):
    """One row in the Data Types availability table."""
    model_config = ConfigDict(extra="allow")

    display_name: str
    count: int
    freq: float


class ChartMetaRow(BaseModel):
    """One entry in the charts-meta list used to build the dashboard layout."""
    model_config = ConfigDict(extra="allow")

    attr_id: str
    display_name: str
    chart_type: str
    datatype: str | None = None
    priority: int
    w: int
    h: int


class NavbarCounts(BaseModel):
    """Patient and sample counts for the navbar selection indicator."""
    n_patients: int
    n_samples: int
