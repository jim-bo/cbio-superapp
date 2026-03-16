"""Study metadata and chart configuration queries."""
from __future__ import annotations

from .filters import get_clinical_attributes, _build_filter_subquery

_CHART_DIMS: dict[str, dict] = {
    "pie":   {"w": 2, "h": 5},
    "bar":   {"w": 4, "h": 5},
    "table": {"w": 4, "h": 10},
}

_PIE_TO_TABLE_THRESHOLD = 20  # matches cBioPortal StudyViewConfig.ts `pieToTable`
_MAX_CLINICAL_CHARTS = 20     # matches cBioPortal studyview_clinical_attribute_chart_count

# Canonical priority overrides for well-known attrs (used in fallback path)
_PRIORITY_OVERRIDES: dict[str, int] = {
    "CANCER_TYPE":          3000,
    "CANCER_TYPE_DETAILED": 2000,
    "GENDER":               9,
    "SEX":                  9,
    "AGE":                  9,
    "CURRENT_AGE_DEID":     9,
    "DIAGNOSIS_AGE":        9,
}

_DATA_TYPE_DISPLAY = {
    "mutation":    "Mutations",
    "cna":         "Putative copy-number alterations from GISTIC",
    "sv":          "Structural Variants",
    "mrna":        "mRNA Expression",
    "protein":     "Protein expression (RPPA)",
    "methylation": "DNA Methylation",
}

# Molecular data types to show (excludes gene_panel, segment, treatment)
_MOLECULAR_DATA_TYPES = {"mutation", "cna", "sv", "mrna", "protein", "methylation"}

# Display order matching legacy portal
_DATA_TYPE_ORDER = ["mutation", "cna", "sv", "mrna", "protein", "methylation"]

# gene_panel table column that tracks which samples were profiled for each data type
_DATA_TYPE_PANEL_COL = {
    "mutation": "mutations",
    "cna":      "cna",
    "sv":       "structural_variants",
}


def get_study_metadata(conn, study_id: str) -> dict | None:
    """Return {name, description, pmid, type_of_cancer, n_patients, n_samples} or None."""
    row = conn.execute(
        "SELECT name, description, pmid, type_of_cancer FROM studies WHERE study_id = ?",
        (study_id,),
    ).fetchone()
    if not row:
        return None
    name, description, pmid, type_of_cancer = row

    # Count patients and samples from per-study tables if they exist
    n_patients = 0
    n_samples = 0
    try:
        n_samples = conn.execute(
            f'SELECT COUNT(*) FROM "{study_id}_sample"'
        ).fetchone()[0]
    except Exception:
        pass
    try:
        n_patients = conn.execute(
            f'SELECT COUNT(*) FROM "{study_id}_patient"'
        ).fetchone()[0]
    except Exception:
        n_patients = n_samples  # fallback

    return {
        "study_id": study_id,
        "name": name or study_id,
        "description": description or "",
        "pmid": pmid or "",
        "type_of_cancer": type_of_cancer or "",
        "n_patients": n_patients,
        "n_samples": n_samples,
    }


def _resolve_chart_type(attr_id: str, datatype: str) -> str:
    """Map attribute ID + datatype → chart_type ('pie' | 'bar' | 'table')."""
    if attr_id in ("CANCER_TYPE", "CANCER_TYPE_DETAILED"):
        return "table"
    if datatype in ("STRING", "BOOLEAN"):
        return "pie"
    if datatype == "NUMBER":
        return "bar"
    return "pie"


def _count_distinct_for_attrs(
    conn, study_id: str, attrs: list[tuple[str, str]]
) -> dict[str, int]:
    """Return {attr_id: distinct_count} for the given (attr_id, source) pairs.

    attrs is a list of (attr_id, source) where source is 'patient' or 'sample'.
    Uses one query per source table.
    """
    from collections import defaultdict
    by_source: dict[str, list[str]] = defaultdict(list)
    for attr_id, source in attrs:
        by_source[source].append(attr_id)

    result: dict[str, int] = {}
    for source, col_list in by_source.items():
        table = f'"{study_id}_{source}"'
        selects = ", ".join(
            f'COUNT(DISTINCT "{col}") AS "{col}"' for col in col_list
        )
        try:
            row = conn.execute(f"SELECT {selects} FROM {table}").fetchone()
            if row:
                for col, val in zip(col_list, row):
                    result[col] = val or 0
        except Exception:
            pass
    return result


def get_charts_meta(conn, study_id: str) -> list[dict]:
    """Return ordered chart metadata list for the study dashboard.

    Biology:
        The study view dashboard displays charts in priority order. Clinical attributes
        with higher priority (as set by the data curator in the 4-line header of
        data_clinical_*.txt files) appear first. Genomic charts (mutations, CNA, SV)
        are appended at the end in a fixed order.

    Engineering:
        Primary source: ``clinical_attribute_meta`` table (populated at load time by
        loader.py parsing the 4-line header of each clinical TSV file).
        Fallback: synthesise metadata from DuckDB column types via get_clinical_attributes().

        Post-processing rules (mirror legacy StudyViewUtils.tsx logic):
          - Pie charts with > 20 distinct values are promoted to table charts.
          - Single-value pie charts are excluded after the top-N cap is applied.
            (They still consume a priority slot before the cap — matching legacy.)
          - Maximum _MAX_CLINICAL_CHARTS (20) clinical attribute charts are shown.

        Special genomic chart IDs use a '_' prefix:
          _mutated_genes, _cna_genes, _sv_genes, _scatter, _km, _data_types.

    Each item: {attr_id, display_name, chart_type, patient_attribute, priority, w, h}.

    Citation:
        Priority/chart logic: cBioPortal StudyViewUtils.tsx:getClinicalAttributeCharts()
        and StudyViewConfig.ts:pieToTable / clinicalAttributeChartCount.
    """
    charts: list[dict] = []
    _distinct_counts: dict[str, int] = {}

    # --- Primary path: clinical_attribute_meta table ---
    has_meta_rows = False
    try:
        rows = conn.execute(
            """
            SELECT attr_id, display_name, datatype, patient_attribute, priority, description
            FROM clinical_attribute_meta
            WHERE study_id = ? AND priority != 0
            ORDER BY priority DESC
            """,
            (study_id,),
        ).fetchall()
        has_meta_rows = bool(rows)
        for attr_id, display_name, datatype, patient_attribute, priority, description in rows:
            chart_type = _resolve_chart_type(attr_id, datatype or "STRING")
            dims = _CHART_DIMS[chart_type]
            charts.append({
                "attr_id":           attr_id,
                "display_name":      display_name or attr_id,
                "chart_type":        chart_type,
                "patient_attribute": bool(patient_attribute),
                "priority":          priority,
                "description":       description,
                **dims,
            })
        # Promote pie → table for high-cardinality STRING attrs
        _pie_string_attrs = [
            (c["attr_id"], "patient" if c["patient_attribute"] else "sample")
            for c in charts if c["chart_type"] == "pie"
        ]
        if _pie_string_attrs:
            distinct_counts = _count_distinct_for_attrs(conn, study_id, _pie_string_attrs)
            _distinct_counts = distinct_counts
            for c in charts:
                if c["chart_type"] == "pie" and distinct_counts.get(c["attr_id"], 0) > _PIE_TO_TABLE_THRESHOLD:
                    c["chart_type"] = "table"
                    c.update(_CHART_DIMS["table"])
    except Exception:
        pass

    # --- Fallback: synthesise from column introspection ---
    if not has_meta_rows:
        attrs = get_clinical_attributes(conn, study_id)
        for attr_id, source in attrs.items():
            priority = _PRIORITY_OVERRIDES.get(attr_id, 1)
            try:
                desc = conn.execute(f'DESCRIBE "{study_id}_{source}"').fetchall()
                col_types = {r[0]: r[1].upper() for r in desc}
                dtype_raw = col_types.get(attr_id, "VARCHAR")
            except Exception:
                dtype_raw = "VARCHAR"
            if any(t in dtype_raw for t in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "HUGEINT")):
                datatype = "NUMBER"
            else:
                datatype = "STRING"
            chart_type = _resolve_chart_type(attr_id, datatype)
            dims = _CHART_DIMS[chart_type]
            charts.append({
                "attr_id":           attr_id,
                "display_name":      attr_id.replace("_", " ").title(),
                "chart_type":        chart_type,
                "patient_attribute": source == "patient",
                "priority":          priority,
                "description":       None,
                **dims,
            })
        _pie_string_attrs_fb = [
            (c["attr_id"], "patient" if c["patient_attribute"] else "sample")
            for c in charts if c["chart_type"] == "pie"
        ]
        if _pie_string_attrs_fb:
            distinct_counts_fb = _count_distinct_for_attrs(conn, study_id, _pie_string_attrs_fb)
            _distinct_counts = distinct_counts_fb
            for c in charts:
                if c["chart_type"] == "pie" and distinct_counts_fb.get(c["attr_id"], 0) > _PIE_TO_TABLE_THRESHOLD:
                    c["chart_type"] = "table"
                    c.update(_CHART_DIMS["table"])

    # --- Limit clinical attribute charts to top N ---
    charts.sort(key=lambda c: (-c["priority"], c["attr_id"]))
    charts = charts[:_MAX_CLINICAL_CHARTS]

    # --- Exclude single-value pie charts after cap ---
    if _distinct_counts:
        charts = [
            c for c in charts
            if not (c["chart_type"] == "pie" and _distinct_counts.get(c["attr_id"], 0) == 1)
        ]

    # --- Append special genomic charts based on study_data_types ---
    try:
        data_types = {r[0] for r in conn.execute(
            "SELECT data_type FROM study_data_types WHERE study_id = ?", (study_id,)
        ).fetchall()}
    except Exception:
        data_types = set()

    all_attrs = set(get_clinical_attributes(conn, study_id).keys())

    if "mutation" in data_types:
        charts.append({
            "attr_id": "_mutated_genes", "display_name": "Mutated Genes",
            "chart_type": "_mutated_genes", "patient_attribute": False,
            "priority": 90, "w": 4, "h": 10,
            "description": "Genes with somatic mutations in the study cohort.",
        })
    if "cna" in data_types:
        charts.append({
            "attr_id": "_cna_genes", "display_name": "CNA Genes",
            "chart_type": "_cna_genes", "patient_attribute": False,
            "priority": 80, "w": 4, "h": 10,
            "description": "Genes with copy number alterations (amplification or deep deletion) detected in the cohort.",
        })
    if "sv" in data_types:
        charts.append({
            "attr_id": "_sv_genes", "display_name": "Structural Variant Genes",
            "chart_type": "_sv_genes", "patient_attribute": False,
            "priority": 70, "w": 4, "h": 10,
            "description": "Genes involved in structural variants (fusions, rearrangements) detected in the cohort.",
        })
    if "mutation" in data_types and "cna" in data_types:
        charts.append({
            "attr_id": "_scatter", "display_name": "Mutation Count vs Fraction Genome Altered",
            "chart_type": "_scatter", "patient_attribute": False,
            "priority": 50, "w": 4, "h": 10,
            "description": "Scatter plot of tumor mutational burden (mutation count) vs fraction of genome altered (FGA) per sample.",
        })
    if data_types:
        charts.append({
            "attr_id": "_data_types", "display_name": "Data Types",
            "chart_type": "_data_types", "patient_attribute": False,
            "priority": 1000, "w": 4, "h": 10,
            "description": "Molecular data types available in this study.",
        })

    if "OS_MONTHS" in all_attrs and "OS_STATUS" in all_attrs:
        charts.append({
            "attr_id": "_km", "display_name": "KM Plot: Overall (months)",
            "chart_type": "_km", "patient_attribute": False,
            "priority": 400, "w": 2, "h": 5,
            "description": "Kaplan-Meier overall survival curve for the current cohort (OS_MONTHS / OS_STATUS).",
        })

    charts.sort(key=lambda c: (-c["priority"], c["attr_id"]))
    return charts


def get_data_types_chart(conn, study_id: str, filter_json: str | None) -> list[dict]:
    """Return list of {data_type, display_name, count, freq} for each data type in the study.

    Counts profiled samples from the gene_panel table (mirrors legacy sample_profile logic).
    Only shows molecular data types (mutation/cna/sv/mrna/protein/methylation).
    """
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    try:
        total = conn.execute(
            f'SELECT COUNT(*) FROM ({filter_sql})', params
        ).fetchone()[0]
    except Exception:
        total = 0

    try:
        all_data_types = {r[0] for r in conn.execute(
            "SELECT data_type FROM study_data_types WHERE study_id = ?",
            (study_id,),
        ).fetchall()}
    except Exception:
        all_data_types = set()

    data_types = [dt for dt in _DATA_TYPE_ORDER if dt in all_data_types & _MOLECULAR_DATA_TYPES]

    result = []
    for dt in data_types:
        display_name = _DATA_TYPE_DISPLAY.get(dt, dt)
        panel_col = _DATA_TYPE_PANEL_COL.get(dt)
        if panel_col:
            try:
                count = conn.execute(
                    f'SELECT COUNT(*) FROM "{study_id}_gene_panel" '
                    f'WHERE SAMPLE_ID IN ({filter_sql}) '
                    f"AND {panel_col} IS NOT NULL AND {panel_col} != ''",
                    params,
                ).fetchone()[0]
            except Exception:
                count = total
        else:
            count = total
        freq = (count / total * 100) if total > 0 else 0.0
        result.append({
            "data_type":    dt,
            "display_name": display_name,
            "count":        count,
            "freq":         round(freq, 1),
        })
    return result


def build_filtered_sample_ids(conn, study_id: str, filter_json: str | None) -> list[str] | None:
    """Legacy helper: return list of SAMPLE_IDs matching the filter."""
    sql, params = _build_filter_subquery(conn, study_id, filter_json)
    try:
        rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return None
