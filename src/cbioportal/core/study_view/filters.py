"""Filter engine: build DuckDB subqueries from dashboard filter JSON.

This module is the heart of the study view query system. Every chart endpoint
calls _build_filter_subquery() to convert the client's filter state into a SQL
subquery that yields the set of SAMPLE_IDs matching all active filters.

Key design:
  - Filters are ANDed across attributes (INTERSECT) but ORed within an attribute.
  - Clinical (sample-level) filters: direct SAMPLE_ID lookup in {study_id}_sample.
  - Clinical (patient-level) filters: join {study_id}_patient → {study_id}_sample.
  - Gene mutation filters: lookup in {study_id}_mutations by Hugo_Symbol.
  - NA filter: matches both NULL values and the string literal 'NA'.
  - Numeric range filters: use TRY_CAST to handle mixed-type columns gracefully.
"""
from __future__ import annotations

import json

_EXCLUDED_COLS = {"study_id", "PATIENT_ID", "SAMPLE_ID"}


def _get_table_columns(conn, table_name: str) -> list[str]:
    """Return column names for the given table, excluding internal cols."""
    try:
        rows = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        return [r[0] for r in rows if r[0] not in _EXCLUDED_COLS]
    except Exception:
        return []


def get_clinical_attributes(conn, study_id: str) -> dict[str, str]:
    """Return {column_name: source_table} for all clinical attribute columns."""
    sample_cols = _get_table_columns(conn, f"{study_id}_sample")
    patient_cols = _get_table_columns(conn, f"{study_id}_patient")

    attrs: dict[str, str] = {}
    for col in sample_cols:
        attrs[col] = "sample"
    for col in patient_cols:
        if col not in attrs:
            attrs[col] = "patient"
    return attrs


def _get_mutation_sample_col(conn, study_id: str) -> str:
    """Return the sample ID column name for the mutation table (SAMPLE_ID or Tumor_Sample_Barcode)."""
    try:
        cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_mutations"').fetchall()]
        if "SAMPLE_ID" in cols:
            return "SAMPLE_ID"
        if "Tumor_Sample_Barcode" in cols:
            return "Tumor_Sample_Barcode"
        return "SAMPLE_ID"  # default
    except Exception:
        return "SAMPLE_ID"


def _build_filter_subquery(conn, study_id: str, filter_json: str | None) -> tuple[str, list]:
    """Build a SQL subquery returning SAMPLE_IDs that match all active filters.

    Biology:
        The study view dashboard shows charts for a filtered cohort. When a user
        clicks a pie slice or selects a gene, only samples matching those criteria
        should appear in all other charts. This function translates the client's
        filter state (a JSON object) into a DuckDB subquery that can be embedded
        in any chart query via WHERE SAMPLE_ID IN (...).

    Engineering:
        Filter JSON structure (mirrors cBioPortal StudyViewFilter API):
            {
              "clinicalDataFilters": [
                { "attributeId": "CANCER_TYPE", "values": [{"value": "BRCA"}] },
                { "attributeId": "AGE", "values": [{"start": 50, "end": 65}] }
              ],
              "mutationFilter": { "genes": ["TP53", "KRAS"] },
              "svFilter": { "genes": [] }
            }

        INTERSECT semantics: each filter clause is a separate subquery; the final
        result is the intersection of all clause results (AND across filters).
        Within a single clinical attribute, multiple values are ORed together.

        NA handling: a {"value": "NA"} entry matches both NULL values and the
        literal string 'NA', mirroring cBioPortal's legacy behavior.

    Returns:
        (sql, params) — sql is a complete SELECT SAMPLE_ID subquery; params is the
        corresponding positional parameter list for DuckDB parameterized execution.
    """
    if not filter_json:
        return f'SELECT SAMPLE_ID FROM "{study_id}_sample"', []

    try:
        if isinstance(filter_json, str):
            f = json.loads(filter_json)
        else:
            f = filter_json
    except (json.JSONDecodeError, TypeError):
        return f'SELECT SAMPLE_ID FROM "{study_id}_sample"', []

    clinical_filters = f.get("clinicalDataFilters", [])
    mutation_filter_genes = f.get("mutationFilter", {}).get("genes", [])
    sv_filter_genes = f.get("svFilter", {}).get("genes", [])
    cna_filter_genes = f.get("cnaFilter", {}).get("genes", [])

    if not clinical_filters and not mutation_filter_genes and not sv_filter_genes and not cna_filter_genes:
        return f'SELECT SAMPLE_ID FROM "{study_id}_sample"', []

    subqueries: list[str] = []
    params: list = []

    # Clinical filters
    attrs = get_clinical_attributes(conn, study_id)
    for cf in clinical_filters:
        attr_id = cf.get("attributeId", "")
        values = cf.get("values", [])
        if not attr_id or not values:
            continue

        source = attrs.get(attr_id, "sample")
        table = f'"{study_id}_{source}"'

        conditions = []
        local_params: list = []
        for v in values:
            val = v.get("value")
            start = v.get("start")
            end = v.get("end")
            if val is not None:
                if val == "NA":
                    conditions.append(f'("{attr_id}" IS NULL OR "{attr_id}" = \'NA\')')
                else:
                    conditions.append(f'"{attr_id}" = ?')
                    local_params.append(val)
            elif start is not None or end is not None:
                parts = []
                if start is not None:
                    parts.append(f'TRY_CAST("{attr_id}" AS DOUBLE) >= ?')
                    local_params.append(float(start))
                if end is not None:
                    parts.append(f'TRY_CAST("{attr_id}" AS DOUBLE) <= ?')
                    local_params.append(float(end))
                if parts:
                    conditions.append(f"({' AND '.join(parts)})")

        if conditions:
            where = f"({' OR '.join(conditions)})"
            if source == "sample":
                subqueries.append(f'SELECT DISTINCT SAMPLE_ID FROM {table} WHERE {where}')
            else:
                # Patient level filter needs to join to samples
                subqueries.append(
                    f'SELECT DISTINCT s.SAMPLE_ID FROM "{study_id}_sample" s '
                    f'JOIN {table} p ON s.PATIENT_ID = p.PATIENT_ID WHERE {where}'
                )
            params.extend(local_params)

    # Mutation gene filter
    if mutation_filter_genes:
        mut_sample_col = _get_mutation_sample_col(conn, study_id)
        for gene in mutation_filter_genes:
            subqueries.append(
                f'SELECT DISTINCT {mut_sample_col} FROM "{study_id}_mutations" WHERE Hugo_Symbol = ?'
            )
            params.append(gene)

    # SV gene filter
    if sv_filter_genes:
        try:
            sv_cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_sv"').fetchall()]
            if "Site1_Hugo_Symbol" in sv_cols:
                gene_col = "Site1_Hugo_Symbol"
            elif "Gene1" in sv_cols:
                gene_col = "Gene1"
            else:
                gene_col = None
            if gene_col:
                for gene in sv_filter_genes:
                    subqueries.append(
                        f'SELECT DISTINCT "Sample_Id" FROM "{study_id}_sv" WHERE "{gene_col}" = ?'
                    )
                    params.append(gene)
        except Exception:
            pass

    # CNA gene filter
    if cna_filter_genes:
        for gene in cna_filter_genes:
            subqueries.append(
                f'SELECT DISTINCT sample_id FROM "{study_id}_cna" WHERE hugo_symbol = ?'
            )
            params.append(gene)

    if not subqueries:
        return f'SELECT SAMPLE_ID FROM "{study_id}_sample"', []

    sql = "\nINTERSECT\n".join(subqueries)
    return sql, params
