"""Study View repository: DB queries for the study summary dashboard."""
from __future__ import annotations

import json
import logging
import math
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Study metadata
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Filter engine (Internal)
# ---------------------------------------------------------------------------

def _get_mutation_sample_col(conn, study_id: str) -> str:
    """Return the sample ID column name for the mutation table (SAMPLE_ID or Tumor_Sample_Barcode)."""
    try:
        cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_mutations"').fetchall()]
        if "SAMPLE_ID" in cols:
            return "SAMPLE_ID"
        if "Tumor_Sample_Barcode" in cols:
            return "Tumor_Sample_Barcode"
        return "SAMPLE_ID" # default
    except Exception:
        return "SAMPLE_ID"

def _build_filter_subquery(conn, study_id: str, filter_json: str | None) -> tuple[str, list]:
    """
    Returns (sql_subquery, params). 
    The SQL returns a list of SAMPLE_IDs that match the filters.
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

    if not clinical_filters and not mutation_filter_genes:
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
                    conditions.append(f'"{attr_id}" IS NULL')
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
                subqueries.append(f'SELECT DISTINCT s.SAMPLE_ID FROM "{study_id}_sample" s JOIN {table} p ON s.PATIENT_ID = p.PATIENT_ID WHERE {where}')
            params.extend(local_params)

    # Mutation gene filter
    if mutation_filter_genes:
        mut_sample_col = _get_mutation_sample_col(conn, study_id)
        for gene in mutation_filter_genes:
            subqueries.append(
                f'SELECT DISTINCT {mut_sample_col} FROM "{study_id}_mutations" WHERE Hugo_Symbol = ?'
            )
            params.append(gene)

    if not subqueries:
        return f'SELECT SAMPLE_ID FROM "{study_id}_sample"', []

    sql = "\nINTERSECT\n".join(subqueries)
    return sql, params


# ---------------------------------------------------------------------------
# Color mapping logic
# ---------------------------------------------------------------------------

CBIOPORTAL_D3_COLORS = [
    "#3366cc", "#dc3912", "#ff9900", "#109618", "#990099", "#0099c6", "#dd4477",
    "#66aa00", "#b82e2e", "#316395", "#994499", "#22aa99", "#aaaa11", "#6633cc",
    "#e67300", "#8b0707", "#651067", "#329262", "#5574a6", "#3b3eac", "#b77322",
    "#16d620", "#b91383", "#f4359e", "#9c5935", "#a9c413", "#2a778d", "#668d1c",
    "#bea413", "#0c5922", "#743411"
]

RESERVED_COLORS = {
    "male": "#2986E2",
    "female": "#E0699E",
    "yes": "#1b9e77",
    "no": "#d95f02",
    "true": "#1b9e77",
    "false": "#d95f02",
    "deceased": "#d95f02",
    "living": "#1b9e77",
    "na": "#D3D3D3",
    "unknown": "#A9A9A9"
}

def _hash_string(s: str) -> int:
    h = 0
    for char in s:
        h = (31 * h + ord(char)) & 0xFFFFFFFF
    return h

def get_value_color(conn, value: str, attr_id: str = None) -> str:
    """Resolve color based on reserved maps, OncoTree, or D3 fallback."""
    v_lower = str(value).lower().strip()
    
    # 1. Reserved colors
    if v_lower in RESERVED_COLORS:
        return RESERVED_COLORS[v_lower]
    
    # 2. OncoTree colors (if it's a cancer type)
    if attr_id == "CANCER_TYPE" or attr_id == "CANCER_TYPE_DETAILED":
        try:
            row = conn.execute("SELECT dedicated_color FROM cancer_types WHERE name = ?", (value,)).fetchone()
            if row and row[0] and row[0] != 'Gainsboro':
                return row[0]
        except Exception:
            pass

    # 3. D3 Fallback (consistent hashing)
    idx = abs(_hash_string(str(value))) % len(CBIOPORTAL_D3_COLORS)
    return CBIOPORTAL_D3_COLORS[idx]


# ---------------------------------------------------------------------------
# Clinical attribute counts
# ---------------------------------------------------------------------------

def get_clinical_counts(
    conn,
    study_id: str,
    attribute_id: str,
    source_table: str = "sample",
    filter_json: str | None = None,
) -> list[dict]:
    """Return [{value, count, pct, color}] sorted by count desc for a clinical attribute."""
    table = f'"{study_id}_{source_table}"'
    col = f'"{attribute_id}"'

    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        # Join the filtered sample list against the attribute table
        if source_table == "sample":
            sql = f"""
                SELECT
                    COALESCE(CAST(t.{col} AS VARCHAR), 'NA') AS val,
                    COUNT(*) AS cnt
                FROM {table} t
                WHERE t.SAMPLE_ID IN ({filter_sql})
                GROUP BY val
                ORDER BY cnt DESC, val ASC
                LIMIT 100
            """
        else:
            # Patient table join - count distinct patients to match cBioPortal behavior
            sql = f"""
                SELECT
                    COALESCE(CAST(p.{col} AS VARCHAR), 'NA') AS val,
                    COUNT(DISTINCT p.PATIENT_ID) AS cnt
                FROM "{study_id}_sample" s
                JOIN {table} p ON s.PATIENT_ID = p.PATIENT_ID
                WHERE s.SAMPLE_ID IN ({filter_sql})
                GROUP BY val
                ORDER BY cnt DESC, val ASC
                LIMIT 100
            """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    total = sum(r[1] for r in rows) or 1
    
    results = []
    for i, r in enumerate(rows):
        value = r[0]
        count = r[1]
        v_lower = str(value).lower().strip()
        
        # 1. Check Reserved Colors first
        if v_lower in RESERVED_COLORS:
            color = RESERVED_COLORS[v_lower]
        else:
            # 2. Assign by Rank (Order of Frequency) to match cBioPortal aesthetic
            color = CBIOPORTAL_D3_COLORS[i % len(CBIOPORTAL_D3_COLORS)]
            
        results.append({
            "value": value,
            "count": count,
            "pct": round(count / total * 100, 1),
            "color": color
        })
        
    return results



def get_all_clinical_counts(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> dict[str, list[dict]]:
    """Return clinical counts for every available attribute."""
    attrs = get_clinical_attributes(conn, study_id)
    result: dict[str, list[dict]] = {}
    for attr_id, source in attrs.items():
        result[attr_id] = get_clinical_counts(
            conn, study_id, attr_id, source, filter_json
        )
    return result


# ---------------------------------------------------------------------------
# Genomic table widgets
# ---------------------------------------------------------------------------

def _get_panel_availability(conn, study_id: str, panel_col: str = "mutations") -> bool:
    """True if panel-aware freq calculation is possible for this study and alteration type.

    panel_col: column in {study_id}_gene_panel to check — 'mutations', 'structural_variants', or 'cna'.
    """
    try:
        cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_gene_panel"').fetchall()]
        if panel_col not in cols:
            return False
    except Exception:
        return False
    try:
        return conn.execute("SELECT COUNT(*) FROM gene_panel_definitions").fetchone()[0] > 0
    except Exception:
        return False


def get_mutated_genes(
    conn,
    study_id: str,
    filter_json: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return [{gene, n_mut, n_samples, n_profiled, freq}] sorted by n_samples desc.

    When gene panel data is available, freq = n_samples / n_profiled_for_gene.
    Falls back to freq = n_samples / total_filtered_samples otherwise.
    """
    # Match public cBioPortal behaviour: only exclude 'UNCALLED' mutation status.
    # UNCALLED mutations are used in Patient View to show supporting reads but are
    # not functional mutations. All variant classifications (including Silent) are
    # counted by default — optional mutation type filtering is only applied when
    # the user explicitly provides alterationFilter parameters.
    # Ref: ClickhouseAlterationMapper.xml getMutatedGenes query, line 21
    vc_exclusion = "AND COALESCE(Mutation_Status, '') != 'UNCALLED'"

    table = f'"{study_id}_mutations"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    mut_sample_col = _get_mutation_sample_col(conn, study_id)

    try:
        if _get_panel_availability(conn, study_id):
            sql = f"""
                WITH filtered_samples AS (
                    SELECT fs.SAMPLE_ID, CAST(gp.mutations AS VARCHAR) AS panel_id
                    FROM ({filter_sql}) fs
                    LEFT JOIN "{study_id}_gene_panel" gp ON fs.SAMPLE_ID = gp.SAMPLE_ID
                ),
                sample_classification AS (
                    SELECT
                        SAMPLE_ID, panel_id,
                        CASE
                            WHEN UPPER(panel_id) IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME')
                            THEN 'wes'
                            WHEN panel_id IS NOT NULL AND panel_id != 'NA'
                            THEN 'targeted'
                            ELSE 'unassigned'
                        END AS panel_class
                    FROM filtered_samples
                ),
                gene_profiled AS (
                    SELECT gpd.hugo_gene_symbol AS Hugo_Symbol, sc.SAMPLE_ID
                    FROM sample_classification sc
                    JOIN gene_panel_definitions gpd ON sc.panel_id = gpd.panel_id
                    WHERE sc.panel_class = 'targeted'
                    UNION ALL
                    SELECT m_genes.Hugo_Symbol, sc.SAMPLE_ID
                    FROM sample_classification sc
                    CROSS JOIN (SELECT DISTINCT Hugo_Symbol FROM {table} WHERE Hugo_Symbol IS NOT NULL) m_genes
                    WHERE sc.panel_class = 'wes'
                ),
                profiled_counts AS (
                    SELECT Hugo_Symbol, COUNT(DISTINCT SAMPLE_ID) AS n_profiled
                    FROM gene_profiled
                    GROUP BY Hugo_Symbol
                ),
                mutated_counts AS (
                    SELECT Hugo_Symbol, COUNT(*) AS n_mut, COUNT(DISTINCT {mut_sample_col}) AS n_samples
                    FROM {table}
                    WHERE {mut_sample_col} IN (SELECT SAMPLE_ID FROM filtered_samples)
                    {vc_exclusion}
                    GROUP BY Hugo_Symbol
                )
                SELECT
                    mc.Hugo_Symbol,
                    mc.n_mut,
                    mc.n_samples,
                    COALESCE(pc.n_profiled, mc.n_samples) AS n_profiled,
                    ROUND(100.0 * mc.n_samples / NULLIF(COALESCE(pc.n_profiled, mc.n_samples), 0), 1) AS freq
                FROM mutated_counts mc
                LEFT JOIN profiled_counts pc ON mc.Hugo_Symbol = pc.Hugo_Symbol
                ORDER BY mc.n_samples DESC, mc.Hugo_Symbol ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
        else:
            total_sql = f"SELECT COUNT(*) FROM ({filter_sql})"
            total = conn.execute(total_sql, params).fetchone()[0] or 1
            sql = f"""
                SELECT
                    Hugo_Symbol,
                    COUNT(*) AS n_mut,
                    COUNT(DISTINCT {mut_sample_col}) AS n_samples,
                    {total} AS n_profiled,
                    NULL as freq
                FROM {table}
                WHERE {mut_sample_col} IN ({filter_sql})
                {vc_exclusion}
                GROUP BY Hugo_Symbol
                ORDER BY n_samples DESC, Hugo_Symbol ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    return [
        {
            "gene": r[0],
            "n_mut": r[1],
            "n_samples": r[2],
            "n_profiled": r[3],
            "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1),
        }
        for r in rows
    ]


def get_age_histogram(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> list[dict]:
    """Return binned age counts. Tries CURRENT_AGE_DEID, AGE, then DIAGNOSIS_AGE."""
    attrs = get_clinical_attributes(conn, study_id)

    age_col = None
    source = "sample"
    # MSK-CHORD specific check: CURRENT_AGE_DEID
    for candidate in ("CURRENT_AGE_DEID", "AGE", "DIAGNOSIS_AGE", "AGE_AT_SEQ_REPORT", "AGE_AT_DIAGNOSIS"):
        if candidate in attrs:
            age_col = candidate
            source = attrs[candidate]
            break

    if not age_col:
        return []

    table = f'"{study_id}_{source}"'
    sample_table = f'"{study_id}_sample"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    # For patient-level columns, filter via a PATIENT_ID subquery instead of SAMPLE_ID
    if source == "patient":
        id_filter = f't.PATIENT_ID IN (SELECT PATIENT_ID FROM {sample_table} WHERE SAMPLE_ID IN ({filter_sql}))'
    else:
        id_filter = f't.SAMPLE_ID IN ({filter_sql})'

    try:
        sql = f"""
            SELECT
                CASE
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 35 THEN '<=35'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 40 THEN '35-40'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 45 THEN '40-45'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 50 THEN '45-50'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 55 THEN '50-55'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 60 THEN '55-60'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 65 THEN '60-65'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 70 THEN '65-70'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 75 THEN '70-75'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 80 THEN '75-80'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) <= 85 THEN '80-85'
                    WHEN TRY_CAST(t."{age_col}" AS DOUBLE) > 85 THEN '>85'
                    ELSE 'NA'
                END AS bin,
                COUNT(*) AS cnt
            FROM {table} t
            WHERE {id_filter}
            GROUP BY bin
            ORDER BY
                CASE bin
                    WHEN '<=35' THEN 1 WHEN '35-40' THEN 2 WHEN '40-45' THEN 3
                    WHEN '45-50' THEN 4 WHEN '50-55' THEN 5 WHEN '55-60' THEN 6
                    WHEN '60-65' THEN 7 WHEN '65-70' THEN 8 WHEN '70-75' THEN 9
                    WHEN '75-80' THEN 10 WHEN '80-85' THEN 11 WHEN '>85' THEN 12
                    WHEN 'NA' THEN 13
                    ELSE 99
                END
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        logger.warning("get_age_histogram failed for %s: %s", study_id, e)
        return []

    return [{"x": r[0], "y": r[1]} for r in rows]


# (Note: SV and CNA would be updated similarly)

def get_data_types(conn, study_id: str) -> list[str]:
    """Return list of data type strings available for the study."""
    try:
        rows = conn.execute(
            "SELECT data_type FROM study_data_types WHERE study_id = ? ORDER BY data_type",
            (study_id,),
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_sv_genes(
    conn,
    study_id: str,
    filter_json: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return [{gene, n_sv, n_samples, n_profiled, freq}] sorted by n_samples desc.

    When gene panel data is available, freq = n_samples / n_profiled_for_gene.
    Falls back to freq = n_samples / total_filtered_samples otherwise.
    """
    table = f'"{study_id}_sv"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        col_names = [r[0] for r in conn.execute(f'DESCRIBE {table}').fetchall()]
        if "Site1_Hugo_Symbol" in col_names:
            gene_col = "Site1_Hugo_Symbol"
            sample_col = "Sample_Id"
        elif "Gene1" in col_names:
            gene_col = "Gene1"
            sample_col = "Sample_Id"
        else:
            return []

        if _get_panel_availability(conn, study_id, "structural_variants"):
            sql = f"""
                WITH filtered_samples AS (
                    SELECT fs.SAMPLE_ID, CAST(gp.structural_variants AS VARCHAR) AS panel_id
                    FROM ({filter_sql}) fs
                    LEFT JOIN "{study_id}_gene_panel" gp ON fs.SAMPLE_ID = gp.SAMPLE_ID
                ),
                sample_classification AS (
                    SELECT
                        SAMPLE_ID, panel_id,
                        CASE
                            WHEN UPPER(panel_id) IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME')
                            THEN 'wes'
                            WHEN panel_id IS NOT NULL AND panel_id != 'NA'
                            THEN 'targeted'
                            ELSE 'unassigned'
                        END AS panel_class
                    FROM filtered_samples
                ),
                gene_profiled AS (
                    SELECT gpd.hugo_gene_symbol AS gene, sc.SAMPLE_ID
                    FROM sample_classification sc
                    JOIN gene_panel_definitions gpd ON sc.panel_id = gpd.panel_id
                    WHERE sc.panel_class = 'targeted'
                    UNION ALL
                    SELECT sv_genes.gene, sc.SAMPLE_ID
                    FROM sample_classification sc
                    CROSS JOIN (
                        SELECT DISTINCT {gene_col} AS gene FROM {table}
                        WHERE {gene_col} IS NOT NULL AND {gene_col} != ''
                    ) sv_genes
                    WHERE sc.panel_class = 'wes'
                ),
                profiled_counts AS (
                    SELECT gene, COUNT(DISTINCT SAMPLE_ID) AS n_profiled
                    FROM gene_profiled
                    GROUP BY gene
                ),
                sv_counts AS (
                    SELECT
                        {gene_col} AS gene,
                        COUNT(*) AS n_sv,
                        COUNT(DISTINCT {sample_col}) AS n_samples
                    FROM {table}
                    WHERE {sample_col} IN (SELECT SAMPLE_ID FROM filtered_samples)
                    AND {gene_col} IS NOT NULL AND {gene_col} != ''
                    GROUP BY gene
                )
                SELECT
                    sc.gene,
                    sc.n_sv,
                    sc.n_samples,
                    COALESCE(pc.n_profiled, sc.n_samples) AS n_profiled,
                    ROUND(100.0 * sc.n_samples / NULLIF(COALESCE(pc.n_profiled, sc.n_samples), 0), 1) AS freq
                FROM sv_counts sc
                LEFT JOIN profiled_counts pc ON sc.gene = pc.gene
                ORDER BY sc.n_sv DESC, sc.gene ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
        else:
            total_sql = f"SELECT COUNT(*) FROM ({filter_sql})"
            total = conn.execute(total_sql, params).fetchone()[0] or 1
            sql = f"""
                SELECT
                    {gene_col} AS gene,
                    COUNT(*) AS n_sv,
                    COUNT(DISTINCT {sample_col}) AS n_samples,
                    {total} AS n_profiled
                FROM {table}
                WHERE {sample_col} IN ({filter_sql})
                AND {gene_col} IS NOT NULL AND {gene_col} != ''
                GROUP BY gene
                ORDER BY n_sv DESC, gene ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    return [
        {
            "gene": r[0],
            "n_sv": r[1],
            "n_samples": r[2],
            "n_profiled": r[3],
            "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1),
        }
        for r in rows
    ]


def get_cna_genes(
    conn,
    study_id: str,
    filter_json: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return [{gene, cna_type, n_samples, n_profiled, freq}] (AMP=cna_value 2, HOMDEL=cna_value -2).

    When gene panel data is available, freq = n_samples / n_profiled_for_gene.
    Falls back to freq = n_samples / total_filtered_samples otherwise.
    """
    table = f'"{study_id}_cna"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        if _get_panel_availability(conn, study_id, "cna"):
            sql = f"""
                WITH filtered_samples AS (
                    SELECT fs.SAMPLE_ID, CAST(gp.cna AS VARCHAR) AS panel_id
                    FROM ({filter_sql}) fs
                    LEFT JOIN "{study_id}_gene_panel" gp ON fs.SAMPLE_ID = gp.SAMPLE_ID
                ),
                sample_classification AS (
                    SELECT
                        SAMPLE_ID, panel_id,
                        CASE
                            WHEN UPPER(panel_id) IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME')
                            THEN 'wes'
                            WHEN panel_id IS NOT NULL AND panel_id != 'NA'
                            THEN 'targeted'
                            ELSE 'unassigned'
                        END AS panel_class
                    FROM filtered_samples
                ),
                gene_profiled AS (
                    SELECT gpd.hugo_gene_symbol AS hugo_symbol, sc.SAMPLE_ID
                    FROM sample_classification sc
                    JOIN gene_panel_definitions gpd ON sc.panel_id = gpd.panel_id
                    WHERE sc.panel_class = 'targeted'
                    UNION ALL
                    SELECT cna_genes.hugo_symbol, sc.SAMPLE_ID
                    FROM sample_classification sc
                    CROSS JOIN (
                        SELECT DISTINCT hugo_symbol FROM {table} WHERE hugo_symbol IS NOT NULL
                    ) cna_genes
                    WHERE sc.panel_class = 'wes'
                ),
                profiled_counts AS (
                    SELECT hugo_symbol, COUNT(DISTINCT SAMPLE_ID) AS n_profiled
                    FROM gene_profiled
                    GROUP BY hugo_symbol
                ),
                cna_counts AS (
                    SELECT
                        hugo_symbol,
                        CASE WHEN cna_value = 2 THEN 'AMP' ELSE 'HOMDEL' END AS cna_type,
                        COUNT(DISTINCT sample_id) AS n_samples
                    FROM {table}
                    WHERE cna_value IN (2, -2)
                    AND sample_id IN (SELECT SAMPLE_ID FROM filtered_samples)
                    -- Skip isoforms to match portal counts for CDKN2A
                    AND hugo_symbol NOT IN ('CDKN2Ap14ARF', 'CDKN2Ap16INK4A')
                    GROUP BY hugo_symbol, cna_type
                )
                SELECT
                    cc.hugo_symbol,
                    cc.cna_type,
                    cc.n_samples,
                    COALESCE(pc.n_profiled, cc.n_samples) AS n_profiled,
                    ROUND(100.0 * cc.n_samples / NULLIF(COALESCE(pc.n_profiled, cc.n_samples), 0), 1) AS freq
                FROM cna_counts cc
                LEFT JOIN profiled_counts pc ON cc.hugo_symbol = pc.hugo_symbol
                ORDER BY cc.n_samples DESC, cc.hugo_symbol ASC, cc.cna_type ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
        else:
            total_sql = f"SELECT COUNT(*) FROM ({filter_sql})"
            total = conn.execute(total_sql, params).fetchone()[0] or 1
            sql = f"""
                SELECT
                    hugo_symbol,
                    CASE WHEN cna_value = 2 THEN 'AMP' ELSE 'HOMDEL' END AS cna_type,
                    COUNT(DISTINCT sample_id) AS n_samples,
                    {total} AS n_profiled,
                    ROUND(100.0 * COUNT(DISTINCT sample_id) / NULLIF({total}, 0), 1) AS freq
                FROM {table}
                WHERE cna_value IN (2, -2)
                AND sample_id IN ({filter_sql})
                -- Skip isoforms to match portal counts for CDKN2A
                AND hugo_symbol NOT IN ('CDKN2Ap14ARF', 'CDKN2Ap16INK4A')
                GROUP BY hugo_symbol, cna_type
                ORDER BY n_samples DESC, hugo_symbol ASC, cna_type ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    return [
        {
            "gene": r[0],
            "cna_type": r[1],
            "n_samples": r[2],
            "n_profiled": r[3],
            "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1),
        }
        for r in rows
    ]


def get_km_data(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> list[dict]:
    """Return [{time, survival}] KM curve points from OS_MONTHS + OS_STATUS."""
    attrs = get_clinical_attributes(conn, study_id)
    time_col = None
    status_col = None
    source = "patient"

    for tc in ("OS_MONTHS", "os_months"):
        if tc in attrs:
            time_col = tc
            source = attrs[tc]
            break
    for sc in ("OS_STATUS", "os_status"):
        if sc in attrs:
            status_col = sc
            break

    if not time_col or not status_col:
        return []

    table = f'"{study_id}_{source}"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        # Join to samples if patient table
        sql = f"""
            SELECT
                TRY_CAST(p."{time_col}" AS DOUBLE) AS t,
                CASE
                    WHEN p."{status_col}" ILIKE '%deceased%' OR p."{status_col}" = '1:DECEASED' THEN 1
                    ELSE 0
                END AS event
            FROM "{study_id}_sample" s
            JOIN {table} p ON s.PATIENT_ID = p.PATIENT_ID
            WHERE s.SAMPLE_ID IN ({filter_sql})
            AND TRY_CAST(p."{time_col}" AS DOUBLE) IS NOT NULL
            ORDER BY t
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    pairs = [(r[0], r[1]) for r in rows if r[0] is not None]
    return compute_km_curve(pairs)


def compute_km_curve(pairs: list[tuple[float, int]]) -> list[dict]:
    """Kaplan-Meier step function. pairs = [(time, event)] where event=1=death."""
    if not pairs:
        return []
    pairs = sorted(pairs, key=lambda x: x[0])
    survival = 1.0
    n_at_risk = len(pairs)
    curve = [{"time": 0.0, "survival": 1.0}]
    i = 0
    while i < len(pairs):
        t = pairs[i][0]
        deaths = 0
        censored = 0
        j = i
        while j < len(pairs) and pairs[j][0] == t:
            if pairs[j][1] == 1:
                deaths += 1
            else:
                censored += 1
            j += 1
        if deaths > 0:
            survival *= (n_at_risk - deaths) / n_at_risk
            curve.append({"time": t, "survival": round(survival, 4)})
        n_at_risk -= (deaths + censored)
        i = j
    return curve


def get_tmb_fga_scatter(
    conn,
    study_id: str,
    filter_json: str | None = None,
    max_points: int = 2000,
) -> list[dict]:
    """Return [{mutation_count, fga}] scatter data."""
    attrs = get_clinical_attributes(conn, study_id)
    fga_col = None
    for candidate in ("FRACTION_GENOME_ALTERED", "FGA"):
        if candidate in attrs:
            fga_col = candidate
            break
    if not fga_col:
        return []

    sample_table = f'"{study_id}_sample"'
    mut_table = f'"{study_id}_mutations"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    mut_sample_col = _get_mutation_sample_col(conn, study_id)

    try:
        sql = f"""
            SELECT
                s.SAMPLE_ID,
                TRY_CAST(s."{fga_col}" AS DOUBLE) AS fga,
                COUNT(m.{mut_sample_col}) AS mutation_count
            FROM {sample_table} s
            LEFT JOIN {mut_table} m ON s.SAMPLE_ID = m.{mut_sample_col}
            WHERE s.SAMPLE_ID IN ({filter_sql})
            GROUP BY s.SAMPLE_ID, fga
            HAVING fga IS NOT NULL
            LIMIT {max_points}
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    return [
        {"sample_id": r[0], "fga": round(r[1], 4), "mutation_count": r[2]}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Clinical attributes introspection
# ---------------------------------------------------------------------------

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

def build_filtered_sample_ids(conn, study_id: str, filter_json: str | None) -> list[str] | None:
    """Legacy helper for non-refactored routes."""
    sql, params = _build_filter_subquery(conn, study_id, filter_json)
    try:
        rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    except:
        return None
