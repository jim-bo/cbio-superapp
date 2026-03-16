"""Study View repository: DB queries for the study summary dashboard."""
from __future__ import annotations

import json
import logging
import math
from typing import Any

from scipy import stats

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


_EMPTY_SCATTER = {
    "bins": [], "pearson_corr": 0, "pearson_pval": 1,
    "spearman_corr": 0, "spearman_pval": 1,
    "count_min": 0, "count_max": 0,
    "x_bin_size": 0.025, "y_bin_size": 1.0,
}


def get_tmb_fga_scatter(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> dict:
    """Return density-binned scatter data with Pearson/Spearman correlations."""
    attrs = get_clinical_attributes(conn, study_id)
    fga_col = None
    for candidate in ("FRACTION_GENOME_ALTERED", "FGA"):
        if candidate in attrs:
            fga_col = candidate
            break
    if not fga_col:
        return _EMPTY_SCATTER

    sample_table = f'"{study_id}_sample"'
    mut_table = f'"{study_id}_mutations"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    mut_sample_col = _get_mutation_sample_col(conn, study_id)

    try:
        sql = f"""
            SELECT
                TRY_CAST(s."{fga_col}" AS DOUBLE) AS fga,
                COUNT(DISTINCT
                    CASE WHEN m.Chromosome IS NOT NULL
                    THEN CONCAT_WS('|', m.Chromosome,
                                   CAST(m.Start_Position AS VARCHAR),
                                   CAST(m.End_Position AS VARCHAR),
                                   m.Reference_Allele,
                                   m.Tumor_Seq_Allele1)
                    ELSE NULL END
                ) AS mutation_count
            FROM {sample_table} s
            LEFT JOIN {mut_table} m
                ON s.SAMPLE_ID = m.{mut_sample_col}
                AND COALESCE(m.Mutation_Status, '') <> 'GERMLINE'
                AND COALESCE(m.Variant_Classification, '') <> 'Fusion'
            WHERE s.SAMPLE_ID IN ({filter_sql})
            GROUP BY s.SAMPLE_ID, fga
            HAVING fga IS NOT NULL AND mutation_count > 0
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return _EMPTY_SCATTER

    if not rows:
        return _EMPTY_SCATTER

    fga_arr = [r[0] for r in rows]
    mut_arr = [r[1] for r in rows]

    if len(fga_arr) > 2:
        pearson_r, pearson_p = stats.pearsonr(fga_arr, mut_arr)
        spearman_r, spearman_p = stats.spearmanr(fga_arr, mut_arr)
    else:
        pearson_r = pearson_p = spearman_r = spearman_p = 0.0

    X_BINS, Y_BINS = 40, 35
    x_bin_size = 1.0 / X_BINS
    max_mut = max(mut_arr) if mut_arr else 1
    y_bin_size = max_mut / Y_BINS

    bin_counts: dict[tuple, int] = {}
    for fga_val, mut_val in zip(fga_arr, mut_arr):
        bx = round(min(int(fga_val / x_bin_size), X_BINS - 1) * x_bin_size, 6)
        by = round(int(mut_val / y_bin_size) * y_bin_size, 6)
        bin_counts[(bx, by)] = bin_counts.get((bx, by), 0) + 1

    counts = list(bin_counts.values())
    return {
        "bins": [{"bin_x": bx, "bin_y": by, "count": c}
                 for (bx, by), c in bin_counts.items()],
        "pearson_corr":  round(float(pearson_r), 4),
        "pearson_pval":  round(float(pearson_p), 4),
        "spearman_corr": round(float(spearman_r), 4),
        "spearman_pval": round(float(spearman_p), 4),
        "count_min": min(counts) if counts else 0,
        "count_max": max(counts) if counts else 0,
        "x_bin_size": x_bin_size,
        "y_bin_size": round(y_bin_size, 4),
    }


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

# ---------------------------------------------------------------------------
# Charts metadata — drive the dynamic dashboard
# ---------------------------------------------------------------------------

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

    Each item: {attr_id, display_name, chart_type, patient_attribute, priority, w, h}.

    Primary source: ``clinical_attribute_meta`` table (populated at load time).
    Fallback: synthesise metadata from DuckDB column types via get_clinical_attributes().
    Special genomic charts (_mutated_genes, _cna_genes, _sv_genes, _scatter, _km)
    are appended based on ``study_data_types``.
    """
    charts: list[dict] = []
    _distinct_counts: dict[str, int] = {}  # populated by whichever path runs; used post-cap

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
            # Guess datatype from DuckDB DESCRIBE output
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
        # Promote pie → table for high-cardinality STRING attrs
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

    # --- Limit clinical attribute charts to top N (matches legacy portal default) ---
    charts.sort(key=lambda c: (-c["priority"], c["attr_id"]))
    charts = charts[:_MAX_CLINICAL_CHARTS]

    # --- Exclude single-value pie charts after cap (matches legacy shouldShowChart logic) ---
    # Applied post-cap so single-value attrs still consume a priority slot (matching legacy),
    # which prevents lower-priority attrs from advancing into the visible set.
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

    # For KM: check whether OS columns exist regardless of priority
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
# (equivalent to legacy sample_profile join table)
_DATA_TYPE_PANEL_COL = {
    "mutation": "mutations",
    "cna":      "cna",
    "sv":       "structural_variants",
}


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

    # Only show molecular profile types, in canonical order
    data_types = [dt for dt in _DATA_TYPE_ORDER if dt in all_data_types & _MOLECULAR_DATA_TYPES]

    result = []
    for dt in data_types:
        display_name = _DATA_TYPE_DISPLAY.get(dt, dt)
        panel_col = _DATA_TYPE_PANEL_COL.get(dt)
        if panel_col:
            # Count filtered samples that have a non-null panel entry for this data type.
            # This mirrors the legacy sample_profile count (profiled samples, not just
            # those with detected alterations).
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
    """Legacy helper for non-refactored routes."""
    sql, params = _build_filter_subquery(conn, study_id, filter_json)
    try:
        rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    except:
        return None
