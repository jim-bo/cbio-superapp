"""Genomic alteration queries: mutated genes, CNA genes, SV genes, and age histogram.

Performance strategy: queries use pre-computed "{study_id}_genomic_event_derived" and
"{study_id}_profiled_counts" tables created at load time by create_genomic_derived_tables().
This mirrors cBioPortal's ClickHouse genomic_event_derived approach — all expensive
panel-awareness joins are done once at load time, not per-request.
"""
from __future__ import annotations

import logging

from .filters import _build_filter_subquery, get_clinical_attributes

logger = logging.getLogger(__name__)


def _has_derived_table(conn, study_id: str) -> bool:
    """Check if the pre-computed derived table exists for this study."""
    try:
        conn.execute(f'SELECT 1 FROM "{study_id}_genomic_event_derived" LIMIT 0')
        return True
    except Exception:
        return False


def _has_profiled_table(conn, study_id: str) -> bool:
    """Check if the pre-computed profiled counts table exists."""
    try:
        conn.execute(f'SELECT 1 FROM "{study_id}_profiled_counts" LIMIT 0')
        return True
    except Exception:
        return False


def get_mutated_genes(
    conn,
    study_id: str,
    filter_json: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return [{gene, n_mut, n_samples, n_profiled, freq}] sorted by n_samples desc.

    Uses the pre-computed genomic_event_derived table for fast aggregation.
    Falls back to direct table scan if the derived table doesn't exist.
    """
    if not _has_derived_table(conn, study_id):
        return _get_mutated_genes_legacy(conn, study_id, filter_json, limit)

    derived = f'"{study_id}_genomic_event_derived"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    has_profiled = _has_profiled_table(conn, study_id)

    try:
        if has_profiled:
            sql = f"""
                WITH filtered AS (
                    SELECT sample_id FROM ({filter_sql})
                ),
                event_counts AS (
                    SELECT
                        hugo_symbol,
                        COUNT(*) AS n_mut,
                        COUNT(DISTINCT sample_id) AS n_samples
                    FROM {derived}
                    WHERE variant_type = 'mutation'
                    AND sample_id IN (SELECT sample_id FROM filtered)
                    GROUP BY hugo_symbol
                )
                SELECT
                    ec.hugo_symbol,
                    ec.n_mut,
                    ec.n_samples,
                    COALESCE(pc.n_profiled, ec.n_samples) AS n_profiled,
                    ROUND(100.0 * ec.n_samples / NULLIF(COALESCE(pc.n_profiled, ec.n_samples), 0), 1) AS freq
                FROM event_counts ec
                LEFT JOIN "{study_id}_profiled_counts" pc
                    ON ec.hugo_symbol = pc.hugo_symbol AND pc.variant_type = 'mutation'
                ORDER BY ec.n_samples DESC, ec.hugo_symbol ASC
                LIMIT {limit}
            """
        else:
            total_sql = f"SELECT COUNT(*) FROM ({filter_sql})"
            total = conn.execute(total_sql, params).fetchone()[0] or 1
            sql = f"""
                SELECT
                    hugo_symbol,
                    COUNT(*) AS n_mut,
                    COUNT(DISTINCT sample_id) AS n_samples,
                    {total} AS n_profiled,
                    NULL AS freq
                FROM {derived}
                WHERE variant_type = 'mutation'
                AND sample_id IN ({filter_sql})
                GROUP BY hugo_symbol
                ORDER BY n_samples DESC, hugo_symbol ASC
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


def get_sv_genes(
    conn,
    study_id: str,
    filter_json: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return [{gene, n_sv, n_samples, n_profiled, freq}] sorted by n_samples desc."""
    if not _has_derived_table(conn, study_id):
        return _get_sv_genes_legacy(conn, study_id, filter_json, limit)

    derived = f'"{study_id}_genomic_event_derived"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    has_profiled = _has_profiled_table(conn, study_id)

    try:
        if has_profiled:
            sql = f"""
                WITH filtered AS (
                    SELECT sample_id FROM ({filter_sql})
                ),
                sv_counts AS (
                    SELECT
                        hugo_symbol AS gene,
                        COUNT(*) AS n_sv,
                        COUNT(DISTINCT sample_id) AS n_samples
                    FROM {derived}
                    WHERE variant_type = 'structural_variant'
                    AND sample_id IN (SELECT sample_id FROM filtered)
                    GROUP BY hugo_symbol
                )
                SELECT
                    sc.gene,
                    sc.n_sv,
                    sc.n_samples,
                    COALESCE(pc.n_profiled, sc.n_samples) AS n_profiled,
                    ROUND(100.0 * sc.n_samples / NULLIF(COALESCE(pc.n_profiled, sc.n_samples), 0), 1) AS freq
                FROM sv_counts sc
                LEFT JOIN "{study_id}_profiled_counts" pc
                    ON sc.gene = pc.hugo_symbol AND pc.variant_type = 'structural_variant'
                ORDER BY sc.n_sv DESC, sc.gene ASC
                LIMIT {limit}
            """
        else:
            total_sql = f"SELECT COUNT(*) FROM ({filter_sql})"
            total = conn.execute(total_sql, params).fetchone()[0] or 1
            sql = f"""
                SELECT
                    hugo_symbol AS gene,
                    COUNT(*) AS n_sv,
                    COUNT(DISTINCT sample_id) AS n_samples,
                    {total} AS n_profiled,
                    ROUND(100.0 * COUNT(DISTINCT sample_id) / NULLIF({total}, 0), 1) AS freq
                FROM {derived}
                WHERE variant_type = 'structural_variant'
                AND sample_id IN ({filter_sql})
                GROUP BY hugo_symbol
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
    """Return [{gene, cytoband, cna_type, n_samples, n_profiled, freq}]."""
    if not _has_derived_table(conn, study_id):
        return _get_cna_genes_legacy(conn, study_id, filter_json, limit)

    derived = f'"{study_id}_genomic_event_derived"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    has_profiled = _has_profiled_table(conn, study_id)

    try:
        if has_profiled:
            sql = f"""
                WITH filtered AS (
                    SELECT sample_id FROM ({filter_sql})
                ),
                cna_counts AS (
                    SELECT
                        hugo_symbol,
                        cna_type,
                        COUNT(DISTINCT sample_id) AS n_samples
                    FROM {derived}
                    WHERE variant_type = 'cna'
                    AND sample_id IN (SELECT sample_id FROM filtered)
                    GROUP BY hugo_symbol, cna_type
                )
                SELECT
                    cc.hugo_symbol,
                    cc.cna_type,
                    cc.n_samples,
                    COALESCE(pc.n_profiled, cc.n_samples) AS n_profiled,
                    ROUND(100.0 * cc.n_samples / NULLIF(COALESCE(pc.n_profiled, cc.n_samples), 0), 1) AS freq
                FROM cna_counts cc
                LEFT JOIN "{study_id}_profiled_counts" pc
                    ON cc.hugo_symbol = pc.hugo_symbol AND pc.variant_type = 'cna'
                ORDER BY cc.n_samples DESC, cc.hugo_symbol ASC, cc.cna_type ASC
                LIMIT {limit}
            """
        else:
            total_sql = f"SELECT COUNT(*) FROM ({filter_sql})"
            total = conn.execute(total_sql, params).fetchone()[0] or 1
            sql = f"""
                SELECT
                    hugo_symbol,
                    cna_type,
                    COUNT(DISTINCT sample_id) AS n_samples,
                    {total} AS n_profiled,
                    ROUND(100.0 * COUNT(DISTINCT sample_id) / NULLIF({total}, 0), 1) AS freq
                FROM {derived}
                WHERE variant_type = 'cna'
                AND sample_id IN ({filter_sql})
                GROUP BY hugo_symbol, cna_type
                ORDER BY n_samples DESC, hugo_symbol ASC, cna_type ASC
                LIMIT {limit}
            """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    # Fetch cytoband from gene_reference
    cytoband_map: dict[str, str] = {}
    try:
        cb_rows = conn.execute(
            "SELECT hugo_gene_symbol, cytoband FROM gene_reference "
            "WHERE cytoband IS NOT NULL AND cytoband != ''"
        ).fetchall()
        cytoband_map = {r[0]: r[1] for r in cb_rows}
    except Exception:
        pass

    return [
        {
            "gene": r[0],
            "cytoband": cytoband_map.get(r[0], ""),
            "cna_type": r[1],
            "n_samples": r[2],
            "n_profiled": r[3],
            "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1),
        }
        for r in rows
    ]


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


def get_age_histogram(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> list[dict]:
    """Return binned age counts. Tries CURRENT_AGE_DEID, AGE, then DIAGNOSIS_AGE."""
    attrs = get_clinical_attributes(conn, study_id)

    age_col = None
    source = "sample"
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


# ── Legacy fallback functions (used when derived tables don't exist) ──────────

def _get_mutation_sample_col(conn, study_id: str) -> str:
    """Detect whether mutation table uses Tumor_Sample_Barcode or SAMPLE_ID."""
    try:
        cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_mutations"').fetchall()]
        if "Tumor_Sample_Barcode" in cols:
            return "Tumor_Sample_Barcode"
        if "SAMPLE_ID" in cols:
            return "SAMPLE_ID"
    except Exception:
        pass
    return "Tumor_Sample_Barcode"


def _get_panel_availability(conn, study_id: str, panel_col: str = "mutations") -> bool:
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


def _get_mutated_genes_legacy(conn, study_id, filter_json, limit):
    """Original query-time join implementation — used as fallback."""
    from .filters import _get_mutation_sample_col
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
                    SELECT SAMPLE_ID, panel_id,
                        CASE
                            WHEN UPPER(panel_id) IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME') THEN 'wes'
                            WHEN panel_id IS NOT NULL AND panel_id != 'NA' THEN 'targeted'
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
                    FROM gene_profiled GROUP BY Hugo_Symbol
                ),
                mutated_counts AS (
                    SELECT Hugo_Symbol, COUNT(*) AS n_mut, COUNT(DISTINCT {mut_sample_col}) AS n_samples
                    FROM {table}
                    WHERE {mut_sample_col} IN (SELECT SAMPLE_ID FROM filtered_samples)
                    {vc_exclusion}
                    GROUP BY Hugo_Symbol
                )
                SELECT mc.Hugo_Symbol, mc.n_mut, mc.n_samples,
                    COALESCE(pc.n_profiled, mc.n_samples) AS n_profiled,
                    ROUND(100.0 * mc.n_samples / NULLIF(COALESCE(pc.n_profiled, mc.n_samples), 0), 1) AS freq
                FROM mutated_counts mc
                LEFT JOIN profiled_counts pc ON mc.Hugo_Symbol = pc.Hugo_Symbol
                ORDER BY mc.n_samples DESC, mc.Hugo_Symbol ASC
                LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
        else:
            total = conn.execute(f"SELECT COUNT(*) FROM ({filter_sql})", params).fetchone()[0] or 1
            sql = f"""
                SELECT Hugo_Symbol, COUNT(*) AS n_mut, COUNT(DISTINCT {mut_sample_col}) AS n_samples,
                    {total} AS n_profiled, NULL as freq
                FROM {table}
                WHERE {mut_sample_col} IN ({filter_sql}) {vc_exclusion}
                GROUP BY Hugo_Symbol ORDER BY n_samples DESC, Hugo_Symbol ASC LIMIT {limit}
            """
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []
    return [{"gene": r[0], "n_mut": r[1], "n_samples": r[2], "n_profiled": r[3],
             "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1)} for r in rows]


def _get_sv_genes_legacy(conn, study_id, filter_json, limit):
    """Original SV query — fallback when derived table doesn't exist."""
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    table = f'"{study_id}_sv"'
    try:
        col_names = [r[0] for r in conn.execute(f'DESCRIBE {table}').fetchall()]
        gene_col = "Site1_Hugo_Symbol" if "Site1_Hugo_Symbol" in col_names else "Gene1"
        sample_col = "Sample_Id"
        total = conn.execute(f"SELECT COUNT(*) FROM ({filter_sql})", params).fetchone()[0] or 1
        sql = f"""
            SELECT {gene_col} AS gene, COUNT(*) AS n_sv, COUNT(DISTINCT {sample_col}) AS n_samples,
                {total} AS n_profiled,
                ROUND(100.0 * COUNT(DISTINCT {sample_col}) / NULLIF({total}, 0), 1) AS freq
            FROM {table}
            WHERE {sample_col} IN ({filter_sql})
            AND {gene_col} IS NOT NULL AND {gene_col} != ''
            GROUP BY gene ORDER BY n_sv DESC, gene ASC LIMIT {limit}
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []
    return [{"gene": r[0], "n_sv": r[1], "n_samples": r[2], "n_profiled": r[3],
             "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1)} for r in rows]


def _get_cna_genes_legacy(conn, study_id, filter_json, limit):
    """Original CNA query — fallback when derived table doesn't exist."""
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)
    table = f'"{study_id}_cna"'
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM ({filter_sql})", params).fetchone()[0] or 1
        sql = f"""
            SELECT hugo_symbol, CASE WHEN cna_value >= 2 THEN 'AMP' ELSE 'HOMDEL' END AS cna_type,
                COUNT(DISTINCT sample_id) AS n_samples, {total} AS n_profiled,
                ROUND(100.0 * COUNT(DISTINCT sample_id) / NULLIF({total}, 0), 1) AS freq
            FROM {table}
            WHERE (cna_value >= 2 OR cna_value <= -1.5)
            AND sample_id IN ({filter_sql})
            AND hugo_symbol NOT IN ('CDKN2Ap14ARF', 'CDKN2Ap16INK4A')
            GROUP BY hugo_symbol, cna_type ORDER BY n_samples DESC, hugo_symbol ASC, cna_type ASC LIMIT {limit}
        """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []
    cytoband_map = {}
    try:
        cytoband_map = {r[0]: r[1] for r in conn.execute(
            "SELECT hugo_gene_symbol, cytoband FROM gene_reference WHERE cytoband IS NOT NULL AND cytoband != ''"
        ).fetchall()}
    except Exception:
        pass
    return [{"gene": r[0], "cytoband": cytoband_map.get(r[0], ""), "cna_type": r[1],
             "n_samples": r[2], "n_profiled": r[3],
             "freq": r[4] if r[4] is not None else round(r[2] / (r[3] or 1) * 100, 1)} for r in rows]
