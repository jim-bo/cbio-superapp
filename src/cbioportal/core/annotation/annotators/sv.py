"""SV (structural variant / fusion) annotator.

Each SV row produces one output row per gene (site1 and site2 separately).
MOAlmanac is pre-aggregated to one best assertion per fusion gene.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def annotate_sv(
    conn,
    study_id: str,
    cache_db_path: str,
) -> list[dict]:
    """Annotate SV/fusion rows with MOAlmanac assertions."""
    table = f'"{study_id}_sv"'

    existing = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        (f"{study_id}_sv",),
    ).fetchone()
    if not existing:
        logger.debug("No SV table for study %s — skipping", study_id)
        return []

    sv_cols = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            (f"{study_id}_sv",),
        ).fetchall()
    }

    sample_col = next(
        (c for c in ["Tumor_Sample_Barcode", "Sample_Id", "SAMPLE_ID", "sample_id"] if c in sv_cols),
        None,
    )
    site1_col = next((c for c in ["Site1_Hugo_Symbol", "site1_hugo_symbol", "Gene1"] if c in sv_cols), None)
    site2_col = next((c for c in ["Site2_Hugo_Symbol", "site2_hugo_symbol", "Gene2"] if c in sv_cols), None)
    class_col = next((c for c in ["Class", "SV_Class", "Event_Info", "sv_class"] if c in sv_cols), None)

    if not sample_col:
        logger.warning("Cannot find sample_id column in SV table for %s", study_id)
        return []

    site1_expr = f'sv."{site1_col}"' if site1_col else "NULL"
    site2_expr = f'sv."{site2_col}"' if site2_col else "NULL"
    class_expr = f'sv."{class_col}"' if class_col else "NULL"

    try:
        conn.execute(f"ATTACH '{cache_db_path}' AS _cache (READ_ONLY)")

        # Pre-aggregate MOAlmanac fusions: best assertion per gene
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _moa_sv_best AS
            SELECT
                mf.gene,
                mab.score_bin             AS moalmanac_score_bin,
                mab.oncogenic             AS moalmanac_oncogenic,
                mab.clinical_significance AS moalmanac_clinical_significance,
                mab.drug                  AS moalmanac_drug,
                mab.disease               AS moalmanac_disease,
                ROW_NUMBER() OVER (
                    PARTITION BY mf.gene
                    ORDER BY
                        CASE mab.clinical_significance
                            WHEN 'FDA-Approved'     THEN 1
                            WHEN 'Guideline'        THEN 2
                            WHEN 'Clinical trial'   THEN 3
                            WHEN 'Clinical evidence' THEN 4
                            WHEN 'Preclinical'      THEN 5
                            WHEN 'Inferential'      THEN 6
                            ELSE 7
                        END
                ) AS rn
            FROM _cache.moalmanac_features_bulk mf
            JOIN _cache.moalmanac_assertions_bulk mab ON mf.feature_id = mab.feature_id
            WHERE mf.feature_type = 'fusion'
        """)

        sql = f"""
        WITH sv_base AS (
            SELECT
                sv."{sample_col}"   AS sample_id,
                {site1_expr}        AS gene1,
                {site2_expr}        AS gene2,
                {class_expr}        AS sv_class
            FROM {table} sv
        ),
        sv_genes AS (
            SELECT sample_id, gene1 AS hugo_symbol, gene2 AS sv_partner_gene, sv_class FROM sv_base WHERE gene1 IS NOT NULL
            UNION ALL
            SELECT sample_id, gene2 AS hugo_symbol, gene1 AS sv_partner_gene, sv_class FROM sv_base WHERE gene2 IS NOT NULL AND gene2 != gene1
        )
        SELECT
            sg.sample_id,
            sg.hugo_symbol,
            sg.sv_partner_gene,
            sg.sv_class,
            moa.moalmanac_score_bin,
            moa.moalmanac_oncogenic,
            moa.moalmanac_clinical_significance,
            moa.moalmanac_drug,
            moa.moalmanac_disease
        FROM sv_genes sg
        LEFT JOIN _moa_sv_best moa
            ON sg.hugo_symbol = moa.gene
            AND moa.rn = 1
        """

        raw_rows = conn.execute(sql).fetchall()
        col_names = [
            "sample_id", "hugo_symbol", "sv_partner_gene", "sv_class",
            "moalmanac_score_bin", "moalmanac_oncogenic",
            "moalmanac_clinical_significance", "moalmanac_drug", "moalmanac_disease",
        ]

    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS _moa_sv_best")
        except Exception:
            pass
        try:
            conn.execute("DETACH _cache")
        except Exception:
            pass

    rows_out = []
    for raw in raw_rows:
        r = dict(zip(col_names, raw))
        rows_out.append({
            "study_id": study_id,
            "alteration_type": "SV",
            "sample_id": r["sample_id"],
            "hugo_symbol": r["hugo_symbol"],
            "hgvsp_short": None,
            "variant_classification": None,
            "cna_value": None,
            "sv_class": r.get("sv_class"),
            "sv_partner_gene": r.get("sv_partner_gene"),
            "vep_impact": None,
            "vep_consequence": None,
            "vep_transcript_id": None,
            "vep_exon_number": None,
            "am_score": None,
            "am_class": None,
            "hotspot_type": None,
            "mutation_effect": None,
            "mutation_effect_source": None,
            "moalmanac_score_bin": r.get("moalmanac_score_bin"),
            "moalmanac_oncogenic": r.get("moalmanac_oncogenic"),
            "moalmanac_clinical_significance": r.get("moalmanac_clinical_significance"),
            "moalmanac_drug": r.get("moalmanac_drug"),
            "moalmanac_disease": r.get("moalmanac_disease"),
            "civic_evidence_id": None,
            "civic_evidence_level": None,
            "civic_clinical_significance": None,
            "civic_drugs": None,
            "intogen_role": None,
            "oncokb_oncogenic": None,
            "oncokb_mutation_effect": None,
            "oncokb_highest_sensitive_level": None,
            "annotated_at": None,
        })

    return rows_out
