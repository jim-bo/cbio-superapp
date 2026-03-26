"""CNA annotator — joins study CNA alterations against cache reference tables.

Each CNA row (where cna_value = ±2) produces exactly ONE output row.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def annotate_cna(
    conn,
    study_id: str,
    cache_db_path: str,
) -> list[dict]:
    """Annotate CNA rows (±2 only) with MOAlmanac assertions.

    Uses pre-aggregation to ensure one best MOAlmanac assertion per (gene, alt_type).
    """
    table = f'"{study_id}_cna"'

    existing = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        (f"{study_id}_cna",),
    ).fetchone()
    if not existing:
        logger.debug("No CNA table for study %s — skipping", study_id)
        return []

    cna_cols = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            (f"{study_id}_cna",),
        ).fetchall()
    }
    sample_col = "sample_id" if "sample_id" in cna_cols else "SAMPLE_ID"

    try:
        conn.execute(f"ATTACH '{cache_db_path}' AS _cache (READ_ONLY)")

        # Pre-aggregate MOAlmanac: best assertion per (gene, alt_type)
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _moa_cna_best AS
            SELECT
                mf.gene,
                mf.alt_type,
                mab.score_bin             AS moalmanac_score_bin,
                mab.oncogenic             AS moalmanac_oncogenic,
                mab.clinical_significance AS moalmanac_clinical_significance,
                mab.drug                  AS moalmanac_drug,
                mab.disease               AS moalmanac_disease,
                ROW_NUMBER() OVER (
                    PARTITION BY mf.gene, mf.alt_type
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
            WHERE mf.feature_type = 'copy_number'
        """)

        sql = f"""
        SELECT
            c."{sample_col}"    AS sample_id,
            c.hugo_symbol       AS hugo_symbol,
            c.cna_value         AS cna_value,
            moa.moalmanac_score_bin,
            moa.moalmanac_oncogenic,
            moa.moalmanac_clinical_significance,
            moa.moalmanac_drug,
            moa.moalmanac_disease
        FROM {table} c
        LEFT JOIN _moa_cna_best moa
            ON c.hugo_symbol = moa.gene
            AND (
                (c.cna_value =  2 AND moa.alt_type = 'Amplification') OR
                (c.cna_value = -2 AND moa.alt_type = 'Deletion')
            )
            AND moa.rn = 1
        WHERE c.cna_value IN (2, -2)
        """

        raw_rows = conn.execute(sql).fetchall()
        col_names = [
            "sample_id", "hugo_symbol", "cna_value",
            "moalmanac_score_bin", "moalmanac_oncogenic",
            "moalmanac_clinical_significance", "moalmanac_drug", "moalmanac_disease",
        ]

    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS _moa_cna_best")
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
            "alteration_type": "CNA",
            "sample_id": r["sample_id"],
            "hugo_symbol": r["hugo_symbol"],
            "hgvsp_short": None,
            "variant_classification": None,
            "cna_value": r["cna_value"],
            "sv_class": None,
            "sv_partner_gene": None,
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
