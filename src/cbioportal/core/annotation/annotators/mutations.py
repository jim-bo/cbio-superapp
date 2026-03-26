"""Mutation annotator — joins study mutations against cache reference tables.

Each mutation row produces exactly ONE output row (best available annotation).
MOAlmanac and CIViC are pre-aggregated to their best match per (gene, alteration).
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Clinical significance priority for MOAlmanac (lower = better)
MOALMANAC_PRIORITY = {
    "FDA-Approved": 1,
    "Guideline": 2,
    "Clinical trial": 3,
    "Clinical evidence": 4,
    "Preclinical": 5,
    "Inferential": 6,
}

# Evidence level priority for CIViC (A = best)
CIVIC_LEVEL_PRIORITY = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}


def annotate_mutations(
    conn,
    study_id: str,
    cache_db_path: str,
    vep_lookup: dict | None = None,
) -> list[dict]:
    """Annotate mutations with MOAlmanac, CIViC, IntOGen, and vibe-vep results.

    Each input mutation row maps to exactly one output row.
    """
    table = f'"{study_id}_mutations"'

    existing = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        (f"{study_id}_mutations",),
    ).fetchone()
    if not existing:
        logger.warning("No mutations table for study %s — skipping", study_id)
        return []

    mut_cols = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            (f"{study_id}_mutations",),
        ).fetchall()
    }

    sample_col = "Tumor_Sample_Barcode" if "Tumor_Sample_Barcode" in mut_cols else "SAMPLE_ID"
    hgvsp_col = "HGVSp_Short" if "HGVSp_Short" in mut_cols else None
    vc_col = "Variant_Classification" if "Variant_Classification" in mut_cols else None

    hgvsp_expr = f'"{hgvsp_col}"' if hgvsp_col else "NULL"
    vc_expr = f'"{vc_col}"' if vc_col else "NULL"
    # For stripped hgvsp (no "p." prefix) used in joins — must include table alias
    hgvsp_stripped = f"REGEXP_REPLACE(COALESCE(m.\"{hgvsp_col}\", ''), '^p\\.', '')" if hgvsp_col else "''"

    chr_col = "Chromosome" if "Chromosome" in mut_cols else None
    start_col = "Start_Position" if "Start_Position" in mut_cols else None
    ref_col = "Reference_Allele" if "Reference_Allele" in mut_cols else None
    alt_col = "Tumor_Seq_Allele2" if "Tumor_Seq_Allele2" in mut_cols else None

    chr_expr = f'"{chr_col}"' if chr_col else "NULL"
    start_expr = f'"{start_col}"' if start_col else "NULL"
    ref_expr = f'"{ref_col}"' if ref_col else "NULL"
    alt_expr = f'"{alt_col}"' if alt_col else "NULL"

    try:
        conn.execute(f"ATTACH '{cache_db_path}' AS _cache (READ_ONLY)")

        # ── Pre-aggregate MOAlmanac: best assertion per (gene, alteration) ────
        # "Best" = lowest priority number in MOALMANAC_PRIORITY
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _moa_best AS
            SELECT
                mf.gene,
                mf.alteration,
                mab.score_bin         AS moalmanac_score_bin,
                mab.oncogenic         AS moalmanac_oncogenic,
                mab.clinical_significance AS moalmanac_clinical_significance,
                mab.drug              AS moalmanac_drug,
                mab.disease           AS moalmanac_disease,
                ROW_NUMBER() OVER (
                    PARTITION BY mf.gene, mf.alteration
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
            WHERE mf.feature_type = 'somatic_variant'
        """)

        # ── Pre-aggregate CIViC: best predictive/functional evidence per (gene, hgvsp) ─
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _civic_best AS
            SELECT
                gene,
                hgvsp_short,
                evidence_id           AS civic_evidence_id,
                evidence_level        AS civic_evidence_level,
                clinical_significance AS civic_clinical_significance,
                drugs                 AS civic_drugs,
                ROW_NUMBER() OVER (
                    PARTITION BY gene, hgvsp_short
                    ORDER BY
                        CASE evidence_type WHEN 'Predictive' THEN 1 WHEN 'Functional' THEN 2 ELSE 3 END,
                        CASE evidence_level WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 WHEN 'E' THEN 5 ELSE 6 END
                ) AS rn
            FROM _cache.civic_evidence
            WHERE hgvsp_short IS NOT NULL AND hgvsp_short <> ''
        """)

        # ── Best CIViC Functional evidence per (gene, hgvsp) for mutation_effect ─
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _civic_func AS
            SELECT
                gene,
                hgvsp_short,
                clinical_significance AS civic_func_significance,
                ROW_NUMBER() OVER (
                    PARTITION BY gene, hgvsp_short
                    ORDER BY CASE evidence_level WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5 END
                ) AS rn
            FROM _cache.civic_evidence
            WHERE evidence_type = 'Functional'
              AND hgvsp_short IS NOT NULL AND hgvsp_short <> ''
        """)

        # ── IntOGen: best match per gene (first cancer-type match or any) ───────
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _intogen_best AS
            SELECT
                symbol,
                role AS intogen_role,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY qvalue_combination ASC) AS rn
            FROM _cache.intogen_drivers
        """)

        # ── Main join (one row per mutation) ─────────────────────────────────
        sql = f"""
        SELECT
            m."{sample_col}"  AS sample_id,
            m.Hugo_Symbol     AS hugo_symbol,
            m.{hgvsp_expr}    AS hgvsp_short,
            m.{vc_expr}       AS variant_classification,
            m.{chr_expr}      AS chr,
            m.{start_expr}    AS start_pos,
            m.{ref_expr}      AS ref_allele,
            m.{alt_expr}      AS alt_allele,
            -- MOAlmanac
            moa.moalmanac_score_bin,
            moa.moalmanac_oncogenic,
            moa.moalmanac_clinical_significance,
            moa.moalmanac_drug,
            moa.moalmanac_disease,
            -- CIViC best
            cv.civic_evidence_id,
            cv.civic_evidence_level,
            cv.civic_clinical_significance,
            cv.civic_drugs,
            -- CIViC functional (for mutation_effect)
            cvf.civic_func_significance,
            -- IntOGen
            ig.intogen_role
        FROM {table} m
        LEFT JOIN _moa_best moa
            ON m.Hugo_Symbol = moa.gene
            AND {hgvsp_stripped} = COALESCE(moa.alteration, '')
            AND moa.rn = 1
        LEFT JOIN _civic_best cv
            ON m.Hugo_Symbol = cv.gene
            AND {hgvsp_stripped} = REGEXP_REPLACE(COALESCE(cv.hgvsp_short, ''), '^p\\.', '')
            AND cv.rn = 1
        LEFT JOIN _civic_func cvf
            ON m.Hugo_Symbol = cvf.gene
            AND {hgvsp_stripped} = REGEXP_REPLACE(COALESCE(cvf.hgvsp_short, ''), '^p\\.', '')
            AND cvf.rn = 1
        LEFT JOIN _intogen_best ig
            ON m.Hugo_Symbol = ig.symbol
            AND ig.rn = 1
        """

        raw_rows = conn.execute(sql).fetchall()
        col_names = [
            "sample_id", "hugo_symbol", "hgvsp_short", "variant_classification",
            "chr", "start_pos", "ref_allele", "alt_allele",
            "moalmanac_score_bin", "moalmanac_oncogenic", "moalmanac_clinical_significance",
            "moalmanac_drug", "moalmanac_disease",
            "civic_evidence_id", "civic_evidence_level", "civic_clinical_significance",
            "civic_drugs", "civic_func_significance",
            "intogen_role",
        ]

    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS _moa_best")
            conn.execute("DROP TABLE IF EXISTS _civic_best")
            conn.execute("DROP TABLE IF EXISTS _civic_func")
            conn.execute("DROP TABLE IF EXISTS _intogen_best")
        except Exception:
            pass
        try:
            conn.execute("DETACH _cache")
        except Exception:
            pass

    rows_out = []
    for raw in raw_rows:
        r = dict(zip(col_names, raw))

        civic_func = r.pop("civic_func_significance", None)
        mutation_effect, mutation_effect_source = _resolve_mutation_effect(
            civic_func, r.get("intogen_role")
        )

        # VEP lookup
        vep_ann = {}
        if vep_lookup is not None:
            key = (
                r.get("hugo_symbol", ""),
                str(r.pop("chr", "") or ""),
                str(r.pop("start_pos", "") or ""),
                str(r.pop("ref_allele", "") or ""),
                str(r.pop("alt_allele", "") or ""),
            )
            vep_ann = vep_lookup.get(key, {})
        else:
            r.pop("chr", None)
            r.pop("start_pos", None)
            r.pop("ref_allele", None)
            r.pop("alt_allele", None)

        rows_out.append({
            "study_id": study_id,
            "alteration_type": "MUTATION",
            "sample_id": r["sample_id"],
            "hugo_symbol": r["hugo_symbol"],
            "hgvsp_short": r.get("hgvsp_short"),
            "variant_classification": r.get("variant_classification"),
            "cna_value": None,
            "sv_class": None,
            "sv_partner_gene": None,
            "vep_impact": vep_ann.get("vep_impact"),
            "vep_consequence": vep_ann.get("vep_consequence"),
            "vep_transcript_id": vep_ann.get("vep_transcript_id"),
            "vep_exon_number": vep_ann.get("vep_exon_number"),
            "am_score": vep_ann.get("am_score"),
            "am_class": vep_ann.get("am_class"),
            "hotspot_type": vep_ann.get("hotspot_type"),
            "mutation_effect": mutation_effect,
            "mutation_effect_source": mutation_effect_source,
            "moalmanac_score_bin": r.get("moalmanac_score_bin"),
            "moalmanac_oncogenic": r.get("moalmanac_oncogenic"),
            "moalmanac_clinical_significance": r.get("moalmanac_clinical_significance"),
            "moalmanac_drug": r.get("moalmanac_drug"),
            "moalmanac_disease": r.get("moalmanac_disease"),
            "civic_evidence_id": r.get("civic_evidence_id"),
            "civic_evidence_level": r.get("civic_evidence_level"),
            "civic_clinical_significance": r.get("civic_clinical_significance"),
            "civic_drugs": r.get("civic_drugs"),
            "intogen_role": r.get("intogen_role"),
            "oncokb_oncogenic": None,
            "oncokb_mutation_effect": None,
            "oncokb_highest_sensitive_level": None,
            "annotated_at": None,
        })

    return rows_out


def _resolve_mutation_effect(
    civic_func_significance: str | None,
    intogen_role: str | None,
) -> tuple[str, str]:
    """Resolve (mutation_effect, mutation_effect_source) from best available source.

    CIViC Functional significance → mutation effect mapping:
        Gain-of-function, Activating, Gain of Function       → Gain-of-function
        Loss-of-function, Loss of Function, Dominant Negative,
        Neomorphic                                            → Loss-of-function
    IntOGen role: Act→GoF, LoF→LoF, Amb→Unknown
    """
    if civic_func_significance:
        sig_lower = civic_func_significance.lower()
        gof_terms = ("gain-of-function", "gain of function", "activating", "oncogenic")
        lof_terms = ("loss-of-function", "loss of function", "dominant negative", "neomorphic")
        if any(t in sig_lower for t in gof_terms):
            return "Gain-of-function", "civic"
        if any(t in sig_lower for t in lof_terms):
            return "Loss-of-function", "civic"

    if intogen_role:
        role_map = {
            "Act": ("Gain-of-function", "intogen"),
            "LoF": ("Loss-of-function", "intogen"),
            "Amb": ("Unknown", "intogen"),
        }
        if intogen_role in role_map:
            return role_map[intogen_role]

    return "Unknown", "unknown"
