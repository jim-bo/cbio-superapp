"""OncoPrint + Results View data queries — mutations, CNA, SV, clinical tracks, lollipop."""
from __future__ import annotations

import re

# Mutation type → simplified display type (mirrors DataUtils.ts getSimplifiedMutationType)
_VARIANT_TO_DISP = {
    "Missense_Mutation": "missense",
    "Nonsense_Mutation": "trunc",
    "Frame_Shift_Del": "trunc",
    "Frame_Shift_Ins": "trunc",
    "Stop_Codon_Del": "trunc",
    "Stop_Codon_Ins": "trunc",
    "Nonstop_Mutation": "trunc",
    "In_Frame_Del": "inframe",
    "In_Frame_Ins": "inframe",
    "Splice_Site": "splice",
    "Splice_Region": "splice",
    "5'Flank": "promoter",  # TERT only (enforced at load time)
}

# Priority order: higher index = higher priority (last one wins when iterating)
# _rec suffix = driver (MoAlmanac FDA-Approved / Clinical evidence); ranked above VUS
_MUT_PRIORITY = [
    "other", "promoter", "missense", "inframe", "splice", "trunc",
    "other_rec", "promoter_rec", "missense_rec", "inframe_rec", "splice_rec", "trunc_rec",
]

_DRIVER_SIGNIFICANCE = {"FDA-Approved", "Clinical evidence"}


def _classify_mutation(variant_classification: str | None) -> str:
    """Map variant classification to simplified display type."""
    if not variant_classification:
        return "other"
    return _VARIANT_TO_DISP.get(variant_classification, "other")


def _mut_priority(disp_mut: str) -> int:
    try:
        return _MUT_PRIORITY.index(disp_mut)
    except ValueError:
        return -1


def get_oncoprint_data(
    conn,
    study_id: str,
    gene: str,
    sample_ids: list[str] | None = None,
) -> list[dict]:
    """Return one dict per sample with alteration display state for `gene`.

    Biology:
        For each sample in the study, returns the pre-computed alteration flags
        used by OncoprintJS to color each cell. Multiple mutations in the same
        sample are collapsed to the highest-priority type.

    Engineering:
        Mirrors the data contract expected by OncoprintJS 6.x GENETIC rule sets.
        CNA values: 2→amp, -2→homdel, 1→gain, -1→hetloss (0/null = no event).
        Mutations: UNCALLED excluded; TERT 5'Flank already kept at load time.
    """
    # 1. Build sample universe
    sample_filter = ""
    params: list = []
    if sample_ids:
        placeholders = ", ".join(["?" for _ in sample_ids])
        sample_filter = f"WHERE SAMPLE_ID IN ({placeholders})"
        params.extend(sample_ids)

    samples_sql = f'SELECT SAMPLE_ID, PATIENT_ID FROM "{study_id}_sample" {sample_filter}'
    samples = conn.execute(samples_sql, params).fetchall()
    if not samples:
        return []

    all_sample_ids = [row[0] for row in samples]
    sample_to_patient = {row[0]: row[1] for row in samples}

    # 2. Fetch mutations for this gene
    mut_rows: dict[str, list[tuple[str, bool, bool]]] = {}  # sample_id → [(disp, is_germline)]
    try:
        cols = {c[0] for c in conn.execute(f'DESCRIBE "{study_id}_mutations"').fetchall()}
        has_mutations_table = True
    except Exception:
        has_mutations_table = False

    if has_mutations_table:
        sample_col = "Tumor_Sample_Barcode" if "Tumor_Sample_Barcode" in cols else "SAMPLE_ID"
        # Check for variant_annotations table (driver status)
        has_va = False
        try:
            conn.execute(f'SELECT 1 FROM "{study_id}_variant_annotations" LIMIT 1')
            has_va = True
        except Exception:
            pass

        if has_va:
            mut_sql = f"""
                SELECT
                    m.{sample_col} AS sample_id,
                    m.Variant_Classification,
                    COALESCE(m.Mutation_Status, '') AS mutation_status,
                    va.moalmanac_clinical_significance
                FROM "{study_id}_mutations" m
                LEFT JOIN "{study_id}_variant_annotations" va
                    ON va.study_id = m.study_id
                    AND va.sample_id = m.{sample_col}
                    AND va.hugo_symbol = m.Hugo_Symbol
                    AND va.alteration_type = 'MUTATION'
                    AND va.variant_classification = m.Variant_Classification
                    AND va.hgvsp_short = m.HGVSp_Short
                WHERE m.Hugo_Symbol = ?
                  AND COALESCE(m.Mutation_Status, '') != 'UNCALLED'
            """
        else:
            mut_sql = f"""
                SELECT
                    {sample_col} AS sample_id,
                    Variant_Classification,
                    COALESCE(Mutation_Status, '') AS mutation_status,
                    NULL AS moalmanac_clinical_significance
                FROM "{study_id}_mutations"
                WHERE Hugo_Symbol = ?
                  AND COALESCE(Mutation_Status, '') != 'UNCALLED'
            """
        for row in conn.execute(mut_sql, [gene]).fetchall():
            sid, vc, mut_status, clin_sig = row
            if sid not in mut_rows:
                mut_rows[sid] = []
            disp = _classify_mutation(vc)
            if clin_sig in _DRIVER_SIGNIFICANCE:
                disp += "_rec"
            is_germline = mut_status.lower() == "germline"
            mut_rows[sid].append((disp, is_germline))

    # 3. Fetch CNA for this gene
    cna_map: dict[str, str] = {}
    try:
        cna_sql = f"""
            SELECT sample_id, cna_value
            FROM "{study_id}_cna"
            WHERE hugo_symbol = ?
        """
        for row in conn.execute(cna_sql, [gene]).fetchall():
            sid, val = row
            if val == 2:
                cna_map[sid] = "amp"
            elif val == -2:
                cna_map[sid] = "homdel"
            elif val == 1:
                cna_map[sid] = "gain"
            elif val == -1:
                cna_map[sid] = "hetloss"
    except Exception:
        pass

    # 4. Fetch SV for this gene (with driver status from variant_annotations)
    sv_map: dict[str, str] = {}  # sample_id → "sv_rec" | "sv"
    try:
        # Check for variant_annotations
        sv_has_va = False
        try:
            conn.execute(f'SELECT 1 FROM "{study_id}_variant_annotations" LIMIT 1')
            sv_has_va = True
        except Exception:
            pass

        if sv_has_va:
            sv_sql = f"""
                SELECT COALESCE(s.Sample_Id, s.SAMPLE_ID) AS sample_id,
                       va.moalmanac_clinical_significance
                FROM "{study_id}_sv" s
                LEFT JOIN "{study_id}_variant_annotations" va
                    ON va.study_id = s.study_id
                    AND va.sample_id = COALESCE(s.Sample_Id, s.SAMPLE_ID)
                    AND va.hugo_symbol = ?
                    AND va.alteration_type = 'SV'
                WHERE s.Site1_Hugo_Symbol = ? OR s.Site2_Hugo_Symbol = ?
            """
            for row in conn.execute(sv_sql, [gene, gene, gene]).fetchall():
                sid, clin_sig = row
                is_driver = clin_sig in _DRIVER_SIGNIFICANCE
                # Keep highest priority: sv_rec > sv
                if sid not in sv_map or (is_driver and sv_map[sid] == "sv"):
                    sv_map[sid] = "sv_rec" if is_driver else "sv"
        else:
            sv_sql = f"""
                SELECT COALESCE(Sample_Id, SAMPLE_ID) AS sample_id
                FROM "{study_id}_sv"
                WHERE Site1_Hugo_Symbol = ? OR Site2_Hugo_Symbol = ?
            """
            for row in conn.execute(sv_sql, [gene, gene]).fetchall():
                sv_map[row[0]] = "sv"
    except Exception:
        pass

    # 5. Profiling status (check gene_panel table)
    not_profiled_mut: set[str] = set()
    not_profiled_cna: set[str] = set()
    try:
        gp_cols = {c[0] for c in conn.execute(f'DESCRIBE "{study_id}_gene_panel"').fetchall()}
        # If panel has 'mutations' column, check targeted panels
        if "mutations" in gp_cols:
            # Get gene panel definitions to check if gene is covered
            covered_panels_sql = """
                SELECT DISTINCT panel_id FROM gene_panel_definitions WHERE hugo_gene_symbol = ?
            """
            covered_panels = {row[0] for row in conn.execute(covered_panels_sql, [gene]).fetchall()}
            # WES/WGS panels are always profiled; targeted panels only if gene is on them
            gp_sql = f"""
                SELECT SAMPLE_ID, CAST(mutations AS VARCHAR) AS panel_id
                FROM "{study_id}_gene_panel"
            """
            for row in conn.execute(gp_sql).fetchall():
                sid, panel_id = row
                if panel_id is None or panel_id.upper() in ("NA", ""):
                    continue
                if panel_id.upper() in ("WES", "WXS", "WGS", "WHOLE_EXOME", "WHOLE_GENOME"):
                    continue  # always profiled
                if panel_id not in covered_panels:
                    not_profiled_mut.add(sid)
        if "cna" in gp_cols:
            covered_panels_sql = """
                SELECT DISTINCT panel_id FROM gene_panel_definitions WHERE hugo_gene_symbol = ?
            """
            covered_panels = {row[0] for row in conn.execute(covered_panels_sql, [gene]).fetchall()}
            gp_sql = f"""
                SELECT SAMPLE_ID, CAST(cna AS VARCHAR) AS panel_id
                FROM "{study_id}_gene_panel"
            """
            for row in conn.execute(gp_sql).fetchall():
                sid, panel_id = row
                if panel_id is None or panel_id.upper() in ("NA", ""):
                    continue
                if panel_id.upper() in ("WES", "WXS", "WGS", "WHOLE_EXOME", "WHOLE_GENOME"):
                    continue
                if panel_id not in covered_panels:
                    not_profiled_cna.add(sid)
    except Exception:
        pass

    # 6. Assemble result per sample
    result = []
    for sid in all_sample_ids:
        # Collapse mutations to highest-priority type
        disp_mut: str | None = None
        disp_germ = False
        if sid in mut_rows:
            best = max(mut_rows[sid], key=lambda t: _mut_priority(t[0]))
            disp_mut = best[0]
            disp_germ = best[1]

        result.append({
            "uid": sid,
            "patient": sample_to_patient.get(sid, sid),
            "disp_mut": disp_mut,
            "disp_cna": cna_map.get(sid),
            "disp_structuralVariant": sv_map.get(sid),
            "disp_germ": disp_germ,
            "not_profiled_for_mutations": sid in not_profiled_mut,
            "not_profiled_for_cna": sid in not_profiled_cna,
            "na": (sid in not_profiled_mut and sid in not_profiled_cna and sid not in sv_map),
        })

    return result


def get_clinical_track_options(conn, study_id: str) -> list[dict]:
    """Return clinical attributes sorted by data completeness (DESC).

    Used to populate the Tracks dropdown panel in OncoPrint.
    """
    try:
        # Check clinical_attribute_meta exists
        conn.execute("SELECT 1 FROM clinical_attribute_meta LIMIT 1")
    except Exception:
        return []

    try:
        n_samples = conn.execute(
            f'SELECT COUNT(*) FROM "{study_id}_sample"'
        ).fetchone()[0]
        if n_samples == 0:
            return []
    except Exception:
        return []

    # Get all attributes with their completeness fraction
    sql = """
        SELECT attr_id, display_name, datatype, patient_attribute
        FROM clinical_attribute_meta
        WHERE study_id = ?
          AND priority > 0
        ORDER BY priority DESC, attr_id
    """
    attrs = conn.execute(sql, [study_id]).fetchall()
    if not attrs:
        return []

    result = []
    for attr_id, display_name, datatype, patient_attribute in attrs:
        # Compute completeness
        try:
            if patient_attribute:
                count_sql = f"""
                    SELECT COUNT(*) FROM "{study_id}_sample" s
                    JOIN "{study_id}_patient" p ON s.PATIENT_ID = p.PATIENT_ID
                    WHERE p."{attr_id}" IS NOT NULL
                """
                non_null = conn.execute(count_sql).fetchone()[0]
            else:
                count_sql = f"""
                    SELECT COUNT(*) FROM "{study_id}_sample"
                    WHERE "{attr_id}" IS NOT NULL
                """
                non_null = conn.execute(count_sql).fetchone()[0]
            freq = round(non_null / n_samples, 4) if n_samples > 0 else 0.0
        except Exception:
            freq = 0.0

        result.append({
            "attr_id": attr_id,
            "display_name": display_name or attr_id,
            "datatype": datatype or "STRING",
            "patient_attribute": bool(patient_attribute),
            "freq": freq,
        })

    # Sort by freq DESC
    result.sort(key=lambda x: -x["freq"])
    return result


def get_clinical_track_data(conn, study_id: str, attr_ids: list[str]) -> dict[str, dict]:
    """Return per-sample clinical values for the requested attribute IDs.

    Returns: {sample_id: {attr_id: value, ...}, ...}
    """
    if not attr_ids:
        return {}

    # Determine which attrs are patient-level vs sample-level
    patient_attrs: list[str] = []
    sample_attrs: list[str] = []
    try:
        rows = conn.execute(
            f"""
            SELECT attr_id, patient_attribute FROM clinical_attribute_meta
            WHERE study_id = ? AND attr_id IN ({', '.join('?' for _ in attr_ids)})
            """,
            [study_id] + list(attr_ids),
        ).fetchall()
        patient_attrs = [r[0] for r in rows if r[1]]
        sample_attrs = [r[0] for r in rows if not r[1]]
    except Exception:
        sample_attrs = list(attr_ids)

    result: dict[str, dict] = {}

    # Sample-level attrs
    if sample_attrs:
        try:
            quoted = ", ".join(f'"{a}"' for a in sample_attrs)
            sql = f'SELECT SAMPLE_ID, {quoted} FROM "{study_id}_sample"'
            for row in conn.execute(sql).fetchall():
                sid = row[0]
                if sid not in result:
                    result[sid] = {}
                for i, attr_id in enumerate(sample_attrs):
                    val = row[i + 1]
                    if val is not None:
                        result[sid][attr_id] = str(val)
        except Exception:
            pass

    # Patient-level attrs
    if patient_attrs:
        try:
            quoted = ", ".join(f'p."{a}"' for a in patient_attrs)
            sql = f"""
                SELECT s.SAMPLE_ID, {quoted}
                FROM "{study_id}_sample" s
                JOIN "{study_id}_patient" p ON s.PATIENT_ID = p.PATIENT_ID
            """
            for row in conn.execute(sql).fetchall():
                sid = row[0]
                if sid not in result:
                    result[sid] = {}
                for i, attr_id in enumerate(patient_attrs):
                    val = row[i + 1]
                    if val is not None:
                        result[sid][attr_id] = str(val)
        except Exception:
            pass

    return result


# ── Mutations Tab ─────────────────────────────────────────────────────────────

# Variant classifications that are drivers by convention (truncating + hotspot missense)
# Per cBioPortal: putative driver = OncoKB/hotspot annotated; fallback = truncating
_TRUNC_VCS = {
    "Nonsense_Mutation", "Frame_Shift_Del", "Frame_Shift_Ins",
    "Stop_Codon_Del", "Stop_Codon_Ins", "Nonstop_Mutation",
    "Splice_Site", "Splice_Region",
}


def _parse_hgvsp_position(hgvsp_short: str | None) -> int | None:
    """Extract residue number from p.G12D → 12, p.R175H → 175, p.E294* → 294."""
    if not hgvsp_short:
        return None
    m = re.search(r"[A-Za-z*]+(\d+)", hgvsp_short.replace("p.", ""))
    if m:
        return int(m.group(1))
    return None


def get_lollipop_data(conn, study_id: str, gene: str) -> dict:
    """Aggregate mutation positions for the lollipop plot.

    Returns:
        {
            "mutations": [{"position": int, "count": int, "mut_type": str,
                           "hgvsp_short": str, "hotspot": bool}],
            "protein_length": int | None,
            "total_mutations": int,
            "total_samples": int,
        }
    """
    result = {
        "mutations": [],
        "protein_length": None,
        "total_mutations": 0,
        "total_samples": 0,
    }
    try:
        conn.execute(f'SELECT 1 FROM "{study_id}_mutations" LIMIT 1')
    except Exception:
        return result

    # ── Count total samples and mutations for this gene ──────────────────────
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*), COUNT(DISTINCT "Tumor_Sample_Barcode")
            FROM "{study_id}_mutations"
            WHERE "Hugo_Symbol" = ?
              AND ("Mutation_Status" IS NULL OR UPPER("Mutation_Status") != 'UNCALLED')
            """,
            [gene],
        ).fetchone()
        result["total_mutations"] = row[0] if row else 0
        result["total_samples"] = row[1] if row else 0
    except Exception:
        pass

    # ── Aggregate by HGVSp_Short + position ─────────────────────────────────
    try:
        # Use Protein_position when available; fall back to parsing HGVSp_Short
        has_pp = False
        try:
            conn.execute(
                f'SELECT "Protein_position" FROM "{study_id}_mutations" LIMIT 1'
            )
            has_pp = True
        except Exception:
            pass

        if has_pp:
            rows = conn.execute(
                f"""
                SELECT "HGVSp_Short",
                       CAST("Protein_position" AS INTEGER) AS pos,
                       "Variant_Classification",
                       COUNT(*) AS cnt,
                       COUNT(DISTINCT "Tumor_Sample_Barcode") AS sample_cnt
                FROM "{study_id}_mutations"
                WHERE "Hugo_Symbol" = ?
                  AND "Protein_position" IS NOT NULL
                  AND ("Mutation_Status" IS NULL OR UPPER("Mutation_Status") != 'UNCALLED')
                GROUP BY 1, 2, 3
                ORDER BY cnt DESC
                """,
                [gene],
            ).fetchall()
        else:
            # fall back – positions will be None (plot degrades to table-only)
            rows = []
    except Exception:
        rows = []

    # ── Check variant_annotations for hotspot data ───────────────────────────
    hotspot_positions: set[int] = set()
    try:
        ha_table = f"{study_id}_variant_annotations"
        hs_rows = conn.execute(
            f"""
            SELECT DISTINCT CAST("Protein_position" AS INTEGER)
            FROM "{study_id}_mutations" m
            JOIN "{ha_table}" va
              ON va.hugo_symbol = m."Hugo_Symbol"
             AND va.hgvsp_short = m."HGVSp_Short"
             AND va.sample_id = m."Tumor_Sample_Barcode"
            WHERE m."Hugo_Symbol" = ?
              AND va.hotspot_type IS NOT NULL
              AND m."Protein_position" IS NOT NULL
            """,
            [gene],
        ).fetchall()
        hotspot_positions = {r[0] for r in hs_rows if r[0] is not None}
    except Exception:
        pass

    # ── Protein length ────────────────────────────────────────────────────────
    if rows:
        max_pos = max((r[1] for r in rows if r[1] is not None), default=None)
        if max_pos is not None:
            result["protein_length"] = max_pos  # lower bound; caller fetches pfam

    # ── Build lollipop points ─────────────────────────────────────────────────
    mut_points = []
    for hgvsp, pos, vc, cnt, sample_cnt in rows:
        if pos is None:
            continue
        mut_type = _VARIANT_TO_DISP.get(vc or "", "other")
        mut_points.append({
            "position": pos,
            "count": sample_cnt,      # number of patients (samples)
            "mut_count": cnt,         # total mutation calls at this position
            "mut_type": mut_type,
            "hgvsp_short": hgvsp or "",
            "hotspot": pos in hotspot_positions,
        })

    result["mutations"] = mut_points
    return result


def get_mutation_summary(conn, study_id: str, gene: str) -> dict:
    """Return per-type mutation count summary for the right-side panel.

    Returns:
        {
            "total_mutations": int,
            "mutated_samples": int,
            "total_samples": int,
            "by_type": {"missense": {"driver": N, "vus": N}, ...},
            "has_annotations": bool,
        }
    """
    summary: dict = {
        "total_mutations": 0,
        "mutated_samples": 0,
        "total_samples": 0,
        "by_type": {},
        "has_annotations": False,
    }
    try:
        conn.execute(f'SELECT 1 FROM "{study_id}_mutations" LIMIT 1')
    except Exception:
        return summary

    # Total samples in study
    try:
        summary["total_samples"] = conn.execute(
            f'SELECT COUNT(*) FROM "{study_id}_sample"'
        ).fetchone()[0]
    except Exception:
        pass

    # Mutations for this gene
    try:
        rows = conn.execute(
            f"""
            SELECT "Variant_Classification", COUNT(*) AS cnt,
                   COUNT(DISTINCT "Tumor_Sample_Barcode") AS sample_cnt
            FROM "{study_id}_mutations"
            WHERE "Hugo_Symbol" = ?
              AND ("Mutation_Status" IS NULL OR UPPER("Mutation_Status") != 'UNCALLED')
            GROUP BY 1
            """,
            [gene],
        ).fetchall()
    except Exception:
        rows = []

    total_mut = 0
    mutated_samples_by_vc: dict[str, int] = {}
    vc_counts: dict[str, dict] = {}
    for vc, cnt, sample_cnt in rows:
        disp = _VARIANT_TO_DISP.get(vc or "", "other")
        if disp not in vc_counts:
            vc_counts[disp] = {"driver": 0, "vus": 0}
        # Simple driver rule: truncating = always driver; others = VUS until annotated
        if (vc or "") in _TRUNC_VCS:
            vc_counts[disp]["driver"] += cnt
        else:
            vc_counts[disp]["vus"] += cnt
        total_mut += cnt
        mutated_samples_by_vc[disp] = mutated_samples_by_vc.get(disp, 0) + sample_cnt

    # Upgrade missense/splice to driver if hotspot_type is set in annotations
    has_annotations = False
    try:
        ann_table = f"{study_id}_variant_annotations"
        ann_rows = conn.execute(
            f"""
            SELECT va."Variant_Classification", COUNT(*) AS n
            FROM "{study_id}_mutations" m
            JOIN "{ann_table}" va
              ON va.hugo_symbol = m."Hugo_Symbol"
             AND va.hgvsp_short = m."HGVSp_Short"
             AND va.sample_id = m."Tumor_Sample_Barcode"
            WHERE m."Hugo_Symbol" = ?
              AND va.hotspot_type IS NOT NULL
            GROUP BY 1
            """,
            [gene],
        ).fetchall()
        has_annotations = True
        for vc, n in ann_rows:
            disp = _VARIANT_TO_DISP.get(vc or "", "other")
            if disp in vc_counts:
                upgrade = min(n, vc_counts[disp]["vus"])
                vc_counts[disp]["vus"] -= upgrade
                vc_counts[disp]["driver"] += upgrade
    except Exception:
        pass

    summary["total_mutations"] = total_mut
    summary["mutated_samples"] = sum(mutated_samples_by_vc.values())
    summary["by_type"] = vc_counts
    summary["has_annotations"] = has_annotations
    return summary


_ALLOWED_SORT_COLS = {
    "HGVSp_Short", "Variant_Classification", "Tumor_Sample_Barcode",
    "t_alt_count", "t_depth", "Protein_position",
}


def get_mutations_table(
    conn,
    study_id: str,
    gene: str,
    page: int = 1,
    page_size: int = 25,
    sort_col: str = "Protein_position",
    sort_dir: str = "ASC",
) -> dict:
    """Return paginated mutations for the mutation table.

    Returns:
        {
            "total": int,
            "page": int,
            "page_size": int,
            "rows": [{sample_id, cancer_type, hgvsp_short, mutation_type,
                      allele_freq, mut_count, annotation, ...}]
        }
    """
    if sort_col not in _ALLOWED_SORT_COLS:
        sort_col = "Protein_position"
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "ASC"

    out: dict = {"total": 0, "page": page, "page_size": page_size, "rows": []}

    try:
        conn.execute(f'SELECT 1 FROM "{study_id}_mutations" LIMIT 1')
    except Exception:
        return out

    # Total count
    try:
        total = conn.execute(
            f"""
            SELECT COUNT(*) FROM "{study_id}_mutations"
            WHERE "Hugo_Symbol" = ?
              AND ("Mutation_Status" IS NULL OR UPPER("Mutation_Status") != 'UNCALLED')
            """,
            [gene],
        ).fetchone()[0]
        out["total"] = total
    except Exception:
        return out

    offset = (page - 1) * page_size

    # Check which optional columns exist
    try:
        mut_cols = {c[0] for c in conn.execute(f'DESCRIBE "{study_id}_mutations"').fetchall()}
    except Exception:
        return out

    t_alt = '"t_alt_count"' if "t_alt_count" in mut_cols else "NULL"
    t_dep = '"t_depth"' if "t_depth" in mut_cols else "NULL"
    ncbi_b = '"NCBI_Build"' if "NCBI_Build" in mut_cols else "NULL"
    prot_p = '"Protein_position"' if "Protein_position" in mut_cols else "NULL"

    # Check for sample clinical data (cancer type)
    has_sample_table = False
    try:
        conn.execute(f'SELECT 1 FROM "{study_id}_sample" LIMIT 1')
        has_sample_table = True
    except Exception:
        pass

    cancer_type_col = ""
    join_clause = ""
    if has_sample_table:
        try:
            samp_cols = {c[0] for c in conn.execute(
                f'DESCRIBE "{study_id}_sample"'
            ).fetchall()}
            if "CANCER_TYPE_DETAILED" in samp_cols:
                cancer_type_col = ', s."CANCER_TYPE_DETAILED" AS cancer_type'
                join_clause = (
                    f'LEFT JOIN "{study_id}_sample" s '
                    f'ON s."SAMPLE_ID" = m."Tumor_Sample_Barcode"'
                )
            elif "CANCER_TYPE" in samp_cols:
                cancer_type_col = ', s."CANCER_TYPE" AS cancer_type'
                join_clause = (
                    f'LEFT JOIN "{study_id}_sample" s '
                    f'ON s."SAMPLE_ID" = m."Tumor_Sample_Barcode"'
                )
        except Exception:
            pass

    # Check for annotation table
    has_annotations = False
    try:
        conn.execute(
            f'SELECT 1 FROM "{study_id}_variant_annotations" LIMIT 1'
        )
        has_annotations = True
    except Exception:
        pass

    ann_cols = ""
    ann_join = ""
    if has_annotations:
        ann_cols = (
            ", va.hotspot_type, va.moalmanac_drug, va.civic_evidence_id, "
            "va.mutation_effect, va.moalmanac_clinical_significance"
        )
        ann_join = (
            f'LEFT JOIN "{study_id}_variant_annotations" va '
            f'ON va.hugo_symbol = m."Hugo_Symbol" '
            f'AND va.hgvsp_short = m."HGVSp_Short" '
            f'AND va.sample_id = m."Tumor_Sample_Barcode" '
            f'AND va.alteration_type = \'MUTATION\''
        )

    # Count mutations per sample (for # Mut in Sample column)
    mut_count_cte = f"""
        WITH mut_per_sample AS (
            SELECT "Tumor_Sample_Barcode", COUNT(*) AS mut_count
            FROM "{study_id}_mutations"
            WHERE "Mutation_Status" IS NULL OR UPPER("Mutation_Status") != 'UNCALLED'
            GROUP BY 1
        )
    """

    sql = f"""
        {mut_count_cte}
        SELECT
            m."Tumor_Sample_Barcode"   AS sample_id,
            m."HGVSp_Short"            AS hgvsp_short,
            m."Variant_Classification" AS mutation_type,
            {prot_p}                   AS protein_position,
            {t_alt}                    AS t_alt_count,
            {t_dep}                    AS t_depth,
            {ncbi_b}                   AS ncbi_build,
            mps.mut_count              AS mut_count
            {cancer_type_col}
            {ann_cols}
        FROM "{study_id}_mutations" m
        LEFT JOIN mut_per_sample mps ON mps."Tumor_Sample_Barcode" = m."Tumor_Sample_Barcode"
        {join_clause}
        {ann_join}
        WHERE m."Hugo_Symbol" = ?
          AND (m."Mutation_Status" IS NULL OR UPPER(m."Mutation_Status") != 'UNCALLED')
        ORDER BY m."{sort_col}" {sort_dir} NULLS LAST
        LIMIT ? OFFSET ?
    """
    try:
        rows = conn.execute(sql, [gene, page_size, offset]).fetchall()
    except Exception:
        return out

    result_rows = []
    for row in rows:
        (sample_id, hgvsp, mut_type, prot_pos,
         t_alt_v, t_dep_v, ncbi_build, mut_count_v) = row[:8]
        idx = 8
        cancer_type = None
        if cancer_type_col:
            cancer_type = row[idx]
            idx += 1

        hotspot_type = drug = civic_id = mutation_effect = clin_sig = None
        if has_annotations:
            hotspot_type = row[idx]
            drug = row[idx + 1]
            civic_id = row[idx + 2]
            mutation_effect = row[idx + 3]
            clin_sig = row[idx + 4]

        # Allele frequency
        allele_freq = None
        if t_alt_v is not None and t_dep_v is not None and t_dep_v > 0:
            allele_freq = round(t_alt_v / t_dep_v, 3)

        result_rows.append({
            "sample_id": sample_id,
            "cancer_type": cancer_type,
            "hgvsp_short": hgvsp,
            "mutation_type": mut_type,
            "protein_position": prot_pos,
            "allele_freq": allele_freq,
            "mut_count": mut_count_v,
            "hotspot_type": hotspot_type,
            "moalmanac_drug": drug,
            "civic_evidence_id": civic_id,
            "mutation_effect": mutation_effect,
            "moalmanac_clinical_significance": clin_sig,
        })

    out["rows"] = result_rows
    return out
