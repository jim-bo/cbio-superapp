"""OncoPrint data queries — mutations, CNA, SV, clinical tracks."""
from __future__ import annotations

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
_MUT_PRIORITY = [
    "other", "promoter", "missense", "inframe", "splice", "trunc",
]


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
        mut_sql = f"""
            SELECT
                {sample_col} AS sample_id,
                Variant_Classification,
                COALESCE(Mutation_Status, '') AS mutation_status
            FROM "{study_id}_mutations"
            WHERE Hugo_Symbol = ?
              AND COALESCE(Mutation_Status, '') != 'UNCALLED'
        """
        for row in conn.execute(mut_sql, [gene]).fetchall():
            sid, vc, mut_status = row
            if sid not in mut_rows:
                mut_rows[sid] = []
            disp = _classify_mutation(vc)
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

    # 4. Fetch SV for this gene
    sv_set: set[str] = set()
    try:
        sv_sql = f"""
            SELECT COALESCE(Sample_Id, SAMPLE_ID) AS sample_id
            FROM "{study_id}_sv"
            WHERE Site1_Hugo_Symbol = ? OR Site2_Hugo_Symbol = ?
        """
        for row in conn.execute(sv_sql, [gene, gene]).fetchall():
            sv_set.add(row[0])
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
            "disp_structuralVariant": "sv" if sid in sv_set else None,
            "disp_germ": disp_germ,
            "not_profiled_for_mutations": sid in not_profiled_mut,
            "not_profiled_for_cna": sid in not_profiled_cna,
            "na": (sid in not_profiled_mut and sid in not_profiled_cna and sid not in sv_set),
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
