"""Plots + Cancer Types Summary data queries for the results view."""
from __future__ import annotations

from cbioportal.core.oncoprint_repository import _classify_mutation

# CNA value → alteration type label
_CNA_MAP = {2: "amplification", -2: "deep_deletion", 1: "gain", -1: "shallow_deletion"}

# Legacy ref: PlotsTabUtils.tsx:2070-2072 — cnaCategoryOrder
# Order: ['-2','-1','0','1','2'] mapped through cnaToAppearance legendLabels
_CNA_CATEGORY_ORDER = [
    "Deep Deletion", "Shallow Deletion", "Diploid", "Gain", "Amplification",
]

# Fallback profile names used when the molecular_profiles table is missing or
# the study hasn't been reloaded yet.  Matches the most common profile_name
# values found in meta_*.txt across the datahub.
_FALLBACK_PROFILE_NAMES = {
    "mutation": "Mutations",
    "cna": "Putative copy-number alterations from GISTIC",
    "sv": "Structural variants",
}
# Maps our internal data_type keys to the stable_id used in meta_*.txt files.
_TYPE_TO_STABLE_ID = {"mutation": "mutations", "cna": "cna", "sv": "structural_variants"}


def get_molecular_profile_name(conn, study_id: str, data_type: str) -> str:
    """Return the display name for a molecular data type from the DB.

    Legacy ref: PlotsTab.tsx:2748-2800 — dataTypeToDataSourceOptions loads
    molecular profiles from the API and uses profile.name as dropdown labels.
    We replicate that by querying the molecular_profiles table populated at
    study load time from meta_*.txt files.

    Falls back to hardcoded names if the table doesn't exist or no row found.
    """
    stable_id = _TYPE_TO_STABLE_ID.get(data_type, data_type)
    try:
        row = conn.execute(
            "SELECT profile_name FROM molecular_profiles "
            "WHERE study_id = ? AND stable_id = ?",
            [study_id, stable_id],
        ).fetchone()
        if row and row[0]:
            return row[0]
    except Exception:
        pass  # table may not exist in older DBs
    return _FALLBACK_PROFILE_NAMES.get(data_type, data_type)


def get_molecular_profiles(conn, study_id: str, alteration_type: str = None) -> list[dict]:
    """Return molecular profiles for a study, optionally filtered by alteration type."""
    query = "SELECT * FROM molecular_profiles WHERE study_id = ?"
    params: list = [study_id]
    if alteration_type:
        query += " AND genetic_alteration_type = ?"
        params.append(alteration_type)
    try:
        rows = conn.execute(query, params).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, row)) for row in rows]
    except Exception:
        return []


def get_cancer_types_summary(
    conn,
    study_id: str,
    gene: str,
    group_by: str = "CANCER_TYPE",
    count_by: str = "patients",
) -> dict:
    """Return alteration breakdown for *gene* grouped by cancer type.

    Args:
        group_by: column name — CANCER_TYPE, CANCER_TYPE_DETAILED, or study_id (literal).
        count_by: "patients" or "samples".
    """
    # Validate group_by to prevent SQL injection
    allowed_groups = {"CANCER_TYPE", "CANCER_TYPE_DETAILED", "study_id"}
    if group_by not in allowed_groups:
        group_by = "CANCER_TYPE"

    count_col = "PATIENT_ID" if count_by == "patients" else "SAMPLE_ID"
    distinct_count = f"COUNT(DISTINCT {count_col})"

    # 1. Get all samples with their group column
    samples = conn.execute(
        f'SELECT SAMPLE_ID, PATIENT_ID, "{group_by}" '
        f'FROM "{study_id}_sample"'
    ).fetchall()
    if not samples:
        return {"categories": []}

    sample_group = {}  # sample_id -> group_name
    sample_patient = {}  # sample_id -> patient_id
    for sid, pid, grp in samples:
        sample_group[sid] = grp or "Unknown"
        sample_patient[sid] = pid

    all_sample_ids = set(sample_group.keys())

    # 2. Fetch mutations for this gene (excluding UNCALLED)
    mut_rows = conn.execute(
        f'SELECT Tumor_Sample_Barcode, Variant_Classification '
        f'FROM "{study_id}_mutations" '
        f"WHERE Hugo_Symbol = ? AND (Mutation_Status IS NULL OR Mutation_Status != 'UNCALLED')",
        [gene],
    ).fetchall()

    # 3. Fetch CNA for this gene
    cna_rows = conn.execute(
        f'SELECT sample_id, cna_value '
        f'FROM "{study_id}_cna" '
        f"WHERE hugo_symbol = ? AND cna_value != 0",
        [gene],
    ).fetchall()

    # 4. Fetch SV for this gene
    sv_rows = conn.execute(
        f'SELECT Sample_Id '
        f'FROM "{study_id}_sv" '
        f"WHERE Site1_Hugo_Symbol = ? OR Site2_Hugo_Symbol = ?",
        [gene, gene],
    ).fetchall()

    # 5. Build per-sample alteration sets
    sample_alts: dict[str, set[str]] = {}  # sample_id -> set of alteration types

    for sid, vc in mut_rows:
        if sid in all_sample_ids:
            sample_alts.setdefault(sid, set()).add("mutation")

    for sid, val in cna_rows:
        if sid in all_sample_ids:
            cna_type = _CNA_MAP.get(int(val))
            if cna_type:
                sample_alts.setdefault(sid, set()).add(cna_type)

    for (sid,) in sv_rows:
        if sid in all_sample_ids:
            sample_alts.setdefault(sid, set()).add("structural_variant")

    # 6. Aggregate by group
    from collections import defaultdict

    group_totals: dict[str, set[str]] = defaultdict(set)  # group -> set of count_col values
    group_alt_counts: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )  # group -> alt_type -> set of count_col values
    group_multiple: dict[str, set[str]] = defaultdict(set)

    for sid in all_sample_ids:
        grp = sample_group[sid]
        entity = sample_patient[sid] if count_by == "patients" else sid
        group_totals[grp].add(entity)

        alts = sample_alts.get(sid, set())
        if len(alts) > 1:
            group_multiple[grp].add(entity)
        for alt in alts:
            group_alt_counts[grp][alt].add(entity)

    # 7. Get profiling counts
    profiled = _get_profiling_counts(conn, study_id, sample_group, count_by, sample_patient)

    # 8. Build output
    alt_types = [
        "mutation", "structural_variant", "amplification",
        "deep_deletion", "gain", "shallow_deletion",
    ]
    categories = []
    for grp in sorted(group_totals.keys()):
        cat = {
            "name": grp,
            "total": len(group_totals[grp]),
        }
        for at in alt_types:
            cat[at] = len(group_alt_counts[grp].get(at, set()))
        cat["multiple"] = len(group_multiple[grp])
        cat["profiled"] = profiled.get(grp, {"mutation": 0, "cna": 0, "sv": 0})
        categories.append(cat)

    return {"categories": categories}


def _get_profiling_counts(
    conn, study_id: str, sample_group: dict, count_by: str, sample_patient: dict
) -> dict:
    """Return {group: {mutation: N, cna: N, sv: N}} profiling counts."""
    from collections import defaultdict

    result: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))

    try:
        rows = conn.execute(
            f'SELECT SAMPLE_ID, mutations, cna, structural_variants '
            f'FROM "{study_id}_gene_panel"'
        ).fetchall()
    except Exception:
        # If no gene_panel table, assume all profiled
        groups: dict[str, set] = defaultdict(set)
        for sid, grp in sample_group.items():
            entity = sample_patient[sid] if count_by == "patients" else sid
            groups[grp].add(entity)
        return {
            grp: {"mutation": len(entities), "cna": len(entities), "sv": len(entities)}
            for grp, entities in groups.items()
        }

    for sid, mut_panel, cna_panel, sv_panel in rows:
        if sid not in sample_group:
            continue
        grp = sample_group[sid]
        entity = sample_patient[sid] if count_by == "patients" else sid
        if mut_panel:
            result[grp]["mutation"].add(entity)
        if cna_panel:
            result[grp]["cna"].add(entity)
        if sv_panel:
            result[grp]["sv"].add(entity)

    return {
        grp: {k: len(v) for k, v in counts.items()}
        for grp, counts in result.items()
    }


def get_clinical_attribute_options(conn, study_id: str) -> list[dict]:
    """Return clinical attributes available for Plots axis dropdowns."""
    try:
        rows = conn.execute(
            "SELECT attr_id, display_name, datatype, patient_attribute "
            "FROM clinical_attribute_meta "
            "WHERE study_id = ? ORDER BY priority, display_name",
            [study_id],
        ).fetchall()
        return [
            {
                "attr_id": r[0],
                "display_name": r[1],
                "datatype": r[2],
                "patient_attribute": bool(r[3]),
            }
            for r in rows
        ]
    except Exception:
        # Fallback: derive from sample table columns
        cols = conn.execute(
            f'SELECT column_name FROM information_schema.columns '
            f"WHERE table_name = '{study_id}_sample' "
            f"AND column_name NOT IN ('study_id', 'SAMPLE_ID', 'PATIENT_ID')"
        ).fetchall()
        return [
            {
                "attr_id": c[0],
                "display_name": c[0].replace("_", " ").title(),
                "datatype": "STRING",
                "patient_attribute": False,
            }
            for c in cols
        ]


def get_plots_data(
    conn,
    study_id: str,
    h_config: dict,
    v_config: dict,
) -> dict:
    """Cross-tabulate two axis configurations and return chart-ready data.

    Each config: {"data_type": str, "attribute_id": str, "gene": str, "plot_by": str}
    """
    # Normalize camelCase keys from JS
    h_config = _normalize_config(h_config)
    v_config = _normalize_config(v_config)

    # Get per-sample values for each axis
    h_values = _get_axis_values(conn, study_id, h_config)
    v_values = _get_axis_values(conn, study_id, v_config)

    # Intersect samples present in both axes
    common_ids = set(h_values["values"].keys()) & set(v_values["values"].keys())
    if not common_ids:
        return {"plot_type": "bar", "categories": [], "series": [], "total_samples": 0}

    h_numeric = h_values["is_numeric"]
    v_numeric = v_values["is_numeric"]

    if not h_numeric and not v_numeric:
        return _build_bar_data(
            h_values, v_values, common_ids,
            h_data_type=h_config.get("data_type", ""),
            v_data_type=v_config.get("data_type", ""),
        )
    elif h_numeric and v_numeric:
        return _build_scatter_data(h_values, v_values, common_ids)
    else:
        return _build_box_data(h_values, v_values, common_ids)


def _normalize_config(config: dict) -> dict:
    """Normalize camelCase keys from JS to snake_case."""
    out = dict(config)
    if "dataType" in out:
        out["data_type"] = out.pop("dataType")
    if "attributeId" in out:
        out["attribute_id"] = out.pop("attributeId")
    if "plotBy" in out:
        out["plot_by"] = out.pop("plotBy")
    if "patientAttribute" in out:
        out["patient_attribute"] = out.pop("patientAttribute")
    return out


def _get_axis_values(conn, study_id: str, config: dict) -> dict:
    """Return {values: {sample_id: value}, is_numeric: bool, label: str}."""
    config = _normalize_config(config)
    data_type = config.get("data_type", "")

    if data_type == "clinical_attribute":
        return _get_clinical_axis(conn, study_id, config)
    elif data_type == "mutation":
        return _get_mutation_axis(conn, study_id, config)
    elif data_type == "structural_variant":
        return _get_sv_axis(conn, study_id, config)
    elif data_type == "copy_number":
        return _get_cna_axis(conn, study_id, config)
    else:
        return {"values": {}, "is_numeric": False, "label": "Unknown"}


def _get_clinical_axis(conn, study_id: str, config: dict) -> dict:
    """Get clinical attribute values per sample."""
    attr_id = config.get("attribute_id", "CANCER_TYPE")
    patient_attr = config.get("patient_attribute", False)

    if patient_attr:
        rows = conn.execute(
            f'SELECT s.SAMPLE_ID, p."{attr_id}" '
            f'FROM "{study_id}_sample" s '
            f'JOIN "{study_id}_patient" p ON s.PATIENT_ID = p.PATIENT_ID '
            f'WHERE p."{attr_id}" IS NOT NULL',
        ).fetchall()
    else:
        try:
            rows = conn.execute(
                f'SELECT SAMPLE_ID, "{attr_id}" '
                f'FROM "{study_id}_sample" '
                f'WHERE "{attr_id}" IS NOT NULL',
            ).fetchall()
        except Exception:
            return {"values": {}, "is_numeric": False, "label": attr_id}

    values = {r[0]: r[1] for r in rows}

    # Determine if numeric
    is_numeric = False
    if values:
        sample_val = next(iter(values.values()))
        is_numeric = isinstance(sample_val, (int, float))

    # Build label
    display_name = attr_id.replace("_", " ").title()
    try:
        meta = conn.execute(
            "SELECT display_name FROM clinical_attribute_meta "
            "WHERE study_id = ? AND attr_id = ?",
            [study_id, attr_id],
        ).fetchone()
        if meta:
            display_name = meta[0]
    except Exception:
        pass

    return {"values": values, "is_numeric": is_numeric, "label": display_name}


def _get_mutation_axis(conn, study_id: str, config: dict) -> dict:
    """Get mutation data per sample for a gene."""
    gene = config.get("gene", "")
    plot_by = config.get("plot_by", "mutated_vs_wildtype")

    # Get all samples
    all_samples = conn.execute(
        f'SELECT SAMPLE_ID FROM "{study_id}_sample"'
    ).fetchall()
    all_ids = {r[0] for r in all_samples}

    # Get mutations
    mut_rows = conn.execute(
        f'SELECT Tumor_Sample_Barcode, Variant_Classification '
        f'FROM "{study_id}_mutations" '
        f"WHERE Hugo_Symbol = ? AND (Mutation_Status IS NULL OR Mutation_Status != 'UNCALLED')",
        [gene],
    ).fetchall()

    if plot_by == "type":
        # Classify each sample by highest-priority mutation type
        sample_types: dict[str, str] = {}
        for sid, vc in mut_rows:
            if sid not in all_ids:
                continue
            disp = _classify_mutation(vc)
            if sid not in sample_types:
                sample_types[sid] = disp
            # Keep highest priority
            from cbioportal.core.oncoprint_repository import _mut_priority
            if _mut_priority(disp) > _mut_priority(sample_types[sid]):
                sample_types[sid] = disp

        values = {}
        for sid in all_ids:
            if sid in sample_types:
                values[sid] = sample_types[sid].replace("_", " ").title()
            else:
                values[sid] = "Wild Type"
        profile_name = get_molecular_profile_name(conn, study_id, "mutation")
        return {"values": values, "is_numeric": False, "label": f"{gene}: {profile_name}"}

    else:  # mutated_vs_wildtype
        mutated = {sid for sid, _ in mut_rows if sid in all_ids}
        values = {sid: ("Mutated" if sid in mutated else "Wild Type") for sid in all_ids}
        return {"values": values, "is_numeric": False, "label": f"{gene}: Mutated vs Wild-type"}


def _get_sv_axis(conn, study_id: str, config: dict) -> dict:
    """Get structural variant data per sample."""
    gene = config.get("gene", "")
    plot_by = config.get("plot_by", "variant_vs_no_variant")

    all_samples = conn.execute(
        f'SELECT SAMPLE_ID FROM "{study_id}_sample"'
    ).fetchall()
    all_ids = {r[0] for r in all_samples}

    sv_rows = conn.execute(
        f'SELECT Sample_Id FROM "{study_id}_sv" '
        f"WHERE Site1_Hugo_Symbol = ? OR Site2_Hugo_Symbol = ?",
        [gene, gene],
    ).fetchall()
    sv_samples = {r[0] for r in sv_rows if r[0] in all_ids}

    values = {
        sid: ("With Structural Variants" if sid in sv_samples else "No Structural Variants")
        for sid in all_ids
    }
    return {
        "values": values,
        "is_numeric": False,
        "label": f"{gene}: Variant vs No Variant",
    }


def _get_cna_axis(conn, study_id: str, config: dict) -> dict:
    """Get copy number alteration data per sample."""
    gene = config.get("gene", "")

    # Legacy ref: DiscreteCNACache.ts — only samples that were profiled for CNA
    # (i.e., appear in the CNA matrix for ANY gene) are eligible.  Un-profiled
    # samples are excluded, not counted as Diploid.  Our loader strips value=0
    # rows to save space, so we recover the full profiled set from all CNA rows.
    profiled = conn.execute(
        f'SELECT DISTINCT sample_id FROM "{study_id}_cna"'
    ).fetchall()
    profiled_ids = {r[0] for r in profiled}

    cna_rows = conn.execute(
        f'SELECT sample_id, cna_value FROM "{study_id}_cna" WHERE hugo_symbol = ?',
        [gene],
    ).fetchall()

    _CNA_LABELS = {
        2: "Amplification", -2: "Deep Deletion",
        1: "Gain", -1: "Shallow Deletion", 0: "Diploid",
    }

    # Legacy ref: DiscreteCNACache.ts — the API only returns rows with exact
    # integer CNA values {-2, -1, 0, 1, 2}.  Non-integer values (e.g. -1.5)
    # are GISTIC continuous scores that leaked into the discrete file and must
    # be excluded to match legacy behavior.
    _VALID_CNA = frozenset(_CNA_LABELS.keys())

    values = {}
    for sid, val in cna_rows:
        if sid in profiled_ids:
            int_val = int(val) if float(val) == int(val) else None
            if int_val in _VALID_CNA:
                values[sid] = _CNA_LABELS[int_val]
    # Fill profiled samples missing a value for this gene as Diploid
    for sid in profiled_ids:
        if sid not in values:
            values[sid] = "Diploid"

    profile_name = get_molecular_profile_name(conn, study_id, "cna")
    return {"values": values, "is_numeric": False, "label": f"{gene}: {profile_name}"}


def _build_bar_data(
    h_values: dict,
    v_values: dict,
    common_ids: set,
    h_data_type: str = "",
    v_data_type: str = "",
) -> dict:
    """Build stacked bar chart data for discrete × discrete."""
    from collections import defaultdict

    cross: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for sid in common_ids:
        h_val = str(h_values["values"][sid])
        v_val = str(v_values["values"][sid])
        cross[h_val][v_val] += 1

    # Legacy ref: MultipleCategoryBarPlotUtils.ts:88-105 — sortDataByCategory()
    # When axis is CNA, use fixed cnaCategoryOrder; otherwise alphabetical.
    # Only include categories that actually appear in the data
    # (legacy: usedMajorCategories/usedMinorCategories in makePlotData lines 31-61).
    if h_data_type == "copy_number":
        categories = [c for c in _CNA_CATEGORY_ORDER if c in cross]
    else:
        categories = sorted(cross.keys())

    all_v_values: set[str] = set()
    for counts in cross.values():
        all_v_values.update(counts.keys())

    if v_data_type == "copy_number":
        series_names = [c for c in _CNA_CATEGORY_ORDER if c in all_v_values]
    else:
        series_names = sorted(all_v_values)

    series = []
    for name in series_names:
        data = [cross[cat].get(name, 0) for cat in categories]
        series.append({"name": name, "data": data})

    return {
        "plot_type": "bar",
        "h_label": h_values["label"],
        "v_label": v_values["label"],
        "h_data_type": h_data_type,
        "v_data_type": v_data_type,
        "categories": categories,
        "series": series,
        "total_samples": len(common_ids),
    }


def _build_scatter_data(h_values: dict, v_values: dict, common_ids: set) -> dict:
    """Build scatter plot data for numeric × numeric."""
    points = []
    for sid in common_ids:
        h_val = h_values["values"][sid]
        v_val = v_values["values"][sid]
        try:
            points.append({"x": float(h_val), "y": float(v_val), "sample_id": sid})
        except (TypeError, ValueError):
            continue

    return {
        "plot_type": "scatter",
        "h_label": h_values["label"],
        "v_label": v_values["label"],
        "points": points,
        "total_samples": len(common_ids),
    }


def _build_box_data(h_values: dict, v_values: dict, common_ids: set) -> dict:
    """Build box plot data for numeric × discrete."""
    # Ensure numeric is on v-axis, discrete on h-axis
    if h_values["is_numeric"]:
        num_vals, cat_vals = h_values, v_values
        swapped = True
    else:
        num_vals, cat_vals = v_values, h_values
        swapped = False

    from collections import defaultdict
    import statistics

    groups: dict[str, list[float]] = defaultdict(list)
    for sid in common_ids:
        cat = str(cat_vals["values"][sid])
        try:
            val = float(num_vals["values"][sid])
            groups[cat].append(val)
        except (TypeError, ValueError):
            continue

    categories = sorted(groups.keys())
    box_data = []
    box_raw_data: dict[str, list[dict]] = {}
    for cat in categories:
        vals = sorted(groups[cat])
        if not vals:
            box_data.append([0, 0, 0, 0, 0])
            box_raw_data[cat] = []
            continue
        n = len(vals)
        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        box_data.append([
            vals[0],           # min
            vals[q1_idx],      # Q1
            vals[n // 2],      # median
            vals[q3_idx],      # Q3
            vals[-1],          # max
        ])
        # Raw sample values for scatter overlay coloring
        box_raw_data[cat] = [
            {"sample_id": sid, "value": float(num_vals["values"][sid])}
            for sid in common_ids
            if str(cat_vals["values"][sid]) == cat
        ]

    return {
        "plot_type": "box",
        "h_label": cat_vals["label"],
        "v_label": num_vals["label"],
        "categories": categories,
        "box_data": box_data,
        "box_raw_data": box_raw_data,
        "total_samples": len(common_ids),
        "swapped": swapped,
    }


# ── Coloring overlay ─────────────────────────────────────────────────────────

# Legacy ref: PlotsTabUtils.tsx:1909-1960 — oncoprintMutationTypeToAppearanceDefault
_MUT_TYPE_COLORS = {
    "Missense": "#008000",
    "Inframe": "#993404",
    "Truncating": "#000000",
    "Splice": "#e5802b",
    "Promoter": "#00B7CE",
    "Other": "#cf58bc",
    "Multiple": "#666666",
}

# Legacy ref: PlotsTabUtils.tsx:2020-2046 — cnaToAppearance
_CNA_OVERLAY_COLORS = {
    "Deep Deletion": "#0000ff",
    "Shallow Deletion": "#2aced4",
    "Diploid": "#BEBEBE",
    "Gain": "#ff8c9f",
    "Amplification": "#ff0000",
}

# Legacy ref: PlotsTabUtils.tsx:2048-2052
_SV_OVERLAY_COLOR = "#8B00C9"

# Legacy ref: Colors.ts:39-129 — clinical reserved colors
_CLINICAL_RESERVED_COLORS: dict[str, str] = {}
for _val in ("true", "yes", "positive", "alive", "living", "disease free",
             "tumor free", "not progressed"):
    _CLINICAL_RESERVED_COLORS[_val] = "#1b9e77"
for _val in ("false", "no", "negative", "deceased", "recurred", "progressed",
             "recurred/progressed", "with tumor"):
    _CLINICAL_RESERVED_COLORS[_val] = "#d95f02"
for _val in ("female", "f"):
    _CLINICAL_RESERVED_COLORS[_val] = "#E0699E"
for _val in ("male", "m"):
    _CLINICAL_RESERVED_COLORS[_val] = "#2986E2"
for _val in ("unknown", "na"):
    _CLINICAL_RESERVED_COLORS[_val] = "#D3D3D3"

# Legacy ref: PlotUtils.ts:221-253 — D3 categorical palette
_D3_PALETTE = [
    "#3366cc", "#dc3912", "#ff9900", "#109618", "#990099", "#0099c6",
    "#dd4477", "#66aa00", "#b82e2e", "#316395", "#994499", "#22aa99",
    "#aaaa11", "#6633cc", "#e67300", "#8b0707", "#651067", "#329262",
    "#5574a6", "#3b3eac", "#b77322", "#16d620", "#b91383", "#f4359e",
    "#9c5935", "#a9c413", "#2a778d", "#668d1c", "#bea413", "#0c5922",
    "#743411",
]


def get_color_data(
    conn,
    study_id: str,
    color_config: dict,
) -> dict:
    """Return per-sample color overlay data for scatter/box plots.

    color_config: {"type": "mutation"|"cna"|"sv"|"clinical", "gene": str, "attribute_id": str}

    Returns: {"samples": {sample_id: category}, "colors": {category: hex}, "order": [categories]}
    """
    color_type = color_config.get("type", "")

    if color_type == "mutation":
        return _get_mutation_color(conn, study_id, color_config.get("gene", ""))
    elif color_type == "cna":
        return _get_cna_color(conn, study_id, color_config.get("gene", ""))
    elif color_type == "sv":
        return _get_sv_color(conn, study_id, color_config.get("gene", ""))
    elif color_type == "clinical":
        return _get_clinical_color(conn, study_id, color_config.get("attribute_id", ""))
    else:
        return {"samples": {}, "colors": {}, "order": []}


def _get_mutation_color(conn, study_id: str, gene: str) -> dict:
    """Color by mutation type for a gene."""
    all_samples = conn.execute(
        f'SELECT SAMPLE_ID FROM "{study_id}_sample"'
    ).fetchall()
    all_ids = {r[0] for r in all_samples}

    mut_rows = conn.execute(
        f'SELECT Tumor_Sample_Barcode, Variant_Classification '
        f'FROM "{study_id}_mutations" '
        f"WHERE Hugo_Symbol = ? AND (Mutation_Status IS NULL OR Mutation_Status != 'UNCALLED')",
        [gene],
    ).fetchall()

    sample_types: dict[str, list[str]] = {}
    for sid, vc in mut_rows:
        if sid not in all_ids:
            continue
        disp = _classify_mutation(vc).replace("_", " ").title()
        sample_types.setdefault(sid, []).append(disp)

    samples: dict[str, str] = {}
    for sid in all_ids:
        types = sample_types.get(sid)
        if not types:
            samples[sid] = "Not mutated"
        elif len(set(types)) > 1:
            samples[sid] = "Multiple"
        else:
            samples[sid] = types[0]

    colors = dict(_MUT_TYPE_COLORS)
    colors["Not mutated"] = "#c4e5f5"

    # Legacy ref: PlotsTabUtils.tsx:2087-2097 — mutTypeCategoryOrder
    order = ["Missense", "Inframe", "Truncating", "Splice", "Promoter",
             "Other", "Multiple", "Not mutated"]

    return {"samples": samples, "colors": colors, "order": order}


def _get_cna_color(conn, study_id: str, gene: str) -> dict:
    """Color by CNA status for a gene."""
    profiled = conn.execute(
        f'SELECT DISTINCT sample_id FROM "{study_id}_cna"'
    ).fetchall()
    profiled_ids = {r[0] for r in profiled}

    cna_rows = conn.execute(
        f'SELECT sample_id, cna_value FROM "{study_id}_cna" WHERE hugo_symbol = ?',
        [gene],
    ).fetchall()

    _labels = {2: "Amplification", -2: "Deep Deletion", 1: "Gain", -1: "Shallow Deletion", 0: "Diploid"}
    _valid = frozenset(_labels.keys())

    samples: dict[str, str] = {}
    for sid, val in cna_rows:
        if sid in profiled_ids:
            int_val = int(val) if float(val) == int(val) else None
            if int_val in _valid:
                samples[sid] = _labels[int_val]
    for sid in profiled_ids:
        if sid not in samples:
            samples[sid] = "Diploid"

    return {
        "samples": samples,
        "colors": dict(_CNA_OVERLAY_COLORS),
        "order": _CNA_CATEGORY_ORDER,
    }


def _get_sv_color(conn, study_id: str, gene: str) -> dict:
    """Color by SV status for a gene."""
    all_samples = conn.execute(
        f'SELECT SAMPLE_ID FROM "{study_id}_sample"'
    ).fetchall()
    all_ids = {r[0] for r in all_samples}

    sv_rows = conn.execute(
        f'SELECT Sample_Id FROM "{study_id}_sv" '
        f"WHERE Site1_Hugo_Symbol = ? OR Site2_Hugo_Symbol = ?",
        [gene, gene],
    ).fetchall()
    sv_ids = {r[0] for r in sv_rows if r[0] in all_ids}

    samples = {
        sid: ("Structural Variant" if sid in sv_ids else "No Structural Variant")
        for sid in all_ids
    }
    return {
        "samples": samples,
        "colors": {"Structural Variant": _SV_OVERLAY_COLOR, "No Structural Variant": "#c4e5f5"},
        "order": ["Structural Variant", "No Structural Variant"],
    }


def _get_clinical_color(conn, study_id: str, attr_id: str) -> dict:
    """Color by clinical attribute value."""
    try:
        rows = conn.execute(
            f'SELECT SAMPLE_ID, "{attr_id}" FROM "{study_id}_sample" WHERE "{attr_id}" IS NOT NULL',
        ).fetchall()
    except Exception:
        return {"samples": {}, "colors": {}, "order": []}

    samples = {r[0]: str(r[1]) for r in rows}

    # Build color map: check reserved colors first, then use D3 palette
    unique_vals = sorted(set(samples.values()))
    colors: dict[str, str] = {}
    palette_idx = 0
    for val in unique_vals:
        reserved = _CLINICAL_RESERVED_COLORS.get(val.lower().strip())
        if reserved:
            colors[val] = reserved
        else:
            colors[val] = _D3_PALETTE[palette_idx % len(_D3_PALETTE)]
            palette_idx += 1

    return {"samples": samples, "colors": colors, "order": unique_vals}
