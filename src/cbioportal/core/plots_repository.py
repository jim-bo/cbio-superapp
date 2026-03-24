"""Plots + Cancer Types Summary data queries for the results view."""
from __future__ import annotations

from cbioportal.core.oncoprint_repository import _classify_mutation

# CNA value → alteration type label
_CNA_MAP = {2: "amplification", -2: "deep_deletion", 1: "gain", -1: "shallow_deletion"}


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
        return _build_bar_data(h_values, v_values, common_ids)
    elif h_numeric and v_numeric:
        return _build_scatter_data(h_values, v_values, common_ids)
    else:
        return _build_box_data(h_values, v_values, common_ids)


def _get_axis_values(conn, study_id: str, config: dict) -> dict:
    """Return {values: {sample_id: value}, is_numeric: bool, label: str}."""
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
        return {"values": values, "is_numeric": False, "label": f"{gene}: Mutation Type"}

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

    all_samples = conn.execute(
        f'SELECT SAMPLE_ID FROM "{study_id}_sample"'
    ).fetchall()
    all_ids = {r[0] for r in all_samples}

    cna_rows = conn.execute(
        f'SELECT sample_id, cna_value FROM "{study_id}_cna" WHERE hugo_symbol = ?',
        [gene],
    ).fetchall()

    _CNA_LABELS = {
        2: "Amplification", -2: "Deep Deletion",
        1: "Gain", -1: "Shallow Deletion", 0: "Diploid",
    }

    values = {}
    for sid, val in cna_rows:
        if sid in all_ids:
            values[sid] = _CNA_LABELS.get(int(val), "Diploid")
    # Fill missing as Diploid
    for sid in all_ids:
        if sid not in values:
            values[sid] = "Diploid"

    return {"values": values, "is_numeric": False, "label": f"{gene}: Copy Number"}


def _build_bar_data(h_values: dict, v_values: dict, common_ids: set) -> dict:
    """Build stacked bar chart data for discrete × discrete."""
    from collections import defaultdict

    cross: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for sid in common_ids:
        h_val = str(h_values["values"][sid])
        v_val = str(v_values["values"][sid])
        cross[h_val][v_val] += 1

    categories = sorted(cross.keys())
    # Collect all v-axis values
    all_v_values: set[str] = set()
    for counts in cross.values():
        all_v_values.update(counts.keys())
    series_names = sorted(all_v_values)

    series = []
    for name in series_names:
        data = [cross[cat].get(name, 0) for cat in categories]
        series.append({"name": name, "data": data})

    return {
        "plot_type": "bar",
        "h_label": h_values["label"],
        "v_label": v_values["label"],
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
    for cat in categories:
        vals = sorted(groups[cat])
        if not vals:
            box_data.append([0, 0, 0, 0, 0])
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

    return {
        "plot_type": "box",
        "h_label": cat_vals["label"],
        "v_label": num_vals["label"],
        "categories": categories,
        "box_data": box_data,
        "total_samples": len(common_ids),
        "swapped": swapped,
    }
