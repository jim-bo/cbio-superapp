"""Study repository: all homepage DB queries and study metadata loading."""

DATA_TYPE_OPTIONS = [
    ("mutation", "Mutations"),
    ("cna", "Copy Number"),
    ("mrna", "mRNA Expression"),
    ("methylation", "Methylation"),
    ("protein", "Protein"),
    ("sv", "Structural Variants"),
    ("segment", "Segments"),
    ("treatment", "Treatment"),
]

# Special study collections shown above the cancer type list in the sidebar.
SPECIAL_COLLECTIONS = [
    ("PanCancer Studies", "PanCancer Studies"),
    ("Pediatric Cancer Studies", "Pediatric Cancer Studies"),
    ("Immunogenomic Studies", "Immunogenomic Studies"),
    ("Cell Lines", "Cell Lines"),
    ("PreCancerous/Healthy Studies", "PreCancerous/Healthy Studies"),
]

def load_study_names(conn) -> dict[str, str]:
    """Load human-readable study names from the studies table in the DB."""
    rows = conn.execute("SELECT study_id, name FROM studies").fetchall()
    return {study_id: name for study_id, name in rows if name}

def get_study_catalog(
    conn,
    study_names: dict[str, str],
    cancer_type: str | None = None,
    data_types: list[str] | None = None,
) -> list[dict]:
    """Return list of study dicts filtered by category or data types."""
    where_clauses = []
    params: list = []

    if cancer_type and cancer_type != "All":
        where_clauses.append("s.category = ?")
        params.append(cancer_type)

    if data_types:
        placeholders = ", ".join("?" * len(data_types))
        where_clauses.append(
            f"s.study_id IN (SELECT study_id FROM study_data_types WHERE data_type IN ({placeholders}))"
        )
        params.extend(data_types)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
        SELECT
            s.study_id,
            s.category,
            COALESCE(counts.sample_count, 0) AS sample_count,
            list(DISTINCT sdt.data_type) AS data_types,
            s.description,
            s.pmid
        FROM studies s
        LEFT JOIN (
            SELECT study_id, COUNT(DISTINCT SAMPLE_ID) as sample_count
            FROM clinical_sample
            GROUP BY study_id
        ) counts ON s.study_id = counts.study_id
        LEFT JOIN study_data_types sdt ON s.study_id = sdt.study_id
        {where_sql}
        GROUP BY s.study_id, s.category, counts.sample_count, s.description, s.pmid
        ORDER BY s.study_id
    """

    rows = conn.execute(sql, params).fetchall()
    return [
        {
            "id": study_id,
            "name": study_names.get(study_id, study_id),
            "cancer_type": cat,
            "sample_count": sample_count,
            "data_types": dtypes or [],
            "description": description,
            "pmid": pmid,
        }
        for study_id, cat, sample_count, dtypes, description, pmid in rows
    ]

def get_cancer_type_counts(
    conn,
    data_types: list[str] | None = None,
) -> tuple[dict[str, int], dict[str, int]]:
    """Return (organ_system_counts, special_collection_counts)."""
    params: list = []
    dt_filter = ""
    if data_types:
        placeholders = ", ".join("?" * len(data_types))
        dt_filter = f"WHERE s.study_id IN (SELECT study_id FROM study_data_types WHERE data_type IN ({placeholders}))"
        params.extend(data_types)

    # Get counts for ALL categories
    sql = f"""
        SELECT category, COUNT(*) as n
        FROM studies s
        {dt_filter}
        GROUP BY category
    """
    rows = conn.execute(sql, params).fetchall()
    all_counts = {cat: n for cat, n in rows if cat}

    # Split into Special and Organ
    special_keys = {item[0] for item in SPECIAL_COLLECTIONS}
    
    special_counts = {k: all_counts.get(k, 0) for k in special_keys}
    organ_counts = {k: v for k, v in all_counts.items() if k not in special_keys}

    return organ_counts, special_counts


# ── Query form helpers ───────────────────────────────────────────────────────

# Map data_type values to display labels for genomic profiles
_PROFILE_LABELS = {
    "mutation": "Mutations",
    "sv": "Structural Variant",
    "cna": "Putative copy-number alterations from GISTIC",
}

_PROFILE_ORDER = ["mutation", "sv", "cna"]


def get_query_form_context(conn, study_ids: list[str]) -> dict:
    """Build template context for the query-by-gene form."""
    # Study metadata
    studies_info = []
    total_samples = 0
    all_data_types: set[str] = set()

    for sid in study_ids:
        try:
            row = conn.execute(
                "SELECT study_id, name FROM studies WHERE study_id = ?", [sid]
            ).fetchone()
            name = row[1] if row else sid
        except Exception:
            name = sid

        try:
            n = conn.execute(f'SELECT COUNT(*) FROM "{sid}_sample"').fetchone()[0]
        except Exception:
            n = 0

        try:
            dts = conn.execute(
                "SELECT data_type FROM study_data_types WHERE study_id = ?", [sid]
            ).fetchall()
            dt_set = {r[0] for r in dts}
        except Exception:
            dt_set = set()

        studies_info.append({"id": sid, "name": name, "samples": n, "data_types": dt_set})
        total_samples += n
        all_data_types |= dt_set

    # Genomic profiles (only show if study has the data type)
    profiles = []
    for dt in _PROFILE_ORDER:
        if dt in all_data_types:
            profiles.append({"id": dt, "label": _PROFILE_LABELS.get(dt, dt), "checked": True})

    # Case set options
    case_sets = _build_case_sets(conn, study_ids[0], all_data_types) if study_ids else []

    return {
        "studies_info": studies_info,
        "study_ids_str": ",".join(study_ids),
        "total_samples": total_samples,
        "profiles": profiles,
        "case_sets": case_sets,
    }


def _build_case_sets(conn, study_id: str, data_types: set[str]) -> list[dict]:
    """Build case set options with sample counts."""
    try:
        total = conn.execute(f'SELECT COUNT(*) FROM "{study_id}_sample"').fetchone()[0]
    except Exception:
        return [{"id": "all", "name": "All samples", "count": 0}]

    sets = [{"id": f"{study_id}_all", "name": "All samples", "count": total}]

    # Samples with mutation and CNA data
    if "mutation" in data_types and "cna" in data_types:
        try:
            n = conn.execute(f"""
                SELECT COUNT(DISTINCT SAMPLE_ID) FROM "{study_id}_gene_panel"
                WHERE mutations IS NOT NULL AND cna IS NOT NULL
            """).fetchone()[0]
            sets.append({
                "id": f"{study_id}_cnaseq",
                "name": "Samples with mutation and CNA data",
                "count": n,
            })
        except Exception:
            pass

    # Samples with mutation data
    if "mutation" in data_types:
        try:
            n = conn.execute(f"""
                SELECT COUNT(DISTINCT SAMPLE_ID) FROM "{study_id}_gene_panel"
                WHERE mutations IS NOT NULL
            """).fetchone()[0]
            sets.append({
                "id": f"{study_id}_sequenced",
                "name": "Samples with mutation data",
                "count": n,
            })
        except Exception:
            pass

    # Samples with CNA data
    if "cna" in data_types:
        try:
            n = conn.execute(f"""
                SELECT COUNT(DISTINCT SAMPLE_ID) FROM "{study_id}_gene_panel"
                WHERE cna IS NOT NULL
            """).fetchone()[0]
            sets.append({
                "id": f"{study_id}_cna",
                "name": "Samples with CNA data",
                "count": n,
            })
        except Exception:
            pass

    return sets


def validate_genes(conn, gene_text: str) -> dict:
    """Validate gene symbols against gene_reference table."""
    symbols = [g.strip().upper() for g in gene_text.replace(",", " ").split() if g.strip()]
    if not symbols:
        return {"valid": [], "invalid": [], "aliases": {}}

    valid = []
    invalid = []
    aliases = {}

    # Check against gene_reference
    placeholders = ", ".join(["?"] * len(symbols))
    try:
        rows = conn.execute(
            f"SELECT UPPER(hugo_gene_symbol) FROM gene_reference WHERE UPPER(hugo_gene_symbol) IN ({placeholders})",
            symbols,
        ).fetchall()
        known = {r[0] for r in rows}
    except Exception:
        known = set()

    # Check aliases if gene_alias table exists
    alias_map = {}
    try:
        rows = conn.execute(
            f"SELECT UPPER(alias), hugo_symbol FROM gene_alias WHERE UPPER(alias) IN ({placeholders})",
            symbols,
        ).fetchall()
        alias_map = {r[0]: r[1] for r in rows}
    except Exception:
        pass

    for sym in symbols:
        if sym in known:
            valid.append(sym)
        elif sym in alias_map:
            valid.append(alias_map[sym])
            aliases[sym] = alias_map[sym]
        else:
            invalid.append(sym)

    return {"valid": valid, "invalid": invalid, "aliases": aliases}
