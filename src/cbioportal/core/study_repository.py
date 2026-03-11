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
