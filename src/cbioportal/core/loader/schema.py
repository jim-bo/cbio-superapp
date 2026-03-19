"""DuckDB schema utilities: global union views, study categorization, and metadata."""
import os
from pathlib import Path

import typer
import yaml

from .gene_reference import get_oncotree_root

# Cache for study categories mapping
_CATEGORY_MAPPING = None


def load_category_mapping():
    """Load study-to-category mapping from study_categories.yaml."""
    global _CATEGORY_MAPPING
    if _CATEGORY_MAPPING is not None:
        return _CATEGORY_MAPPING

    mapping_path = Path(__file__).resolve().parent / "study_categories.yaml"
    if mapping_path.exists():
        with open(mapping_path, 'r') as f:
            raw_mapping = yaml.safe_load(f)
            _CATEGORY_MAPPING = {}
            for category, study_ids in raw_mapping.items():
                for sid in study_ids:
                    # First-wins: if a study appears in multiple categories,
                    # the first category in the YAML file takes precedence.
                    if sid.lower() not in _CATEGORY_MAPPING:
                        _CATEGORY_MAPPING[sid.lower()] = category
    else:
        _CATEGORY_MAPPING = {}
    return _CATEGORY_MAPPING


def categorize_study(conn, meta: dict, study_id: str):
    """Determine the category for a study (YAML or OncoTree root)."""
    sid = study_id.lower()
    mapping = load_category_mapping()
    if sid in mapping:
        return mapping[sid]
    raw_type = meta.get("type_of_cancer")
    return get_oncotree_root(conn, raw_type)


def load_study_metadata(conn, study_path: Path):
    """Load study metadata from meta_study.txt into the studies table."""
    from .discovery import parse_meta_file
    meta_file = study_path / "meta_study.txt"
    meta = parse_meta_file(meta_file) if meta_file.exists() else {}
    study_id = meta.get("cancer_study_identifier") or study_path.name
    category = categorize_study(conn, meta, study_id)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS studies (
            study_id VARCHAR PRIMARY KEY,
            type_of_cancer VARCHAR,
            name VARCHAR,
            description VARCHAR,
            short_name VARCHAR,
            public_study BOOLEAN,
            pmid VARCHAR,
            citation VARCHAR,
            groups VARCHAR,
            category VARCHAR
        )
    """)
    conn.execute("INSERT OR REPLACE INTO studies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
        study_id, meta.get("type_of_cancer"), meta.get("name") or study_id,
        meta.get("description"), meta.get("short_name"),
        meta.get("public_study", "false").lower() == "true",
        meta.get("pmid"), meta.get("citation"), meta.get("groups"), category
    ))
    return True


def create_global_views(conn):
    """Refresh unified views across all loaded study tables."""
    tables_res = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'").fetchall()
    tables = [t[0] for t in tables_res]

    # Standard suffixes
    suffixes = {
        "patient": "clinical_patient",
        "sample": "clinical_sample",
        "mutations": "mutations",
        "gene_panel": "gene_panel_matrix",
        "sv": "sv",
        "cna": "cna"
    }

    # Dynamically find timeline suffixes
    for t in tables:
        if "_timeline_" in t:
            suffix = t.split("_timeline_")[-1]
            if f"timeline_{suffix}" not in suffixes:
                suffixes[f"timeline_{suffix}"] = f"timeline_{suffix}"

    for suffix_key, view_name in suffixes.items():
        # Handle timeline keys which might already have the 'timeline_' prefix
        suffix = suffix_key if suffix_key.startswith("timeline_") else f"_{suffix_key}"
        study_tables = [f'"{t}"' for t in tables if t.endswith(suffix) or (suffix_key == t.split("_")[-1] and not suffix_key.startswith("timeline_"))]

        # Refined logic for matching suffixes to avoid overlaps
        if not suffix_key.startswith("timeline_"):
            study_tables = [f'"{t}"' for t in tables if t.endswith(f"_{suffix_key}")]
        else:
            actual_suffix = suffix_key.replace("timeline_", "")
            study_tables = [f'"{t}"' for t in tables if t.endswith(f"_timeline_{actual_suffix}")]

        conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
        if study_tables:
            union_sql = " UNION ALL BY NAME ".join([f"SELECT * FROM {t}" for t in study_tables])
            conn.execute(f'CREATE VIEW "{view_name}" AS {union_sql}')
