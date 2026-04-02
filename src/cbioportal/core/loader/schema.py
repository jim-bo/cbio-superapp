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
        "cna": "cna",
        "expression": "expression",
        "protein": "protein",
        "methylation": "methylation",
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

    # Build pre-aggregated genomic event tables for fast study view queries.
    create_genomic_derived_tables(conn, tables)


def create_genomic_derived_tables(conn, tables: list[str] | None = None):
    """Pre-compute per-study genomic event tables with panel profiling baked in.

    Mirrors cBioPortal's ClickHouse `genomic_event_derived` strategy: at load time,
    denormalize mutations/CNA/SV with gene panel data so query-time joins are eliminated.

    For each study that has a mutations/cna/sv table, creates:
        "{study_id}_genomic_event_derived" with columns:
            study_id, hugo_symbol, sample_id, variant_type, cna_type, is_profiled

    The is_profiled flag pre-computes whether the sample's panel covers that gene,
    so the query-time profiled count is just SUM(is_profiled) grouped by gene.
    """
    if tables is None:
        tables = [t[0] for t in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
        ).fetchall()]

    # Find which studies have genomic data
    study_ids = set()
    for t in tables:
        for suffix in ("_mutations", "_cna", "_sv"):
            if t.endswith(suffix):
                study_ids.add(t[: -len(suffix)])
    if not study_ids:
        return

    # Check if gene_panel_definitions exists
    has_gpd = "gene_panel_definitions" in tables

    for study_id in sorted(study_ids):
        derived_table = f'"{study_id}_genomic_event_derived"'
        conn.execute(f"DROP TABLE IF EXISTS {derived_table}")

        # Determine which genomic tables exist
        has_mutations = f"{study_id}_mutations" in tables
        has_cna = f"{study_id}_cna" in tables
        has_sv = f"{study_id}_sv" in tables
        has_panel = f"{study_id}_gene_panel" in tables

        # Detect mutation sample column
        mut_sample_col = "Tumor_Sample_Barcode"
        if has_mutations:
            try:
                cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_mutations"').fetchall()]
                if "Tumor_Sample_Barcode" not in cols and "SAMPLE_ID" in cols:
                    mut_sample_col = "SAMPLE_ID"
            except Exception:
                pass

        # Detect SV gene/sample columns
        sv_gene_col, sv_sample_col = "Site1_Hugo_Symbol", "Sample_Id"
        if has_sv:
            try:
                sv_cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_sv"').fetchall()]
                if "Site1_Hugo_Symbol" not in sv_cols and "Gene1" in sv_cols:
                    sv_gene_col = "Gene1"
            except Exception:
                pass

        # Detect panel columns for each data type
        panel_cols = {}
        if has_panel:
            try:
                gp_cols = [r[0] for r in conn.execute(f'DESCRIBE "{study_id}_gene_panel"').fetchall()]
                for dtype, col in [("mutation", "mutations"), ("cna", "cna"), ("sv", "structural_variants")]:
                    if col in gp_cols:
                        panel_cols[dtype] = col
            except Exception:
                pass

        # Build UNION ALL of all variant types with profiling pre-computed
        parts = []

        if has_mutations:
            if "mutation" in panel_cols and has_gpd:
                parts.append(f"""
                    SELECT
                        '{study_id}' AS study_id,
                        m.Hugo_Symbol AS hugo_symbol,
                        m.{mut_sample_col} AS sample_id,
                        'mutation' AS variant_type,
                        NULL AS cna_type,
                        CASE
                            WHEN UPPER(CAST(gp.{panel_cols["mutation"]} AS VARCHAR))
                                IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME') THEN 1
                            WHEN EXISTS (
                                SELECT 1 FROM gene_panel_definitions gpd
                                WHERE gpd.panel_id = CAST(gp.{panel_cols["mutation"]} AS VARCHAR)
                                AND gpd.hugo_gene_symbol = m.Hugo_Symbol
                            ) THEN 1
                            WHEN gp.{panel_cols["mutation"]} IS NULL
                                OR CAST(gp.{panel_cols["mutation"]} AS VARCHAR) = 'NA' THEN 0
                            ELSE 0
                        END AS is_profiled
                    FROM "{study_id}_mutations" m
                    LEFT JOIN "{study_id}_gene_panel" gp ON m.{mut_sample_col} = gp.SAMPLE_ID
                    WHERE COALESCE(m.Mutation_Status, '') != 'UNCALLED'
                    AND m.Hugo_Symbol IS NOT NULL
                """)
            else:
                parts.append(f"""
                    SELECT
                        '{study_id}' AS study_id,
                        Hugo_Symbol AS hugo_symbol,
                        {mut_sample_col} AS sample_id,
                        'mutation' AS variant_type,
                        NULL AS cna_type,
                        1 AS is_profiled
                    FROM "{study_id}_mutations"
                    WHERE COALESCE(Mutation_Status, '') != 'UNCALLED'
                    AND Hugo_Symbol IS NOT NULL
                """)

        if has_cna:
            if "cna" in panel_cols and has_gpd:
                parts.append(f"""
                    SELECT
                        '{study_id}' AS study_id,
                        c.hugo_symbol,
                        c.sample_id,
                        'cna' AS variant_type,
                        CASE WHEN c.cna_value >= 2 THEN 'AMP' ELSE 'HOMDEL' END AS cna_type,
                        CASE
                            WHEN UPPER(CAST(gp.{panel_cols["cna"]} AS VARCHAR))
                                IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME') THEN 1
                            WHEN EXISTS (
                                SELECT 1 FROM gene_panel_definitions gpd
                                WHERE gpd.panel_id = CAST(gp.{panel_cols["cna"]} AS VARCHAR)
                                AND gpd.hugo_gene_symbol = c.hugo_symbol
                            ) THEN 1
                            WHEN gp.{panel_cols["cna"]} IS NULL
                                OR CAST(gp.{panel_cols["cna"]} AS VARCHAR) = 'NA' THEN 0
                            ELSE 0
                        END AS is_profiled
                    FROM "{study_id}_cna" c
                    LEFT JOIN "{study_id}_gene_panel" gp ON c.sample_id = gp.SAMPLE_ID
                    WHERE (c.cna_value >= 2 OR c.cna_value <= -1.5)
                    AND c.hugo_symbol NOT IN ('CDKN2Ap14ARF', 'CDKN2Ap16INK4A')
                    AND c.hugo_symbol IS NOT NULL
                """)
            else:
                parts.append(f"""
                    SELECT
                        '{study_id}' AS study_id,
                        hugo_symbol,
                        sample_id,
                        'cna' AS variant_type,
                        CASE WHEN cna_value >= 2 THEN 'AMP' ELSE 'HOMDEL' END AS cna_type,
                        1 AS is_profiled
                    FROM "{study_id}_cna"
                    WHERE (cna_value >= 2 OR cna_value <= -1.5)
                    AND hugo_symbol NOT IN ('CDKN2Ap14ARF', 'CDKN2Ap16INK4A')
                    AND hugo_symbol IS NOT NULL
                """)

        if has_sv:
            if "sv" in panel_cols and has_gpd:
                parts.append(f"""
                    SELECT
                        '{study_id}' AS study_id,
                        sv.{sv_gene_col} AS hugo_symbol,
                        sv.{sv_sample_col} AS sample_id,
                        'structural_variant' AS variant_type,
                        NULL AS cna_type,
                        CASE
                            WHEN UPPER(CAST(gp.{panel_cols["sv"]} AS VARCHAR))
                                IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME') THEN 1
                            WHEN EXISTS (
                                SELECT 1 FROM gene_panel_definitions gpd
                                WHERE gpd.panel_id = CAST(gp.{panel_cols["sv"]} AS VARCHAR)
                                AND gpd.hugo_gene_symbol = sv.{sv_gene_col}
                            ) THEN 1
                            WHEN gp.{panel_cols["sv"]} IS NULL
                                OR CAST(gp.{panel_cols["sv"]} AS VARCHAR) = 'NA' THEN 0
                            ELSE 0
                        END AS is_profiled
                    FROM "{study_id}_sv" sv
                    LEFT JOIN "{study_id}_gene_panel" gp ON sv.{sv_sample_col} = gp.SAMPLE_ID
                    WHERE sv.{sv_gene_col} IS NOT NULL AND sv.{sv_gene_col} != ''
                """)
            else:
                parts.append(f"""
                    SELECT
                        '{study_id}' AS study_id,
                        {sv_gene_col} AS hugo_symbol,
                        {sv_sample_col} AS sample_id,
                        'structural_variant' AS variant_type,
                        NULL AS cna_type,
                        1 AS is_profiled
                    FROM "{study_id}_sv"
                    WHERE {sv_gene_col} IS NOT NULL AND {sv_gene_col} != ''
                """)

        if not parts:
            continue

        union_sql = " UNION ALL ".join(parts)
        conn.execute(f"CREATE TABLE {derived_table} AS {union_sql}")

        # Build a profiled-sample-count table: for each (gene, variant_type),
        # how many filtered samples are profiled. This is the denominator for freq.
        profiled_table = f'"{study_id}_profiled_counts"'
        conn.execute(f"DROP TABLE IF EXISTS {profiled_table}")

        # For profiled counts, we need ALL samples (not just those with events).
        # WES samples are profiled for all genes; targeted panel samples only for panel genes.
        profiled_parts = []
        for vtype, pcol_key in [("mutation", "mutation"), ("cna", "cna"), ("structural_variant", "sv")]:
            if pcol_key == "mutation" and not has_mutations:
                continue
            if pcol_key == "cna" and not has_cna:
                continue
            if pcol_key == "sv" and not has_sv:
                continue
            if pcol_key in panel_cols and has_gpd:
                # Get the gene list for this variant type from the derived table
                gene_source = f'"{study_id}_mutations"' if pcol_key == "mutation" else (
                    f'"{study_id}_cna"' if pcol_key == "cna" else f'"{study_id}_sv"'
                )
                gene_col = "Hugo_Symbol" if pcol_key == "mutation" else (
                    "hugo_symbol" if pcol_key == "cna" else sv_gene_col
                )
                col = panel_cols[pcol_key]
                profiled_parts.append(f"""
                    SELECT gpd.hugo_gene_symbol AS hugo_symbol, gp.SAMPLE_ID, '{vtype}' AS variant_type
                    FROM "{study_id}_gene_panel" gp
                    JOIN gene_panel_definitions gpd ON CAST(gp.{col} AS VARCHAR) = gpd.panel_id
                    WHERE UPPER(CAST(gp.{col} AS VARCHAR)) NOT IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME')
                    AND gp.{col} IS NOT NULL AND CAST(gp.{col} AS VARCHAR) != 'NA'
                    UNION ALL
                    SELECT g.{gene_col} AS hugo_symbol, gp.SAMPLE_ID, '{vtype}' AS variant_type
                    FROM "{study_id}_gene_panel" gp
                    CROSS JOIN (SELECT DISTINCT {gene_col} FROM {gene_source} WHERE {gene_col} IS NOT NULL) g
                    WHERE UPPER(CAST(gp.{col} AS VARCHAR)) IN ('WES','WXS','WGS','WHOLE_EXOME','WHOLE_GENOME')
                """)

        if profiled_parts:
            profiled_union = " UNION ALL ".join(profiled_parts)
            conn.execute(f"""
                CREATE TABLE {profiled_table} AS
                SELECT hugo_symbol, variant_type, COUNT(DISTINCT SAMPLE_ID) AS n_profiled
                FROM ({profiled_union})
                GROUP BY hugo_symbol, variant_type
            """)
