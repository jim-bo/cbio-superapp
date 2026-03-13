import os
import re
import gzip
import json
import time
import psutil
import duckdb
import requests
import yaml
from pathlib import Path
import typer

# Variant classifications excluded by cBioPortal at import time.
# "By default, cBioPortal filters out Silent, Intron, IGR, 3'UTR, 5'UTR,
# 3'Flank and 5'Flank, except for the promoter mutations of the TERT gene
# (5'Flank only)."
# Source: https://docs.cbioportal.org/file-formats/#mutation-data
# These never enter genomic_event_derived, so we must exclude them at load time
# to keep our sample counts consistent with the public portal.
_EXCLUDED_VCS = frozenset({
    "Silent", "Intron", "IGR", "3'UTR", "5'UTR", "3'Flank", "5'Flank",
})

class Monitor:
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.start_time = time.time()
        self.peak_memory = 0

    def get_metrics(self):
        current_mem = self.process.memory_info().rss / (1024 * 1024)
        if current_mem > self.peak_memory:
            self.peak_memory = current_mem
        elapsed = time.time() - self.start_time
        return {
            "current_memory_mb": current_mem,
            "peak_memory_mb": self.peak_memory,
            "elapsed_seconds": elapsed
        }

def get_source_path():
    """Get the path to the studies source (downloads or datahub)."""
    downloads = os.getenv("CBIO_DOWNLOADS")
    datahub = os.getenv("CBIO_DATAHUB")
    mode = os.getenv("CBIO_SOURCE_MODE", "downloads").lower()

    path_str = None
    if downloads and datahub:
        path_str = downloads if mode == "downloads" else datahub
    else:
        path_str = downloads or datahub

    if not path_str:
        return None
    
    path = Path(path_str)
    if not path.exists():
        typer.echo(f"Warning: Source path {path} does not exist.")
        return None
    
    return path

def discover_studies(datahub_path: Path):
    """Recursively find all directories that contain cBioPortal data files."""
    study_dirs = set()
    markers = ["meta_study.txt", "data_clinical_patient.txt", "data_clinical_sample.txt", "data_mutations.txt"]
    for marker in markers:
        for p in datahub_path.rglob(marker):
            study_dirs.add(p.parent)
    return sorted(list(study_dirs))


def find_study_path(study_id: str) -> Path | None:
    """Find a study directory by ID, preferring CBIO_DOWNLOADS over CBIO_DATAHUB."""
    candidates = []
    downloads = os.getenv("CBIO_DOWNLOADS")
    datahub = os.getenv("CBIO_DATAHUB")
    if downloads:
        candidates.append(Path(downloads))
    if datahub:
        candidates.append(Path(datahub))
    for base in candidates:
        if not base.exists():
            continue
        match = next((s for s in discover_studies(base) if s.name == study_id), None)
        if match:
            return match
    return None

def parse_meta_file(file_path: Path):
    """Parse a cBioPortal meta_*.txt file into a dictionary."""
    meta = {}
    if not file_path.exists():
        return meta
    with open(file_path, 'r') as f:
        for line in f:
            if ':' in line:
                key, value = line.split(':', 1)
                meta[key.strip()] = value.strip()
    return meta

# Cache for study categories mapping
_CATEGORY_MAPPING = None

def load_category_mapping():
    global _CATEGORY_MAPPING
    if _CATEGORY_MAPPING is not None:
        return _CATEGORY_MAPPING
    
    mapping_path = Path(__file__).resolve().parent.parent.parent.parent / "study_categories.yaml"
    if mapping_path.exists():
        with open(mapping_path, 'r') as f:
            raw_mapping = yaml.safe_load(f)
            _CATEGORY_MAPPING = {}
            for category, study_ids in raw_mapping.items():
                for sid in study_ids:
                    _CATEGORY_MAPPING[sid.lower()] = category
    else:
        _CATEGORY_MAPPING = {}
    return _CATEGORY_MAPPING

def retrieve_oncotree_cancer_types():
    """Retrieve cancer types from OncoTree API"""
    request_url = 'http://oncotree.mskcc.org/api/tumorTypes/tree?version=oncotree_latest_stable'
    request_headers = {'Accept': 'application/json'}
    response = requests.get(url=request_url, headers=request_headers)
    response.raise_for_status()
    return response.json()

def flatten_oncotree(node, node_name, cancer_types):
    """Recursive function to flatten the JSON formatted cancer types"""
    type_of_cancer_id = node_name.lower()
    name = node['name']
    dedicated_color = node['color']
    short_name = node_name
    parent = node['parent'].lower()
    cancer_types.append((type_of_cancer_id, name, dedicated_color, short_name, parent))
    if 'children' in node and node['children']:
        for child_node_name, child_node in node['children'].items():
            flatten_oncotree(child_node, child_node_name, cancer_types)

def sync_oncotree(conn):
    """Fetch latest OncoTree data and sync to DuckDB."""
    typer.echo("Fetching OncoTree data from API...")
    data = retrieve_oncotree_cancer_types()
    cancer_types = []
    if 'TISSUE' in data:
        for child_name, child_node in data['TISSUE']['children'].items():
            flatten_oncotree(child_node, child_name, cancer_types)
    conn.execute("DROP TABLE IF EXISTS cancer_types")
    conn.execute("""
        CREATE TABLE cancer_types (
            type_of_cancer_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            dedicated_color VARCHAR,
            short_name VARCHAR,
            parent VARCHAR
        )
    """)
    conn.executemany("INSERT INTO cancer_types VALUES (?, ?, ?, ?, ?)", cancer_types)
    typer.echo(f"Successfully synced {len(cancer_types)} cancer types from OncoTree.")

def get_oncotree_root(conn, type_of_cancer_id: str):
    """Traverse up the OncoTree to find the root organ node (parent is 'tissue')."""
    if not type_of_cancer_id:
        return "Other"
    current_id = type_of_cancer_id.lower()
    for _ in range(10):
        res = conn.execute("SELECT name, parent FROM cancer_types WHERE type_of_cancer_id = ?", (current_id,)).fetchone()
        if not res:
            return current_id.capitalize()
        name, parent = res
        if parent == "tissue":
            return name
        current_id = parent
    return "Other"

def categorize_study(conn, meta: dict, study_id: str):
    """Determine the category for a study (YAML or OncoTree root)."""
    sid = study_id.lower()
    mapping = load_category_mapping()
    if sid in mapping:
        return mapping[sid]
    raw_type = meta.get("type_of_cancer")
    return get_oncotree_root(conn, raw_type)

def load_study_metadata(conn, study_path: Path):
    """Load study metadata from meta_study.txt."""
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

def load_study(conn, study_path: Path, load_mutations: bool = False, load_cna: bool = False, load_sv: bool = False, load_timeline: bool = False):
    """Load clinical and genomic data for a study."""
    raw_study_id = study_path.name
    patient_file = study_path / "data_clinical_patient.txt"
    sample_file = study_path / "data_clinical_sample.txt"
    mutation_file = study_path / "data_mutations.txt"
    gene_panel_file = study_path / "data_gene_panel_matrix.txt"
    sv_file = study_path / "data_sv.txt"
    cna_file = study_path / "data_cna.txt"
    timeline_files = list(study_path.glob("data_timeline_*.txt"))
    
    if not mutation_file.exists():
        variants = list(study_path.glob("data_mutations*.txt"))
        if variants: mutation_file = variants[0]

    try:
        loaded_any = False
        if patient_file.exists():
            table_name = f'"{raw_study_id}_patient"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{patient_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        if sample_file.exists():
            table_name = f'"{raw_study_id}_sample"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{sample_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        if load_mutations and mutation_file.exists():
            table_name = f'"{raw_study_id}_mutations"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            _vc_list = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in sorted(_EXCLUDED_VCS))
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT '{raw_study_id}' as study_id, *
                FROM read_csv('{mutation_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)
                WHERE COALESCE(Variant_Classification, '') NOT IN ({_vc_list})
                   -- cBioPortal keeps TERT 5'Flank (promoter mutations) but NOT 5'UTR or others.
                   -- Using a broad OR Hugo_Symbol='TERT' would include TERT 5'UTR rows, overcounting by ~1.
                   -- Ref: cBioPortal File-Formats.md "promoter mutations of the TERT gene"
                   OR (Hugo_Symbol = 'TERT' AND Variant_Classification = '5''Flank')
            """)
            normalize_hugo_symbols(conn, raw_study_id)
            loaded_any = True
        if load_sv and sv_file.exists():
            table_name = f'"{raw_study_id}_sv"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{sv_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        if load_cna and cna_file.exists():
            table_name = f'"{raw_study_id}_cna"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            # Unpivot the wide CNA matrix into long format, keeping only non-zero values
            # Handle 'NA' strings by specifying nullstr='NA'
            unpivot_sql = f"""
                CREATE TABLE {table_name} AS 
                SELECT 
                    '{raw_study_id}' as study_id,
                    Hugo_Symbol as hugo_symbol,
                    sample_id,
                    CAST(cna_value AS SIGNED) as cna_value
                FROM (
                    UNPIVOT (SELECT * FROM read_csv('{cna_file}', delim='\t', header=True, ignore_errors=True, nullstr='NA'))
                    ON COLUMNS(* EXCLUDE Hugo_Symbol)
                    INTO
                        NAME sample_id
                        VALUE cna_value
                )
                WHERE cna_value IS NOT NULL AND cna_value != 0
            """
            conn.execute(unpivot_sql)
            normalize_hugo_symbols(conn, raw_study_id)
            loaded_any = True
        if load_timeline and timeline_files:
            for timeline_file in timeline_files:
                # Extract suffix from filename, e.g., 'treatment' from 'data_timeline_treatment.txt'
                suffix = timeline_file.stem.replace("data_timeline_", "")
                table_name = f'"{raw_study_id}_timeline_{suffix}"'
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{timeline_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        if gene_panel_file.exists():
            table_name = f'"{raw_study_id}_gene_panel"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{gene_panel_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        return loaded_any
    except Exception as e:
        typer.echo(f"Error loading {raw_study_id}: {e}")
        return False
    finally:
        data_types = []
        if (study_path / "data_mutations.txt").exists() or list(study_path.glob("data_mutations_*.txt")): data_types.append("mutation")
        if (study_path / "data_cna.txt").exists(): data_types.append("cna")
        if (study_path / "data_sv.txt").exists(): data_types.append("sv")
        if list(study_path.glob("data_mrna_seq_*.txt")) or list(study_path.glob("data_expression_*.txt")): data_types.append("mrna")
        if list(study_path.glob("data_rppa*.txt")): data_types.append("protein")
        if list(study_path.glob("data_methylation*.txt")): data_types.append("methylation")
        if (study_path / "data_timeline_treatment.txt").exists(): data_types.append("treatment")
        if (study_path / "data_cna_hg19.seg").exists() or list(study_path.glob("data_cna_*.seg")): data_types.append("segment")
        if gene_panel_file.exists(): data_types.append("gene_panel")
        if data_types:
            conn.execute("CREATE TABLE IF NOT EXISTS study_data_types (study_id VARCHAR NOT NULL, data_type VARCHAR NOT NULL, PRIMARY KEY (study_id, data_type))")
            for dt in data_types:
                conn.execute("INSERT OR REPLACE INTO study_data_types VALUES (?, ?)", (raw_study_id, dt))

def load_gene_reference(conn, genes_json_path: Path = None):
    """Load gene reference table from genes.json (entrezGeneId → hugoGeneSymbol).

    Source: $CBIO_DATAHUB/.circleci/portalinfo/genes.json (from cBioPortal/datahub repo).
    Maps Entrez Gene ID → canonical HGNC Hugo symbol.
    Required for Pass 1 of Hugo symbol normalization — the most accurate pass because
    Entrez IDs are stable identifiers even when the Hugo symbol changes.
    """
    if genes_json_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no path provided.")
            raise typer.Exit(code=1)
        genes_json_path = Path(datahub) / ".circleci" / "portalinfo" / "genes.json"

    if not genes_json_path.exists():
        typer.echo(f"Error: genes.json not found at {genes_json_path}")
        raise typer.Exit(code=1)

    typer.echo(f"Loading gene reference from {genes_json_path}...")
    with open(genes_json_path, "r") as f:
        genes = json.load(f)

    conn.execute("DROP TABLE IF EXISTS gene_reference")
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR
        )
    """)

    rows = [
        (g["entrezGeneId"], g["hugoGeneSymbol"], g.get("type"))
        for g in genes
        if g.get("entrezGeneId") is not None
    ]
    conn.executemany("INSERT OR REPLACE INTO gene_reference VALUES (?, ?, ?)", rows)
    typer.echo(f"Successfully loaded {len(rows)} gene reference entries.")


def load_gene_symbol_updates(conn, gene_update_md: Path = None):
    """Parse gene-update.md and populate gene_symbol_updates table.

    Source: $CBIO_DATAHUB/seedDB/gene-update-list/gene-update.md
    Covers genes that were *renamed* (not just aliased) — e.g., C10ORF12 → LCOR.
    Only ~75 entries; does NOT cover KMT2 family aliases (those need gene_alias).
    Used as Pass 2 fallback when Entrez ID is wrong or missing.
    """
    if gene_update_md is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no path provided.")
            raise typer.Exit(code=1)
        gene_update_md = Path(datahub) / "seedDB" / "gene-update-list" / "gene-update.md"

    if not gene_update_md.exists():
        typer.echo(f"Warning: gene-update.md not found at {gene_update_md}, skipping.")
        return

    import re
    conn.execute("DROP TABLE IF EXISTS gene_symbol_updates")
    conn.execute("""
        CREATE TABLE gene_symbol_updates (
            old_symbol VARCHAR PRIMARY KEY,
            new_symbol VARCHAR
        )
    """)

    pattern = re.compile(r'^(\S+)\s+-?\d+\s+->\s+(\S+)\s+-?\d+')
    rows = {}
    in_code_block = False
    with open(gene_update_md, "r") as f:
        for line in f:
            line = line.rstrip()
            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                m = pattern.match(line.strip())
                if m:
                    old_sym, new_sym = m.group(1), m.group(2)
                    if old_sym != new_sym:
                        rows[old_sym] = new_sym

    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO gene_symbol_updates VALUES (?, ?)",
            list(rows.items())
        )
    typer.echo(f"Loaded {len(rows)} gene symbol update entries.")


def load_gene_aliases(conn, seed_sql_path: Path = None):
    """Extract gene_alias table from seed SQL and create alias→canonical mapping.

    Solves the problem of studies with Entrez_Gene_Id=0 for genes that have been renamed.
    For example, msk_chord_2024 ships MLL2/MLL3/MLL/MLL4 with Entrez_Gene_Id=0; these are
    historical aliases that HGNC renamed to KMT2D/KMT2C/KMT2A/KMT2B respectively.
    gene_reference won't match them (no valid Entrez ID); gene_symbol_updates doesn't
    have them either. This table provides the bridge via NCBI alias records.

    Source: $CBIO_DATAHUB/seedDB/seed-cbioportal_hg19_hg38_*.sql.gz — the gene_alias table.
    Contains ~55k alias entries. Key mappings:
      MLL  → KMT2A (Entrez 4297)
      MLL2 → KMT2D (Entrez 8085)
      MLL3 → KMT2C (Entrez 58508)
      MLL4 → KMT2B (Entrez 9757)
    Used as Pass 3 fallback after Entrez ID and symbol-update passes.
    """
    if seed_sql_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no path provided.")
            raise typer.Exit(code=1)
        seed_dir = Path(datahub) / "seedDB"
        candidates = sorted(seed_dir.glob("seed-cbioportal_hg19_hg38_*.sql.gz"))
        if not candidates:
            typer.echo(f"Warning: No seed SQL file found in {seed_dir}. Gene alias normalization will be skipped.")
            return
        seed_sql_path = candidates[-1]

    if not seed_sql_path.exists():
        typer.echo(f"Warning: seed SQL not found at {seed_sql_path}. Gene alias normalization will be skipped.")
        return

    typer.echo(f"Loading gene aliases from {seed_sql_path.name}...")
    insert_re = re.compile(r"INSERT INTO `gene_alias` VALUES (.+);")
    pair_re = re.compile(r"\((\d+),'([^']+)'\)")

    alias_rows: list[tuple[int, str]] = []
    opener = gzip.open if seed_sql_path.suffix == ".gz" else open
    with opener(seed_sql_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = insert_re.match(line.strip())
            if m:
                for entrez_str, alias in pair_re.findall(m.group(1)):
                    alias_rows.append((int(entrez_str), alias))

    conn.execute("DROP TABLE IF EXISTS gene_alias")
    conn.execute("""
        CREATE TABLE gene_alias (
            entrez_gene_id INTEGER,
            alias_symbol VARCHAR,
            PRIMARY KEY (entrez_gene_id, alias_symbol)
        )
    """)
    conn.executemany("INSERT OR REPLACE INTO gene_alias VALUES (?, ?)", alias_rows)
    typer.echo(f"Loaded {len(alias_rows)} gene alias entries.")


def normalize_hugo_symbols(conn, study_id: str):
    """Normalize Hugo symbols in {study_id}_mutations and {study_id}_cna to canonical HGNC symbols.

    Why this is necessary:
      Many studies encode genes by Entrez Gene ID with a stale Hugo symbol (e.g. a study
      might say Hugo_Symbol='MLL2', Entrez_Gene_Id=8085, but the canonical symbol for
      Entrez 8085 is 'KMT2D'). Without normalization, MLL2 and KMT2D count as different
      genes, causing significant undercounting in the mutated-genes chart.

    Three-pass strategy (applied in order):
      Pass 1 — by Entrez Gene ID (most reliable): JOIN against gene_reference on Entrez ID.
                Skips rows where Entrez_Gene_Id=0 (unknown).
      Pass 2 — by gene_symbol_updates (rename list): Catches genes explicitly renamed in
                cBioPortal's gene-update.md (~75 entries, e.g. C12ORF74→PLEKHG7).
      Pass 3 — by gene_alias (NCBI alias table): Catches historical aliases where studies
                shipped Entrez_Gene_Id=0 (e.g. MLL2→KMT2D, MLL3→KMT2C, MLL→KMT2A,
                MLL4→KMT2B). ~55k alias entries sourced from cBioPortal seed SQL.

    CNA pre-pass: CNA files don't carry Entrez IDs, so we derive a stale→canonical map from
      the *same study's mutations table before it is normalized*. This handles cases where
      the mutations table has old symbols that map via Entrez to canonical names.
    """
    # Guard: only run if gene_reference exists and has rows
    try:
        count = conn.execute("SELECT COUNT(*) FROM gene_reference").fetchone()[0]
        if count == 0:
            return
    except Exception:
        return

    tables_res = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
    ).fetchall()
    existing_tables = {t[0] for t in tables_res}

    has_updates = "gene_symbol_updates" in existing_tables
    has_aliases = "gene_alias" in existing_tables

    mutations_table = f"{study_id}_mutations"
    cna_table = f"{study_id}_cna"

    # Pass 3 (CNA only): derive alias map from this study's mutations table *before*
    # mutations are normalized, so stale symbols (e.g. MLL2) are still present.
    if cna_table in existing_tables and mutations_table in existing_tables:
        conn.execute(f"""
            UPDATE "{cna_table}"
            SET hugo_symbol = alias_map.canonical
            FROM (
                SELECT DISTINCT
                    "{mutations_table}".Hugo_Symbol AS old_symbol,
                    gr.hugo_gene_symbol            AS canonical
                FROM "{mutations_table}"
                JOIN gene_reference gr
                  ON TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id
                WHERE TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) > 0
                  AND "{mutations_table}".Hugo_Symbol IS DISTINCT FROM gr.hugo_gene_symbol
            ) alias_map
            WHERE "{cna_table}".hugo_symbol = alias_map.old_symbol
        """)

    if mutations_table in existing_tables:
        # Pass 1: normalize by Entrez Gene ID.
        # Qualify columns with table name to avoid case-insensitive ambiguity with gene_reference.entrez_gene_id
        conn.execute(f"""
            UPDATE "{mutations_table}"
            SET Hugo_Symbol = gr.hugo_gene_symbol
            FROM gene_reference gr
            WHERE TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id
              AND TRY_CAST("{mutations_table}".Entrez_Gene_Id AS INTEGER) > 0
              AND "{mutations_table}".Hugo_Symbol IS DISTINCT FROM gr.hugo_gene_symbol
        """)
        # Pass 2: normalize by symbol map (covers renamed genes in gene-update.md)
        if has_updates:
            conn.execute(f"""
                UPDATE "{mutations_table}"
                SET Hugo_Symbol = su.new_symbol
                FROM gene_symbol_updates su
                WHERE "{mutations_table}".Hugo_Symbol = su.old_symbol
            """)
        # Pass 3: normalize by gene_alias table (covers historical aliases like MLL2→KMT2D)
        if has_aliases:
            conn.execute(f"""
                UPDATE "{mutations_table}"
                SET Hugo_Symbol = gr.hugo_gene_symbol
                FROM gene_alias ga
                JOIN gene_reference gr ON ga.entrez_gene_id = gr.entrez_gene_id
                WHERE "{mutations_table}".Hugo_Symbol = ga.alias_symbol
                  AND "{mutations_table}".Hugo_Symbol IS DISTINCT FROM gr.hugo_gene_symbol
            """)

    if cna_table in existing_tables:
        # CNA symbol map normalization (covers cases not bridged via mutations)
        if has_updates:
            conn.execute(f"""
                UPDATE "{cna_table}"
                SET hugo_symbol = su.new_symbol
                FROM gene_symbol_updates su
                WHERE hugo_symbol = su.old_symbol
            """)
        if has_aliases:
            conn.execute(f"""
                UPDATE "{cna_table}"
                SET hugo_symbol = gr.hugo_gene_symbol
                FROM gene_alias ga
                JOIN gene_reference gr ON ga.entrez_gene_id = gr.entrez_gene_id
                WHERE hugo_symbol = ga.alias_symbol
                  AND hugo_symbol IS DISTINCT FROM gr.hugo_gene_symbol
            """)


def load_gene_panel_definitions(conn, json_path: Path = None):
    """Load gene panel definitions from gene-panels.json into DuckDB."""
    if json_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no --json-path provided.")
            raise typer.Exit(code=1)
        json_path = Path(datahub) / ".circleci" / "portalinfo" / "gene-panels.json"

    if not json_path.exists():
        typer.echo(f"Error: Gene panels JSON not found at {json_path}")
        raise typer.Exit(code=1)

    typer.echo(f"Loading gene panel definitions from {json_path}...")
    with open(json_path, "r") as f:
        panels = json.load(f)

    conn.execute("DROP TABLE IF EXISTS gene_panel_definitions")
    conn.execute("""
        CREATE TABLE gene_panel_definitions (
            panel_id VARCHAR,
            description VARCHAR,
            hugo_gene_symbol VARCHAR,
            entrez_gene_id INTEGER,
            PRIMARY KEY (panel_id, hugo_gene_symbol)
        )
    """)

    rows = []
    for panel in panels:
        panel_id = panel["genePanelId"]
        description = panel.get("description", "")
        for gene in panel.get("genes", []):
            rows.append((panel_id, description, gene["hugoGeneSymbol"], gene.get("entrezGeneId")))

    conn.executemany("INSERT OR REPLACE INTO gene_panel_definitions VALUES (?, ?, ?, ?)", rows)
    panel_count = len(panels)
    gene_count = len(rows)
    typer.echo(f"Successfully loaded {panel_count} gene panels ({gene_count} gene entries).")


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

def ensure_gene_reference(conn):
    """Auto-load gene reference tables when CBIO_DATAHUB is set, if not already present.

    Called before every study load (db add, db load-lfs, db load-all) to ensure
    normalize_hugo_symbols() has the data it needs. Each table is checked independently
    so adding gene_alias later doesn't require re-loading gene_reference.

    IMPORTANT: If CBIO_DATAHUB is not set, normalization is silently skipped. Gene counts
    will then be wrong for studies using legacy aliases (e.g. KMT2 family genes).
    """
    datahub = os.getenv("CBIO_DATAHUB")
    if not datahub:
        return
    existing = {t[0] for t in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    try:
        if "gene_reference" not in existing:
            load_gene_reference(conn, Path(datahub) / ".circleci" / "portalinfo" / "genes.json")
        if "gene_symbol_updates" not in existing:
            load_gene_symbol_updates(conn, Path(datahub) / "seedDB" / "gene-update-list" / "gene-update.md")
        if "gene_alias" not in existing:
            load_gene_aliases(conn)
    except SystemExit:
        typer.echo("Warning: Could not load gene reference tables. Hugo symbol normalization will be skipped.")
    except Exception as e:
        typer.echo(f"Warning: Could not load gene reference tables: {e}. Hugo symbol normalization will be skipped.")


def load_all_studies(conn, datahub_path: Path, limit: int = None, offset: int = 0, load_mutations: bool = False, load_cna: bool = False, load_sv: bool = False, load_timeline: bool = False):
    """Iterate through studies and load them incrementally."""
    monitor = Monitor()

    ensure_gene_reference(conn)

    all_studies = discover_studies(datahub_path)
    start, end = offset, (offset + limit) if limit else len(all_studies)
    studies = all_studies[start:end]
    typer.echo(f"Found {len(all_studies)} total studies. Processing batch of {len(studies)}.")
    total_loaded = 0
    with typer.progressbar(studies, label="Loading studies") as progress:
        for study_path in progress:
            load_study_metadata(conn, study_path)
            if load_study(conn, study_path, load_mutations=load_mutations, load_cna=load_cna, load_sv=load_sv, load_timeline=load_timeline):
                total_loaded += 1
    create_global_views(conn)
    metrics = monitor.get_metrics()
    return total_loaded, metrics
