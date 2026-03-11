import os
import time
import psutil
import duckdb
import requests
import yaml
from pathlib import Path
import typer

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

def load_study(conn, study_path: Path, load_mutations: bool = False, load_cna: bool = False, load_sv: bool = False):
    """Load clinical and genomic data for a study."""
    raw_study_id = study_path.name
    patient_file = study_path / "data_clinical_patient.txt"
    sample_file = study_path / "data_clinical_sample.txt"
    mutation_file = study_path / "data_mutations.txt"
    gene_panel_file = study_path / "data_gene_panel_matrix.txt"
    sv_file = study_path / "data_sv.txt"
    cna_file = study_path / "data_cna.txt"
    
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
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{mutation_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
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
        if data_types:
            conn.execute("CREATE TABLE IF NOT EXISTS study_data_types (study_id VARCHAR NOT NULL, data_type VARCHAR NOT NULL, PRIMARY KEY (study_id, data_type))")
            for dt in data_types:
                conn.execute("INSERT OR REPLACE INTO study_data_types VALUES (?, ?)", (raw_study_id, dt))

def create_global_views(conn):
    """Refresh unified views across all loaded study tables."""
    tables_res = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'").fetchall()
    tables = [t[0] for t in tables_res]
    suffixes = {
        "patient": "clinical_patient", 
        "sample": "clinical_sample", 
        "mutations": "mutations", 
        "gene_panel": "gene_panel_matrix",
        "sv": "sv",
        "cna": "cna"
    }
    for suffix, view_name in suffixes.items():
        study_tables = [f'"{t}"' for t in tables if t.endswith(f"_{suffix}")]
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
        if study_tables:
            union_sql = " UNION ALL BY NAME ".join([f"SELECT * FROM {t}" for t in study_tables])
            conn.execute(f"CREATE VIEW {view_name} AS {union_sql}")

def load_all_studies(conn, datahub_path: Path, limit: int = None, offset: int = 0, load_mutations: bool = False, load_cna: bool = False, load_sv: bool = False):
    """Iterate through studies and load them incrementally."""
    monitor = Monitor()
    all_studies = discover_studies(datahub_path)
    start, end = offset, (offset + limit) if limit else len(all_studies)
    studies = all_studies[start:end]
    typer.echo(f"Found {len(all_studies)} total studies. Processing batch of {len(studies)}.")
    total_loaded = 0
    with typer.progressbar(studies, label="Loading studies") as progress:
        for study_path in progress:
            load_study_metadata(conn, study_path)
            if load_study(conn, study_path, load_mutations=load_mutations, load_cna=load_cna, load_sv=load_sv):
                total_loaded += 1
    create_global_views(conn)
    metrics = monitor.get_metrics()
    return total_loaded, metrics
