"""Study loader — public API.

Public entry points:
    load_study(conn, study_path, ...)
    load_all_studies(conn, datahub_path, ...)
    ensure_gene_reference(conn)

Internal modules:
    discovery    — find_study_path(), discover_studies(), parse_meta_file()
    clinical     — parse_clinical_headers(), _upsert_clinical_attribute_meta()
    genomic      — _EXCLUDED_VCS, _inject_fga_from_seg()
    hugo         — normalize_hugo_symbols() (3-pass system)
    gene_reference — ensure_gene_reference(), load_gene_*, sync_oncotree()
    schema       — create_global_views(), categorize_study(), load_study_metadata()
"""
import os
import time
from pathlib import Path

import psutil
import typer

from .discovery import discover_studies, find_study_path, get_source_path, parse_meta_file
from .clinical import parse_clinical_headers, _upsert_clinical_attribute_meta
from .genomic import _EXCLUDED_VCS, _inject_fga_from_seg
from .hugo import normalize_hugo_symbols
from .gene_reference import (
    ensure_gene_reference,
    load_gene_reference,
    load_gene_symbol_updates,
    load_gene_aliases,
    load_gene_panel_definitions,
    populate_cytoband_from_hgnc,
    sync_oncotree,
    retrieve_oncotree_cancer_types,
    get_oncotree_root,
)
from .schema import create_global_views, categorize_study, load_category_mapping, load_study_metadata

__all__ = [
    # Discovery
    "discover_studies", "find_study_path", "get_source_path", "parse_meta_file",
    # Clinical
    "parse_clinical_headers",
    # Hugo normalization
    "normalize_hugo_symbols", "_EXCLUDED_VCS",
    # Gene reference
    "ensure_gene_reference", "load_gene_reference", "load_gene_symbol_updates",
    "load_gene_aliases", "load_gene_panel_definitions",
    "sync_oncotree", "retrieve_oncotree_cancer_types", "get_oncotree_root",
    # Schema / views
    "create_global_views", "categorize_study", "load_study_metadata",
    # Top-level loaders
    "Monitor", "load_study", "load_all_studies",
]


class Monitor:
    """Track peak memory and elapsed time during a study load batch."""

    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.start_time = time.time()
        self.peak_memory = 0

    def get_metrics(self):
        """Return current memory, peak memory, and elapsed seconds."""
        current_mem = self.process.memory_info().rss / (1024 * 1024)
        if current_mem > self.peak_memory:
            self.peak_memory = current_mem
        elapsed = time.time() - self.start_time
        return {
            "current_memory_mb": current_mem,
            "peak_memory_mb": self.peak_memory,
            "elapsed_seconds": elapsed
        }


def load_study(
    conn,
    study_path: Path,
    load_mutations: bool = False,
    load_cna: bool = False,
    load_sv: bool = False,
    load_timeline: bool = False,
):
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
        if variants:
            mutation_file = variants[0]

    try:
        loaded_any = False
        fga_injected = False
        if patient_file.exists():
            table_name = f'"{raw_study_id}_patient"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{patient_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        if sample_file.exists():
            table_name = f'"{raw_study_id}_sample"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{sample_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            fga_injected = _inject_fga_from_seg(conn, table_name, study_path)
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
            # Hybrid CNA strategy: UNPIVOT (fast, DuckDB-native) for files with
            # ≤ 5,000 sample columns; Python row-by-row (O(1) memory) for wider files.
            # UNPIVOT materialises the entire matrix in C-level memory — at 25k samples
            # it uses ~619 MB, at 54k samples it OOMs (38 GB). Python is ~20x slower
            # per-file but avoids the O(n_samples) memory spike. See BETA.md.
            _NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id", "Cytoband"}
            with open(cna_file) as _f:
                for _line in _f:
                    if not _line.startswith("#"):
                        _header_cols = _line.strip().split("\t")
                        break
            _hugo_col = _header_cols.index("Hugo_Symbol") if "Hugo_Symbol" in _header_cols else None
            _entrez_col = _header_cols.index("Entrez_Gene_Id") if "Entrez_Gene_Id" in _header_cols else None
            _sample_cols = [c for c in _header_cols if c not in _NON_SAMPLE_COLS]
            _n_samples = len(_sample_cols)

            if _n_samples <= 5_000:
                # Fast path: DuckDB UNPIVOT — handles all header variants dynamically.
                _exclude = [c for c in _header_cols if c in _NON_SAMPLE_COLS]
                if len(_exclude) > 1:
                    _exclude_clause = f"({', '.join(_exclude)})"
                elif _exclude:
                    _exclude_clause = _exclude[0]
                else:
                    _exclude_clause = None
                if _hugo_col is not None:
                    _hugo_select = "Hugo_Symbol as hugo_symbol,"
                    _join_clause = ""
                else:
                    _hugo_select = "gr.hugo_gene_symbol as hugo_symbol,"
                    _join_clause = f"JOIN gene_reference gr ON TRY_CAST(unpivoted.Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id"
                _on_clause = f"ON COLUMNS(* EXCLUDE {_exclude_clause})" if _exclude_clause else "ON COLUMNS(*)"
                conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM (
                        SELECT
                            '{raw_study_id}' as study_id,
                            {_hugo_select}
                            sample_id,
                            TRY_CAST(cna_value AS DOUBLE) as cna_value
                        FROM (
                            UNPIVOT (SELECT * FROM read_csv('{cna_file}', delim='\\t', header=True, all_varchar=True, ignore_errors=True, null_padding=True))
                            {_on_clause}
                            INTO NAME sample_id VALUE cna_value
                        ) unpivoted
                        {_join_clause}
                    ) WHERE cna_value IS NOT NULL AND cna_value != 0
                """)
            else:
                # Slow path: Python row-by-row — O(1) memory for 50k+ sample columns.
                conn.execute(f"""
                    CREATE TABLE {table_name} (
                        study_id    VARCHAR NOT NULL,
                        hugo_symbol VARCHAR,
                        sample_id   VARCHAR NOT NULL,
                        cna_value   DOUBLE NOT NULL
                    )
                """)
                _sample_indices = [(i, c) for i, c in enumerate(_header_cols) if c not in _NON_SAMPLE_COLS]
                _entrez_to_hugo: dict[int, str] = {}
                if _hugo_col is None and _entrez_col is not None:
                    _entrez_to_hugo = {
                        r[0]: r[1]
                        for r in conn.execute("SELECT entrez_gene_id, hugo_gene_symbol FROM gene_reference").fetchall()
                        if r[1]
                    }
                _batch: list[tuple] = []
                with open(cna_file) as _f:
                    for _line in _f:
                        if _line.startswith("#"):
                            continue
                        _parts = _line.rstrip("\n").split("\t")
                        if _parts[0] == (_header_cols[0]):
                            continue  # skip header row
                        if _hugo_col is not None:
                            _hugo = _parts[_hugo_col]
                        elif _entrez_col is not None:
                            try:
                                _hugo = _entrez_to_hugo.get(int(_parts[_entrez_col]), "")
                            except (ValueError, IndexError):
                                _hugo = ""
                        else:
                            _hugo = ""
                        for _idx, _sample_id in _sample_indices:
                            try:
                                _raw = _parts[_idx].strip()
                                if _raw in ("", "NA", "null", "NULL"):
                                    continue
                                _val = float(_raw)
                            except (ValueError, IndexError):
                                continue
                            if _val == 0:
                                continue
                            _batch.append((raw_study_id, _hugo, _sample_id, _val))
                        if len(_batch) >= 10_000:
                            conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", _batch)
                            _batch.clear()
                if _batch:
                    conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", _batch)
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
        # Always load treatment and specimen timeline files if present (needed for treatment charts)
        for timeline_name in ("treatment", "specimen"):
            tfile = study_path / f"data_timeline_{timeline_name}.txt"
            if tfile.exists() and not (load_timeline and tfile in timeline_files):
                table_name = f'"{raw_study_id}_timeline_{timeline_name}"'
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{tfile}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
                loaded_any = True
        if gene_panel_file.exists():
            table_name = f'"{raw_study_id}_gene_panel"'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT '{raw_study_id}' as study_id, * FROM read_csv('{gene_panel_file}', delim='\t', header=True, comment='#', null_padding=True, ignore_errors=True)")
            loaded_any = True
        # Parse and store clinical attribute metadata from file headers
        all_attrs: list[dict] = []
        if patient_file.exists():
            all_attrs += parse_clinical_headers(patient_file, patient_attribute=True)
        if sample_file.exists():
            all_attrs += parse_clinical_headers(sample_file, patient_attribute=False)
        if fga_injected:
            all_attrs.append({
                "attr_id": "FRACTION_GENOME_ALTERED",
                "display_name": "Fraction Genome Altered",
                "description": "Fraction of genome with copy number alterations (|seg.mean| >= 0.2)",
                "datatype": "NUMBER",
                "patient_attribute": False,
                "priority": 200,
            })
        if all_attrs:
            _upsert_clinical_attribute_meta(conn, raw_study_id, all_attrs)
        return loaded_any
    except Exception as e:
        typer.echo(f"Error loading {raw_study_id}: {e}")
        raise
    finally:
        data_types = []
        if (study_path / "data_mutations.txt").exists() or list(study_path.glob("data_mutations_*.txt")):
            data_types.append("mutation")
        if (study_path / "data_cna.txt").exists():
            data_types.append("cna")
        if (study_path / "data_sv.txt").exists():
            data_types.append("sv")
        if list(study_path.glob("data_mrna_seq_*.txt")) or list(study_path.glob("data_expression_*.txt")):
            data_types.append("mrna")
        if list(study_path.glob("data_rppa*.txt")):
            data_types.append("protein")
        if list(study_path.glob("data_methylation*.txt")):
            data_types.append("methylation")
        if (study_path / "data_timeline_treatment.txt").exists():
            data_types.append("treatment")
        if (study_path / "data_cna_hg19.seg").exists() or list(study_path.glob("data_cna_*.seg")):
            data_types.append("segment")
        if gene_panel_file.exists():
            data_types.append("gene_panel")
        if data_types:
            conn.execute("CREATE TABLE IF NOT EXISTS study_data_types (study_id VARCHAR NOT NULL, data_type VARCHAR NOT NULL, PRIMARY KEY (study_id, data_type))")
            for dt in data_types:
                conn.execute("INSERT OR REPLACE INTO study_data_types VALUES (?, ?)", (raw_study_id, dt))


def load_all_studies(
    conn,
    datahub_path: Path,
    limit: int = None,
    offset: int = 0,
    load_mutations: bool = False,
    load_cna: bool = False,
    load_sv: bool = False,
    load_timeline: bool = False,
):
    """Iterate through studies and load them incrementally."""
    monitor = Monitor()

    # Cap working memory and enable disk spill for large UNPIVOT operations
    # (e.g. msk_impact_50k_2026 CNA matrix exhausts RAM without this).
    # Scoped here so normal web-serving queries are unaffected.
    import tempfile
    _spill_dir = tempfile.mkdtemp(prefix="duckdb_spill_")
    conn.execute(f"SET memory_limit='8GB'")
    conn.execute(f"SET temp_directory='{_spill_dir}'")

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
            conn.execute("CHECKPOINT")
    create_global_views(conn)
    metrics = monitor.get_metrics()
    return total_loaded, metrics
