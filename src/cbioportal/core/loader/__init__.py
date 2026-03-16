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
        return False
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
