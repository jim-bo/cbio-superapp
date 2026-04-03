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
    molecular_profiles — load_molecular_profiles() (meta_*.txt → molecular_profiles table)
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


def _load_generic_assay(conn, study_id: str, filepath: Path, stable_id: str, meta_props: set[str]) -> None:
    """Load a generic assay file (drug treatment, etc.) into long format.

    Creates table ``{study_id}_ga_{stable_id}`` with columns:
        study_id, entity_id, sample_id, value DOUBLE, is_limit BOOLEAN

    ``meta_props`` is the set of non-sample metadata columns declared in
    ``generic_entity_meta_properties`` (e.g. NAME, URL, DESCRIPTION).
    Values prefixed with '>' or '<' are censored (limit values).
    """
    # Columns that are entity metadata, not sample IDs
    _META_COLS = {"ENTITY_STABLE_ID"} | {p.strip() for p in meta_props}

    with open(filepath) as f:
        for line in f:
            if not line.startswith("#"):
                header_cols = line.strip().split("\t")
                break

    entity_col = header_cols.index("ENTITY_STABLE_ID") if "ENTITY_STABLE_ID" in header_cols else 0
    sample_indices = [(i, c) for i, c in enumerate(header_cols) if c not in _META_COLS]

    safe_id = stable_id.replace("-", "_").replace(" ", "_")
    table_name = f'"{study_id}_ga_{safe_id}"'
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"""
        CREATE TABLE {table_name} (
            study_id  VARCHAR NOT NULL,
            entity_id VARCHAR NOT NULL,
            sample_id VARCHAR NOT NULL,
            value     DOUBLE,
            is_limit  BOOLEAN DEFAULT false
        )
    """)

    batch: list[tuple] = []
    with open(filepath) as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if parts[entity_col] == "ENTITY_STABLE_ID":
                continue  # header row
            entity_id = parts[entity_col] if entity_col < len(parts) else ""
            for idx, sample_id in sample_indices:
                try:
                    raw = parts[idx].strip() if idx < len(parts) else ""
                    if raw in ("", "NA", "null", "NULL"):
                        continue
                    is_limit = raw.startswith(">") or raw.startswith("<")
                    val = float(raw.lstrip("><"))
                except (ValueError, IndexError):
                    continue
                batch.append((study_id, entity_id, sample_id, val, is_limit))
                if len(batch) >= 10_000:
                    conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)", batch)
                    batch.clear()
    if batch:
        conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)", batch)


def _load_wide_matrix(conn, study_id: str, filepath: Path, table_name: str, value_col: str, *, filter_zeros: bool = False):
    """Load a gene×sample wide matrix (CNA, expression, protein, methylation) into long format.

    Args:
        filter_zeros: If True, exclude rows where value == 0 (used for CNA to save space).
    """
    # ENTITY_STABLE_ID is used by methylation probe files (cg* probe IDs).
    # In those files, NAME holds the Hugo symbol; DESCRIPTION and TRANSCRIPT_ID are metadata.
    _NON_SAMPLE = {"Hugo_Symbol", "Entrez_Gene_Id", "Cytoband", "Composite.Element.REF",
                   "ENTITY_STABLE_ID", "NAME", "DESCRIPTION", "TRANSCRIPT_ID"}
    with open(filepath) as f:
        for line in f:
            if not line.startswith("#"):
                header_cols = line.strip().split("\t")
                break
    # Resolve which column holds the Hugo symbol.
    # Methylation probe files (pan-cancer atlas) use NAME instead of Hugo_Symbol.
    if "Hugo_Symbol" in header_cols:
        hugo_col = header_cols.index("Hugo_Symbol")
        hugo_col_name = "Hugo_Symbol"
    elif "NAME" in header_cols:
        hugo_col = header_cols.index("NAME")
        hugo_col_name = "NAME"
    else:
        hugo_col = None
        hugo_col_name = None
    composite_col = header_cols.index("Composite.Element.REF") if "Composite.Element.REF" in header_cols else None
    sample_cols = [c for c in header_cols if c not in _NON_SAMPLE]
    n_samples = len(sample_cols)

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    if n_samples <= 5_000:
        exclude = [f'"{c}"' for c in header_cols if c in _NON_SAMPLE]
        if len(exclude) > 1:
            exclude_clause = f"({', '.join(exclude)})"
        elif exclude:
            exclude_clause = exclude[0]
        else:
            exclude_clause = None
        if hugo_col is not None:
            hugo_select = f'"{hugo_col_name}" as hugo_symbol,'
            join_clause = ""
        elif composite_col is not None:
            hugo_select = 'split_part("Composite.Element.REF", \'|\', 1) as hugo_symbol,'
            join_clause = ""
        else:
            hugo_select = "gr.hugo_gene_symbol as hugo_symbol,"
            join_clause = "JOIN gene_reference gr ON TRY_CAST(unpivoted.Entrez_Gene_Id AS INTEGER) = gr.entrez_gene_id"
        on_clause = f"ON COLUMNS(* EXCLUDE {exclude_clause})" if exclude_clause else "ON COLUMNS(*)"
        where = f"WHERE {value_col} IS NOT NULL" + (f" AND {value_col} != 0" if filter_zeros else "")
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM (
                SELECT
                    '{study_id}' as study_id,
                    {hugo_select}
                    sample_id,
                    TRY_CAST({value_col} AS FLOAT) as {value_col}
                FROM (
                    UNPIVOT (SELECT * FROM read_csv('{filepath}', delim='\\t', header=True, all_varchar=True, ignore_errors=True, null_padding=True))
                    {on_clause}
                    INTO NAME sample_id VALUE {value_col}
                ) unpivoted
                {join_clause}
            ) {where}
        """)
    else:
        conn.execute(f"""
            CREATE TABLE {table_name} (
                study_id    VARCHAR NOT NULL,
                hugo_symbol VARCHAR,
                sample_id   VARCHAR NOT NULL,
                {value_col} FLOAT NOT NULL
            )
        """)
        entrez_col = header_cols.index("Entrez_Gene_Id") if "Entrez_Gene_Id" in header_cols else None
        sample_indices = [(i, c) for i, c in enumerate(header_cols) if c not in _NON_SAMPLE]
        entrez_to_hugo: dict[int, str] = {}
        if hugo_col is None and composite_col is None and entrez_col is not None:
            entrez_to_hugo = {
                r[0]: r[1]
                for r in conn.execute("SELECT entrez_gene_id, hugo_gene_symbol FROM gene_reference").fetchall()
                if r[1]
            }
        batch: list[tuple] = []
        with open(filepath) as f:
            for line in f:
                if line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if parts[0] == header_cols[0]:
                    continue
                if hugo_col is not None:
                    hugo = parts[hugo_col]
                elif composite_col is not None:
                    hugo = parts[composite_col].split("|")[0]
                elif entrez_col is not None:
                    try:
                        hugo = entrez_to_hugo.get(int(parts[entrez_col]), "")
                    except (ValueError, IndexError):
                        hugo = ""
                else:
                    hugo = ""
                for idx, sample_id in sample_indices:
                    try:
                        raw = parts[idx].strip()
                        if raw in ("", "NA", "null", "NULL"):
                            continue
                        val = float(raw)
                    except (ValueError, IndexError):
                        continue
                    if filter_zeros and val == 0:
                        continue
                    batch.append((study_id, hugo, sample_id, val))
                if len(batch) >= 10_000:
                    conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)
                    batch.clear()
        if batch:
            conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", batch)


def load_study(
    conn,
    study_path: Path,
    load_mutations: bool = False,
    load_cna: bool = False,
    load_sv: bool = False,
    load_timeline: bool = False,
    load_expression: bool = False,
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
    expression_files = (
        list(study_path.glob("data_mrna_seq_*.txt"))
        + list(study_path.glob("data_expression_*.txt"))
        + list(study_path.glob("data_rna_seq_*.txt"))
    )
    protein_files = list(study_path.glob("data_rppa*.txt")) + list(study_path.glob("data_protein_quantification*.txt"))
    methylation_files = list(study_path.glob("data_methylation*.txt"))

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
                            TRY_CAST(cna_value AS FLOAT) as cna_value
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
                        cna_value   FLOAT NOT NULL
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
        if load_expression and expression_files:
            _load_wide_matrix(conn, raw_study_id, expression_files[0],
                              f'"{raw_study_id}_expression"', "expression_value")
            normalize_hugo_symbols(conn, raw_study_id)
            loaded_any = True
        if load_expression and protein_files:
            _load_wide_matrix(conn, raw_study_id, protein_files[0],
                              f'"{raw_study_id}_protein"', "protein_value")
            normalize_hugo_symbols(conn, raw_study_id)
            loaded_any = True
        # Methylation skipped — 22k probes × hundreds of samples produces hundreds of
        # millions of rows across pan-cancer studies with no current web view consumer.
        if load_expression:
            # Load generic assay profiles (treatment response, etc.)
            # We load ALL meta_*.txt files with genetic_alteration_type=GENERIC_ASSAY
            for meta_path in sorted(study_path.glob("meta_*.txt")):
                meta = parse_meta_file(meta_path)
                if meta.get("genetic_alteration_type") != "GENERIC_ASSAY":
                    continue
                stable_id = meta.get("stable_id", "")
                data_filename = meta.get("data_filename", "")
                if not stable_id or not data_filename:
                    continue
                data_file = study_path / data_filename
                if not data_file.exists():
                    continue
                raw_props = meta.get("generic_entity_meta_properties", "")
                meta_props = {p.strip() for p in raw_props.split(",") if p.strip()} if raw_props else set()
                _load_generic_assay(conn, raw_study_id, data_file, stable_id, meta_props)
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

        # Parse meta_*.txt files for molecular profile metadata (profile names, etc.)
        from .molecular_profiles import load_molecular_profiles
        load_molecular_profiles(conn, raw_study_id, study_path)

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
    load_expression: bool = False,
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
    failed_studies = []
    with typer.progressbar(studies, label="Loading studies") as progress:
        for study_path in progress:
            try:
                load_study_metadata(conn, study_path)
                if load_study(conn, study_path, load_mutations=load_mutations, load_cna=load_cna, load_sv=load_sv, load_timeline=load_timeline, load_expression=load_expression):
                    total_loaded += 1
                conn.execute("CHECKPOINT")
            except Exception as e:
                failed_studies.append((study_path.name, str(e)))
                typer.echo(f"\nSkipping {study_path.name}: {e}")
                continue

    if failed_studies:
        typer.echo(f"\n{len(failed_studies)} studies failed:")
        for name, err in failed_studies:
            typer.echo(f"  {name}: {err}")
    create_global_views(conn)
    metrics = monitor.get_metrics()
    return total_loaded, metrics
