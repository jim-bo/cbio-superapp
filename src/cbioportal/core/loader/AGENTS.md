# core/loader/

Study ingestion pipeline — loads cBioPortal-format study files into DuckDB.

## Biology context

A cBioPortal study is a directory of tab-separated files. Each file type maps to a
domain concept:
- `data_clinical_patient.txt` / `data_clinical_sample.txt` — demographics, survival, treatment
- `data_mutations.txt` — somatic mutations (Hugo symbol, variant class, sample)
- `data_CNA.txt` (discrete) — copy-number alterations (+2 amp, -2 homdel per gene per sample)
- `data_sv.txt` — structural variants (gene1/gene2 fusions, deletions, inversions)
- `*.seg` — segmentation files for computing Fraction Genome Altered (FGA)

Gene names in mutation files are not always canonical. Studies submitted years apart
use different Hugo symbols for the same gene (e.g. MLL2 vs KMT2D), so normalization
is essential for accurate mutation counts.

## Engineering context

- Entry point: `load_study(conn, study_path)` in `__init__.py`.
- All modules receive a DuckDB connection; no files are returned — side effects only.
- `study_categories.yaml` maps cancer types to display categories (loaded once at import).
- Hugo normalization is a 3-pass system — see `hugo.py` for the full algorithm.
- FGA is computed from the SEG file and injected as a column into clinical sample data.
- Gene panel tables track which samples were profiled for which genes (required for
  correct frequency denominators in mutation/CNA/SV charts).

## Key files

- `__init__.py` — Public API: `load_study()`, `load_all_studies()`, `Monitor`
- `discovery.py` — `find_study_path()`, `discover_studies()`, `parse_meta_file()`
- `clinical.py` — Load clinical TSV files, `parse_clinical_headers()`
- `genomic.py` — Load mutations, CNA, SV; `_inject_fga_from_seg()`
- `hugo.py` — `normalize_hugo_symbols()` — 3-pass normalization (see dual-personality docstring)
- `gene_reference.py` — `ensure_gene_reference()`, gene/alias/oncotree sync from datahub
- `schema.py` — `create_global_views()`, `categorize_study()`, `load_study_metadata()`

## When to cite legacy code

- Excluded variant classifications (`_EXCLUDED_VCS` in `genomic.py`) mirror
  `MutationDataUtils.java:_EXCLUDED_VCS` in the Java backend.
- TERT 5'Flank keep logic mirrors `MutationDataUtils.java:shouldExcludeVariant()`.
- Hugo normalization mirrors the 3-pass system in `GeneDataUtils.java`.
