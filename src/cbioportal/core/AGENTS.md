# core/

Data access and loading logic for the cBioPortal Python port.

## Biology context

cBioPortal studies contain clinical data (patient demographics, survival, treatment)
and molecular data (mutations, copy-number alterations, structural variants). Each
study is self-contained: its data files are loaded into per-study DuckDB tables and
queried independently or via global union views.

Gene names are biologically unstable — the same gene can appear under multiple symbols
across studies (e.g. MLL2 vs KMT2D). Normalization is required before aggregating
counts across studies.

## Engineering context

- DuckDB is the single persistence layer. No ORM. All queries are raw SQL.
- Per-study tables are named `"{study_id}_sample"`, `"{study_id}_mutations"`, etc.
- Global union views (`mutations`, `cna`, `sv`, `clinical_sample`) span all loaded studies.
- All core functions take a `conn` (DuckDB connection) as the first argument.
- All functions must have unit tests in `tests/unit/` using an in-memory DuckDB.

## Key files

- `database.py` — DuckDB connection factory (`get_connection()`)
- `session_repository.py` — SQLAlchemy sessions DB (SQLite dev / PostgreSQL prod); model, engine factory, CRUD
- `fetcher.py` — Data download utilities (fetch study files from cBioPortal datahub)
- `study_repository.py` — Homepage queries (study list, cancer type counts)
- `study_view_repository.py` — Backward-compat shim; imports from `study_view/`
- `loader/` — Study ingestion pipeline (see `loader/AGENTS.md`)
- `study_view/` — Study view dashboard queries (see `study_view/AGENTS.md`)

## When to cite legacy code

Filtering rules (excluded variant classifications, TERT special case, UNCALLED handling)
are ported directly from the Java backend. Cite the relevant class when modifying:
`MutationDataUtils.java`, `StudyViewMyBatisRepository.java`.
