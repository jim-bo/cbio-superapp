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

## DuckDB concurrency — do not regress these constraints

The following rules were established through load testing (see `tests/load/` and `README.md`).
Violating them causes either silent serialisation (all requests queue behind one thread) or
crashes under concurrent load.

**1. Never share a single DuckDB connection across threads.**
DuckDB's Python binding is not thread-safe. Creating connections from multiple threads
simultaneously corrupts internal C state. Connections are created sequentially at startup
and handed out via `queue.Queue` in `database.py`.

**2. Never call `duckdb.connect()` from a request thread.**
Even though it looks safe, concurrent calls to `duckdb.connect()` trigger a C-level init
race that causes `malloc(): double linked list corrupted`. Always borrow from the pool.

**3. Never convert `get_db()` back to yielding a cursor (`conn.cursor()`).**
DuckDB cursors share the underlying C++ connection object — they are also not safe for
concurrent Python threads.

**4. Do not increase `_POOL_SIZE` above 2 without re-running the load test.**
Each connection caches aggressively. 8 connections (pool_size=4 × 2 workers) OOM-killed
the container on the 23 GB database. Two per worker is the tested stable limit.

**5. Do not remove `memory_limit` or `temp_directory` from `configure()`.**
`memory_limit='6GB'` prevents one heavy aggregation from exhausting the host.
`temp_directory='/tmp/duckdb_temp'` is required when the DB file is on a read-only mount
(Cloud Run, local Docker `:ro` bind).

## Key files

- `database.py` — DuckDB connection pool (`configure()`, `get_db()`, `get_connection()`)
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
