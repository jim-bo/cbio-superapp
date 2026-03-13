# core/

Data access and loading logic.

- `database.py` — DuckDB connection (`get_connection()`)
- `loader.py` — Study ingestion, Hugo symbol normalization, gene reference loading.
  See docstrings for gene-counting gotchas (TERT VC filtering, KMT2 alias normalization).
- `study_repository.py` — Homepage queries
- `study_view_repository.py` — Study view queries
- `fetcher.py` — Data download utilities

All functions here must have unit tests in `tests/unit/` using in-memory DuckDB.
