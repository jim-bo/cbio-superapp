# cbio-revamp: Implementation Blueprint

## Tech Stack (uv-managed)
- **Runtime:** Python 3.12+ (managed via `uv`)
- **CLI:** Typer (Command-line interface)
- **Web:** FastAPI + Jinja2 + HTMX + Alpine.js
- **Database:** DuckDB (Local persistence)
- **Cache:** Redis (Optional fragment caching)

## Project Structure (src-layout)
```text
cbio-revamp/
├── pyproject.toml           # uv project/dependency config
├── src/
│   └── cbioportal/          # Main package
│       ├── cli/             # CLI: fetch, db, serve commands
│       ├── core/            # Logic: DuckDB, data fetching
│       └── web/             # Web: app.py, routes, templates/
├── tests/                   # Pytest suite
└── data/                    # Local .duckdb storage (git-ignored)
```

## CLI Entry Points
- `cbioportal fetch`: Download study data.
- `cbioportal db`: Add/Remove/Update studies in DuckDB.
- `cbioportal serve`: Launch FastAPI/HTMX webserver.


## Testing

### Unit tests (fast, no real data)
All data-fetch functions in `src/cbioportal/core/` must have corresponding unit tests
in `tests/unit/` using an **in-memory DuckDB** (`duckdb.connect(":memory:")`).

Run unit tests:
```bash
uv run pytest tests/unit/ -v
```

Unit tests must cover:
- The "happy path" (data is returned correctly)
- Exclusion/inclusion logic (e.g. variant classifications, Mutation_Status filters)
- Edge cases (NULL values, empty tables, missing columns)

### Integration / golden tests
`tests/test_study_view_charts.py` runs against the real DuckDB with loaded study data.
These are slower; run them before opening a PR:
```bash
uv run pytest tests/test_study_view_charts.py -v
```

**Any new feature that touches a repository function must pass both test suites.**

### Golden value protection
Exact numeric assertions in `test_study_view_charts.py` (Pearson/Spearman correlations,
gene counts, survival values, etc.) are pinned against the public cBioPortal portal.

**Never change a golden value to make a failing test pass.** A failing golden test signals
a computation divergence from the legacy portal — fix the code, not the number. Only update
a golden value when the user explicitly approves it after reviewing the discrepancy.

## git
- every feature needs to be implemented on a feature branch
- don't credit the coding agents in commit messages, keep them short
