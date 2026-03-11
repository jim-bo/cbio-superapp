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


## git
- every feature needs to be implemented on a feature branch
- don't credit the coding agents in commit messages, keep them short
