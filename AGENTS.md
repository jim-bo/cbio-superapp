# cbio-revamp: Implementation Blueprint

## Tech Stack (uv-managed)
- **Runtime:** Python 3.12+ (managed via `uv`)
- **CLI:** Typer (Command-line interface) + prompt_toolkit (Interactive TUI)
- **Web:** FastAPI + Jinja2 + HTMX + Alpine.js
- **Database:** DuckDB (Local persistence)
- **Cache:** Redis (Optional fragment caching)

## Project Structure (src-layout)
```text
cbio-revamp/
├── pyproject.toml           # uv project/dependency config
├── src/
│   └── cbioportal/          # Main package
│       ├── cli/             # CLI: cbio (TUI), beta db, serve commands
│       ├── core/            # Logic: DuckDB, data fetching, API caching
│       └── web/             # Web: app.py, routes, templates/
├── tests/                   # Pytest suite
└── data/                    # Local .duckdb storage (git-ignored)
```

## Architecture Notes

**Interactive TUI (`src/cbioportal/cli/display/tui`)**
- The CLI (`cbio`) is powered by `prompt_toolkit`.
- It uses an async event loop (`Application.run_async()`) and maintains a global `AppState` object for tracking history, interactive selections, and active background tasks.
- Avoid printing directly to `stdout`. Use `state.history.add()` and `event.app.invalidate()` to trigger UI renders.

**API Caching (`src/cbioportal/core/cache.py`)**
- Live cBioPortal and MoAlmanac API data is persistently cached in `~/.cbio/cache/cache.duckdb`.
- The `data_puller.py` orchestrator relies on this DuckDB cache for high-speed relational joins (e.g., joining OncoTree contexts with large genomic datasets).

## CLI Entry Points
- `cbio`: Launch the interactive Terminal UI (TUI) for searching and pulling API data.
- `cbio serve`: Launch FastAPI/HTMX webserver.
- `cbio beta db`: Add/Remove/Update studies in DuckDB from a local datahub.

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

### Integration / Golden tests
`tests/test_study_view_charts.py` runs against the real DuckDB with loaded study data.
These are slower; run them before opening a PR:
```bash
uv run pytest tests/test_study_view_charts.py -v
```

### API & Export Validation Tests
Marked with `@pytest.mark.live_api` and `@pytest.mark.docker`. They are skipped by default. To run them, explicitly pass `--run-live-api` and `--run-docker`:
```bash
uv run pytest tests/integration/ -v --run-live-api --run-docker
```

**Any new feature that touches a repository function must pass both test suites.**

### Golden value protection
Exact numeric assertions in `test_study_view_charts.py` (Pearson/Spearman correlations,
gene counts, survival values, etc.) are pinned against the public cBioPortal portal.

**Never change a golden value to make a failing test pass.** A failing golden test signals
a computation divergence from the legacy portal — fix the code, not the number. Only update
a golden value when the user explicitly approves it after reviewing the discrepancy.

### Smoke test before committing

Any change to routes, schemas, or templates must be verified by running the server
and hitting every affected endpoint — a 200 on the HTML page is not sufficient.

```bash
uv run cbioportal serve --port 8002 &
STUDY=msk_chord_2024
FILTER='{"clinicalDataFilters":[],"mutationFilter":{"genes":[]},"svFilter":{"genes":[]}}'
BASE=http://127.0.0.1:8002/study/summary

# Page load
curl -sf "$BASE?id=$STUDY" > /dev/null && echo "OK page"

# charts-meta (GET)
curl -sf "$BASE/charts-meta?id=$STUDY" > /dev/null && echo "OK charts-meta"

# All chart endpoints (POST)
for ep in mutated-genes cna-genes sv-genes age scatter km data-types; do
  curl -sf -X POST "$BASE/chart/$ep" \
    -F "study_id=$STUDY" -F "filter_json=$FILTER" > /dev/null && echo "OK $ep"
done

# clinical (requires attribute_id)
curl -sf -X POST "$BASE/chart/clinical" \
  -F "study_id=$STUDY" -F "filter_json=$FILTER" -F "attribute_id=CANCER_TYPE" > /dev/null \
  && echo "OK clinical"

kill %1
```

## git
- every feature needs to be implemented on a feature branch
- don't credit the coding agents in commit messages, keep them short

## Worktree layout

The repo uses git worktrees so coding agents and the user never collide on branches.

```
cbio-revamp/
├── cbio-implement/    # main branch — user's primary working directory, never modify directly
├── claude-worktree/   # Claude's feature branch workspace
└── gemini-worktree/   # Gemini's feature branch workspace
```

**Rules for coding agents:**
- **Claude** does all feature work inside `claude-worktree/`
- **Gemini** does all feature work inside `gemini-worktree/`
- `cbio-implement/` always tracks `main` — agents never check out or commit there
- Each agent creates a feature branch inside its own worktree:
  ```bash
  git -C /path/to/claude-worktree checkout -b feature/my-feature
  ```
- When the feature is ready, the user merges from `cbio-implement/`:
  ```bash
  git merge feature/my-feature
  ```
- After merging, the agent resets its worktree to detached HEAD at main:
  ```bash
  git -C /path/to/claude-worktree checkout --detach main
  git branch -d feature/my-feature
  ```

## Session Service

The session service (`core/session_repository.py`, `web/routes/session.py`,
`web/middleware/session_sync.py`) persists page state so browser refreshes restore
where you were, and lets users share state via a URL.

**Session syncing is server-side (not JS):**
- **Save:** `SessionSyncMiddleware` intercepts chart POSTs (which already carry
  `study_id` + `filter_json`) and fire-and-forgets a session upsert. Zero JS changes.
- **Restore:** The `GET /study/summary` handler looks up the session and injects
  restored filters into the Jinja2 template context. Zero JS async fetch needed.
- **Share:** A "Copy link" button copies `window.location.href` (which already has
  `?session_id=` set by the server on page load).

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CBIO_SESSIONS_DB_URL` | `sqlite:///data/sessions.db` | SQLAlchemy DB URL |
| `CBIO_SECURE_COOKIES` | `0` | Set to `1` for HTTPS deployments |

**Run Alembic migrations (prod):**
```bash
CBIO_SESSIONS_DB_URL=postgresql+psycopg2://user:pass@host/dbname uv run alembic upgrade head
```

### SQLite / PostgreSQL / AlloyDB compatibility rules — MUST follow

1. **Use `JSON` column type — never `JSONB`.** `JSON` maps to text on SQLite; `JSONB`
   breaks SQLite entirely.
2. **Keep `render_as_batch=True` in `alembic/env.py`** — required for SQLite `ALTER TABLE`.
   It is harmless on PostgreSQL/AlloyDB.
3. **JSON path queries must use SQLAlchemy's subscript operator**
   (`Model.data["key"].as_string()`) — never write raw `JSON_EXTRACT()` or `->>` SQL.
   SQLAlchemy compiles this correctly for both dialects.
4. **No AlloyDB-specific features** — no columnar engine, no vector indexes, no pg
   extensions not in standard PostgreSQL 14.
5. **No `RETURNING` in raw SQL** — use `db.refresh(record)` after commit instead.
   SQLite < 3.35 does not support `RETURNING`.
6. **Sessions DB is completely separate from DuckDB.** Never mix the two connections.

### Route ordering in `session.py`

Specific routes (`/settings`, `/settings/fetch`, `/virtual_study/save`, `/share/{id}`)
**must be registered before** the generic `/{session_type}` wildcard routes. FastAPI
matches routes in registration order — putting specific routes after the wildcard causes
them to be silently swallowed by the generic handler.

### In-memory SQLite tests

Use `poolclass=StaticPool` when creating the test engine. Without it, each new
SQLAlchemy connection gets a fresh SQLite in-memory database (empty, no schema).
`StaticPool` forces all connections to share the same in-memory DB instance.

## Web terminal tray (`/terminal`) — deployment caveats

The web terminal tray embeds the `cbio` Textual TUI in the browser via
`textual-serve`. It is **disabled by default** and must NEVER be
exposed publicly without the safeguards below. The app-layer
mitigations (M1–M8) live in:

- `src/cbioportal/web/routes/terminal.py` — feature flag, origin check, CSRF cookie, env scrubbing, scratch cwd
- `src/cbioportal/web/llm_proxy.py` — local reverse proxy so the subprocess never holds the real OpenRouter key
- `src/cbioportal/web/session_limiter.py` — per-IP + global caps, idle reaping
- `src/cbioportal/cli/tools/_paths.py` — path allowlist for all filesystem-touching tools
- `src/cbioportal/cli/tools/_scrub.py` — prompt-injection dampening for tool output

### Env vars

| Variable | Default | Purpose |
|---|---|---|
| `CBIO_TERMINAL_ENABLED` | `0` | Feature flag. `/terminal` returns 404 unless set to `1`. |
| `CBIO_WEB_OPENROUTER_API_KEY` | _(unset)_ | Dedicated, spend-capped OpenRouter key for web sessions. Held in Python memory only — never passed as env to the subprocess. |
| `CBIO_TERMINAL_ALLOWED_ORIGINS` | _(empty)_ | Comma-separated extra origins permitted by the CSRF origin check. Same-origin is always allowed. |
| `CBIO_STUDIES_DIR` | `./studies:./data` | Colon-separated allowlist roots for path-taking tools (`validate_study_folder`, `load_study_into_db`). |
| `CBIO_TERMINAL_MAX_PER_IP` | `2` | Per-client concurrent session cap (429 past the limit). |
| `CBIO_TERMINAL_MAX_TOTAL` | `10` | Global concurrent session cap (503 past the limit). |
| `CBIO_TERMINAL_IDLE_SECONDS` | `900` | Idle session timeout; reaper kills the subprocess. |
| `CBIO_TERMINAL_ALLOW_PUBLIC_BIND` | `0` | Explicit override to let `cbio beta serve` start with `--host 0.0.0.0` while the terminal is enabled. Only set after putting real auth upstream. |
| `CBIO_SECURE_COOKIES` | `0` | Set to `1` for HTTPS deployments so the CSRF cookie gets `Secure`. |

### Required before enabling on a shared deployment

1. **Dedicated OpenRouter key** with a hard daily cap at the provider.
2. **Authentication in front of the webapp.** The current mitigation
   plan does NOT include per-user auth; `CBIO_TERMINAL_ENABLED=1` on a
   public host is a remote-shell footgun.
3. **Non-root subprocess user + network policy** at the deployment
   layer (container UID, read-only FS outside the scratch dir, egress
   limited to the proxy upstream).
4. **Bind to localhost** (`--host 127.0.0.1`). `cbio beta serve` will
   refuse to start with the terminal enabled on any other bind unless
   `CBIO_TERMINAL_ALLOW_PUBLIC_BIND=1` is set as an explicit
   acknowledgement.

### Local dev usage

```bash
CBIO_TERMINAL_ENABLED=1 \
CBIO_WEB_OPENROUTER_API_KEY=sk-or-... \
uv run cbio beta serve --port 8002
```

The `/terminal` route becomes reachable on `127.0.0.1:8002` and the
LLM proxy mounts at `/llm-proxy`. The real OpenRouter key is consumed
from env at startup and immediately cleared from `os.environ` — it
lives only in Python memory in the parent process. Subprocess env
contains a one-shot session token bound to the localhost proxy.
