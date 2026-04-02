# web/routes/

FastAPI route handlers — one file per page/feature area.

## Biology context

Routes are the HTTP boundary between the browser dashboard and the DuckDB query layer.
Chart endpoints accept the current filter state (as `filter_json`) and return the data
for one chart in the filtered cohort. Understanding the filter model is essential:
see `web/schemas.py` → `DashboardFilters`.

## Engineering context — threading rules (do not regress)

**All route handlers must be plain `def`, not `async def`.**
DuckDB queries are blocking. An `async def` handler runs on the asyncio event loop; any
blocking call there freezes *all* in-flight requests until it returns. Plain `def` handlers
run in FastAPI's anyio thread pool (default 40 threads), enabling true parallelism.
This was validated under 100-concurrent-user load testing — see `README.md` and `tests/load/`.

**All routes that touch DuckDB must use `conn=Depends(get_db)`.**
`get_db()` borrows a connection from the pre-created `queue.Queue` pool in `database.py`.
Never open a new DuckDB connection inside a route handler or call `duckdb.connect()` from
a request thread — this triggers a C-level race that corrupts memory under concurrent load.

## Engineering context

- All chart endpoints accept `POST` with form fields: `study_id`, `filter_json`.
- `filter_json` is validated via `_parse_filters()` using `DashboardFilters.model_validate_json()`.
  Invalid filter JSON returns HTTP 400 with a descriptive error.
- All chart endpoints are decorated with `response_model=` — FastAPI uses this to
  serialize responses and generate OpenAPI documentation.
- Page endpoints return `HTMLResponse` (Jinja2 template render).
- Chart endpoints return JSON (FastAPI auto-serializes Pydantic models or plain dicts).
- The `?format=json` query param is accepted but ignored — all chart endpoints always return JSON.
- Repository functions live in `core/study_view/`; routes are thin wrappers.

## Key files

- `home.py` — Homepage routes (`/`, `/studies`)
- `study_view.py` — Study view routes (`/study/summary`, all `/study/summary/chart/*`)
- `session.py` — Session service REST API (`/api/session/*`); specific routes before generic wildcard

## When to cite legacy code

Route URL paths mirror the cBioPortal Java API where equivalent endpoints exist.
Data shapes (response JSON keys) must match the JS client in `study_view/study_view.js`.
