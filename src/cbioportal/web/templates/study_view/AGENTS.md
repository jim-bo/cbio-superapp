# study_view templates

One dashboard, one URL: `GET /study/summary?id=<study_id>` → `page.html`.

---

## ECharts vanilla dashboard (`page.html`)

Single-page template with **GridStack** layout and **ECharts 5.5**. All chart updates
are driven by JS `fetch()` calls to `?format=json` endpoints — no server-rendered HTML
partials.

### Global JS state

```js
const DashboardState = { studyId, nPatients, nSamples, filters: { ... } };
const Charts = { Pies: {}, AgeHistogram: null, MutatedGenes: null, /* one entry per chart type */ };
```

### How a chart works

1. A `<div class="grid-stack-item" gs-w gs-h gs-x gs-y>` block defines the widget
   position. The inner `<div id="chart-<name>" class="echarts-container">` is the
   ECharts mount point.
2. An `async function update<Name>Widget()` function:
   - Lazily inits the chart: `if (!Charts.X) Charts.X = echarts.init(chartDom)`
   - POSTs to the `?format=json` endpoint via `fetch()` with `FormData` carrying
     `study_id` and `filter_json`.
   - Calls `chart.setOption({...})` with the returned data.
3. `updateAll()` calls every `update*Widget()` — runs on page load and whenever
   `cbio-filter-changed` is dispatched.
4. The GridStack `resizestop` handler and `window resize` handler call `.resize()` on
   every initialized chart instance.

### Adding a chart

1. Ensure the route handler returns `{"data": [...], ...}` (always JSON — no HTML branch needed).
2. Add a `grid-stack-item` block in `page.html` with a unique `id` on the ECharts div.
3. Add a `null` entry in `Charts` (e.g. `Charts.MyChart: null`).
4. Write `async function updateMyChartWidget()` following the pattern above.
5. Call `updateMyChartWidget()` inside `updateAll()`.
6. Add `if (Charts.MyChart) Charts.MyChart.resize()` in the resizer.

---

## Backend endpoint conventions

Chart routes live in `src/cbioportal/web/routes/study_view.py`.

- Endpoints accept `POST` with form fields: `study_id`, `filter_json` (JSON string).
- All endpoints return JSON — the `?format=json` query param is accepted but ignored
  (kept for backwards compatibility with tests).
- Repository functions live in `src/cbioportal/core/study_view_repository.py`.
- Every repository function must have unit tests in `tests/unit/` using in-memory
  DuckDB, and integration tests in `tests/test_study_view_charts.py`.
