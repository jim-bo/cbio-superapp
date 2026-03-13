# study_view templates

This directory contains two chart systems that share the same backend endpoints but
render differently. Know which system you're modifying before making changes.

---

## Two dashboards, two rendering patterns

### 1. HTMX dashboard (`page.html` + `partials/charts/`)

Used at `/study/summary?id=<study_id>`. Server-rendered HTML partials, swapped in
place by HTMX when filters change.

**How a chart works:**
- Each chart lives in its own file under `partials/charts/` (e.g. `pie_chart.html`,
  `histogram.html`).
- The root element carries HTMX attributes:
  ```html
  hx-post="/study/summary/chart/<endpoint>"
  hx-target="#chart-<id>"
  hx-trigger="filterChanged from:body"
  hx-include="#study-id-input,#filter-json-input"
  ```
- On trigger the server re-renders the partial and HTMX swaps the entire element.
- Chart rendering uses **React 16 + Victory 30** loaded as CDN UMD globals. Data is
  embedded in a `<script type="application/json">` tag; an IIFE reads it and calls
  `ReactDOM.render(React.createElement(Victory.*…), el)`.

**Adding a chart to this dashboard:**
1. Add a route handler in `study_view.py` returning the partial template.
2. Create `partials/charts/<name>.html` with HTMX attrs + Victory render script.
3. Include the partial in `partials/summary_tab.html`.

---

### 2. Vanilla ECharts dashboard (`vanilla_dashboard.html`)

Used at `/study/vanilla?id=<study_id>`. Single-page template with GridStack layout
and ECharts 5.5. No server-side rendering of chart HTML — all chart updates are driven
by JS `fetch()` calls to `?format=json` endpoints.

**Global JS state:**

```js
const DashboardState = { studyId, nPatients, nSamples, filters: { ... } };
const Charts = { Pies: {}, AgeHistogram: null, /* one entry per chart type */ };
```

**How a chart works:**
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

**Adding a chart to this dashboard:**
1. Ensure the route handler returns `{"data": [...], ...}` when `format=json`.
2. Add a `grid-stack-item` block in the HTML with a unique `id` on the ECharts div.
3. Add a `null` entry in `Charts` (e.g. `Charts.MyChart: null`).
4. Write `async function updateMyChartWidget()` following the pattern above.
5. Call `updateMyChartWidget()` inside `updateAll()`.
6. Add `if (Charts.MyChart) Charts.MyChart.resize()` in the resizer.

---

## Backend endpoint conventions

Chart routes live in `src/cbioportal/web/routes/study_view.py`.

- Endpoints accept `POST` with form fields: `study_id`, `filter_json` (JSON string).
- The query parameter `?format=json` switches between:
  - **HTML** — returns a Jinja2 `TemplateResponse` (for HTMX dashboard)
  - **JSON** — returns a plain dict/list (for vanilla dashboard `fetch()` calls)
- Repository functions live in `src/cbioportal/core/study_view_repository.py`.
- Every repository function must have unit tests in `tests/unit/` using in-memory
  DuckDB, and integration tests in `tests/test_study_view_charts.py`.
