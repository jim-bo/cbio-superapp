# Load & Smoke Tests

Locust-based load tests that exercise the most expensive parts of the cBioPortal study view dashboard.

## Prerequisites

```bash
# Install dev deps (locust is in the dev group)
uv sync --dev
```

The container must be running before you start a test:

```bash
# Local Docker (bind-mounted DB — fast, no GCS)
inv run-local --db-file ../cbio-implement/data/cbioportal.duckdb --port 8082
```

---

## Quick smoke test (~30 s)

Verifies all endpoints respond without errors using 2 concurrent users.

```bash
inv smoke-test
open tests/load/smoke-report.html
```

---

## Full load test (2 min, 20 users)

```bash
inv load-test
open tests/load/load-report.html
```

Override users / duration:

```bash
inv load-test --users 50 --duration 300s
```

---

## Against Cloud Run

```bash
inv load-test --host https://cbio-revamp-<hash>.run.app
```

---

## Interactive web UI

Run without `--headless` to get the Locust dashboard at http://localhost:8089:

```bash
uv run locust -f tests/load/locustfile.py --host http://localhost:8082
```

Then set user count and spawn rate in the browser and hit **Start**.

---

## User classes

| Class | Weight | What it simulates |
|---|---|---|
| `StudyViewUser` | 3 | Opening a study dashboard; all chart widgets load |
| `HomepageUser` | 2 | Browsing the homepage, filtering by cancer type |
| `HeavyQueryUser` | 1 | Back-to-back heavy queries on the 54k-sample cohort |

Studies used: `msk_impact_50k_2026` (54k samples), `msk_chord_2024` (25k), `ccle_broad_2019` (817k mutations).

---

## Expected baselines (local Docker, M-series Mac)

| Endpoint | p95 target |
|---|---|
| `GET /` homepage | < 200 ms |
| `POST /study/summary/navbar-counts` | < 200 ms |
| `POST /chart/clinical` | < 500 ms |
| `POST /chart/mutated-genes` | < 3 s |
| `POST /chart/scatter` (TMB vs FGA) | < 3 s |
| `POST /chart/km` (Kaplan-Meier) | < 3 s |

No 5xx errors expected at 20 concurrent users.
