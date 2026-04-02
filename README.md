# cBioPortal-cli

An unofficial technology demonstration of what you can build with agentic coding tools.
This should not be used for serious work, but you are more than welcome to explore
this offshoot. Feedback is totally welcome.

## Overview

A cbioportal CLI with a minimal Python stack that includes a rich, full-screen Terminal UI (TUI) for interacting with APIs, querying studies, and exporting customized genomic datasets.

![Interface Demonstration](docs/img/tui_demo.gif)

## Installation

### Use it (pip / uv)

No clone required. Install directly from GitHub:

```bash
# with pip
pip install git+https://github.com/jim-bo/cbio-cli.git

# with uv
uv pip install git+https://github.com/jim-bo/cbio-cli.git
```

This installs the `cbio` CLI entry point. Then run:

```bash
cbio
```

### Development Setup

Clone the repo and install with dev dependencies:

```bash
git clone https://github.com/jim-bo/cbio-cli.git
cd cbio-cli
uv sync
uv run cbio
```

**Optional:** [Docker](https://www.docker.com/) is required only for validating MAF exports (`--run-docker`).

## TUI Reference

The primary interface is the interactive Terminal UI.

### `cbio` (Interactive TUI)

Launch the full-screen interactive REPL:

```bash
cbio
```

**Features & Commands within the TUI:**
- `/search [query]`: Search the public cBioPortal for studies.
- `pull`: Start the interactive data-pulling wizard to fetch, annotate (via MoAlmanac), and export MAF files for a selected study.
- `/config`: View the current backend configuration.
- `exit` (or press `Ctrl+D` twice): Safely exit the application.

*Note: The TUI caches API responses in a dedicated DuckDB file at `~/.cbio/cache/cache.duckdb` for high-speed offline analysis.*

## Testing (Core TUI)

### Core Tests 

These tests cover the TUI, API clients, and core DuckDB logic.

```bash
uv run pytest tests/unit/ tests/integration/ -v
```

*Note: Use `--run-live-api` and `--run-docker` for full API and export validation.*

---

## Experimental Features

Documentation for the experimental local web app and datahub ingestion can be found in [BETA.md](BETA.md).

---

## Cloud Run Deployment

Merging to `main` automatically tests, builds, and deploys to Cloud Run via GitHub Actions.

### First-time setup

1. **Run the WIF setup script** (creates GCP service account + Workload Identity Federation):
   ```bash
   GCP_PROJECT=your-project GITHUB_REPO=owner/repo ./scripts/setup-wif.sh
   ```

2. **Add GitHub secrets** (printed by the script):
   | Secret | Description |
   |---|---|
   | `WIF_PROVIDER` | Workload Identity Federation provider resource name |
   | `WIF_SERVICE_ACCOUNT` | GCP service account email |
   | `GCP_PROJECT` | GCP project ID |

3. **Set the GCS bucket** as a GitHub Actions variable (not a secret):
   - Go to Settings → Variables → Actions → New repository variable
   - Name: `CBIO_GCS_BUCKET`, Value: your bucket name

4. **Upload the DuckDB** to GCS:
   ```bash
   inv sync-db
   ```

5. **Push to main** — the workflow will trigger automatically.

### Updating data

The deploy pipeline does not update the DuckDB. To refresh study data:

```bash
# Load/update studies locally
uv run cbio beta db add msk_chord_2024

# Upload to GCS
inv sync-db

# Cloud Run picks up the new DB on next instance start (FUSE mount).
# To force immediate refresh:
gcloud run services update cbio-revamp --region us-central1 --no-traffic-migration
```

---

## Study View Performance: Pre-computed Derived Tables

The study view dashboard shows per-gene mutation/CNA/SV frequencies across a cohort.
Computing these requires joining genomic event tables with gene panel profiling data
to determine the correct denominator (how many samples were actually sequenced for
each gene). This is the most expensive query pattern in the application.

### The problem

The naive approach runs these joins at request time. On a local M4 Mac this takes
~350ms per gene table endpoint. On Cloud Run with GCS FUSE it balloons to ~4 seconds
due to slower CPUs and network-backed storage — unacceptable for an interactive dashboard.

### The solution: `genomic_event_derived`

We mirror the strategy used by cBioPortal's own ClickHouse backend. At **study load
time**, the loader pre-computes two tables per study:

- **`{study_id}_genomic_event_derived`** — a single denormalized table containing all
  mutations, CNAs, and structural variants with panel profiling (`is_profiled`) pre-joined.
  No query-time joins needed.
- **`{study_id}_profiled_counts`** — per-gene, per-variant-type profiled sample counts.
  This is the frequency denominator, pre-aggregated.

At query time, the gene table endpoints become simple GROUP BY queries against the
derived table with a LEFT JOIN to profiled counts — no CTEs, no CROSS JOINs, no
subqueries into `gene_panel_definitions`.

### Benchmarks

Study: `msk_chord_2024` (25,040 samples, 208K mutations, 64K CNAs, 7K SVs)

| Endpoint (unfiltered, warm) | M4 Mac (before) | M4 Mac (after) | Cloud Run FUSE (before) | Cloud Run FUSE (after) |
|---|---|---|---|---|
| **mutated-genes** | 350ms | **11ms** | 3,900ms | **125ms** |
| **cna-genes** | 350ms | **7ms** | 3,900ms | **111ms** |
| **sv-genes** | 350ms | **5ms** | 3,900ms | **100ms** |
| scatter | 50ms | 50ms | 500ms | 314ms |
| age | 9ms | 9ms | 200ms | 101ms |
| km | 36ms | 36ms | 400ms | 178ms |

The gene table queries improved **32-39x on Cloud Run** and **35-70x locally**.
Filtered queries (clicking a cancer type in the dashboard) were already fast (~100ms)
and remain unchanged.

### Trade-offs

- **Load time cost**: the derived tables add a few seconds to study ingestion. For
  `msk_chord_2024` this is negligible relative to the existing load time.
- **Storage**: the derived table adds ~2MB per study (275K rows for msk_chord). The
  profiled counts table is tiny (~2K rows).
- **Correctness**: derived table results match the legacy query-time join implementation
  exactly for all top genes. A legacy fallback path is preserved for databases that
  predate this change.

### Reference

This approach is directly inspired by cBioPortal's ClickHouse backend:
- `genomic_event_derived` table in `clickhouse.sql`
- `ClickhouseAlterationMapper.xml` for the simplified query patterns
- Application-level caching of unfiltered results via EhCache/Redis

## Concurrency and Load Testing

Load tests are in `tests/load/` and use [Locust](https://locust.io). Reports are saved as HTML files in that directory. Run them with:

```bash
inv smoke-test          # 2 users, 30 s — quick sanity check
inv load-test           # 20 users, 120 s — standard baseline
inv load-test --users 100 --duration 120s   # stress test
```

### Benchmark results (local Docker, M-series Mac, 100 concurrent users, 120 s)

Tests ran against the three largest loaded studies: `msk_impact_50k_2026` (54k samples), `msk_chord_2024` (25k), `ccle_broad_2019` (817k mutations).

| Configuration | p50 | p95 | Failures | Report |
|---|---|---|---|---|
| Baseline — 1 worker, 1 shared connection | 25 s | 40 s | 0 | `baseline-single-connection-report.html` |
| Level 1 — 2 uvicorn workers | 18 s | 36 s | 1 | `two-worker-report.html` |
| Level 2 — 2 workers + connection pool (4 total) | **3.6 s** | **6.8 s** | 1\* | `pool-queue-report.html` |

\*Single MetricsUser connection-reset; not a real request failure.

### Why it is designed this way

**`def` routes, not `async def`**
DuckDB queries are CPU-bound and blocking. FastAPI runs `async def` handlers on the asyncio event loop; a blocking DuckDB call there freezes *all* concurrent requests until it completes. Declaring handlers as plain `def` causes FastAPI to run them in its anyio thread pool (default 40 threads), so requests truly execute in parallel.

**Pre-created `queue.Queue` connection pool**
DuckDB's Python binding is not thread-safe for concurrent initialisation. Creating connections from multiple threads simultaneously corrupts internal C state (`malloc(): double linked list corrupted`). The solution is to create all connections sequentially in the main thread at startup and distribute them to request threads via a `queue.Queue`. This is the pattern [recommended by DuckDB for multi-threaded Python servers](https://duckdb.org/docs/api/python/overview.html).

**`_POOL_SIZE = 2` per worker (4 total)**
Each DuckDB connection can cache aggressively. Testing showed that 8 connections (pool_size=4 × 2 workers) exhausted container RAM on the 23 GB database (`OutOfMemoryException`). Two connections per worker is the stable sweet spot: enough concurrency for the anyio thread pool to overlap I/O, small enough to keep peak RSS under 10 GiB.

**`memory_limit = '6GB'` per connection**
Without a cap, a single large aggregation query (e.g. mutated-genes on the 54k-sample cohort) can request a 3–4 GiB contiguous allocation. The cap forces DuckDB to spill intermediate results to `temp_directory` rather than exhausting the host, and prevents one heavy query from starving all others.

**`temp_directory = '/tmp/duckdb_temp'`**
On Cloud Run and local Docker the DuckDB file is mounted read-only (`:ro`). DuckDB's default is to create its temp directory alongside the database file, which fails on a read-only mount. `/tmp` is always writable.
