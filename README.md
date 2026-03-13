# cBioPortal Revamp Implementation

A modern, lightweight cBioPortal using FastAPI, Jinja2, HTMX, and DuckDB.

## Overview

Reimplements the cBioPortal web interface with a minimal Python stack — no Java,
no Spring Boot, no separate database server. Study data is loaded from the cBioPortal
datahub into a local DuckDB file; the FastAPI server serves the UI with HTMX-powered
partial updates.

## Prerequisites

- [uv](https://github.com/astral-sh/uv) for package management
- A local clone of [cBioPortal/datahub](https://github.com/cBioPortal/datahub)

## Setup

1. Set environment variables:
   ```bash
   export CBIO_DATAHUB=/path/to/your/datahub
   # Optional: override DuckDB path (default: data/cbioportal.duckdb)
   export CBIO_DB_PATH=/path/to/cbioportal.duckdb
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

## CLI Reference

All commands run via `uv run cbioportal <command>`.

### `db load-all`

Load all studies from the datahub into DuckDB.

```bash
uv run cbioportal db load-all [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit N` | Load only the first N studies (for testing) |
| `--offset N` | Skip the first N studies |
| `--mutations / --no-mutations` | Load mutation data (default: enabled) |
| `--cna / --no-cna` | Load CNA data (default: enabled) |
| `--sv / --no-sv` | Load structural variant data (default: disabled) |
| `--timeline / --no-timeline` | Load timeline data (default: disabled) |

Gene reference tables (`gene_reference`, `gene_symbol_updates`, `gene_alias`) are loaded
automatically from `CBIO_DATAHUB` before study ingestion begins.

### `db add <study_id>`

Load a single study by its ID (directory name in datahub).

```bash
uv run cbioportal db add <study_id> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--mutations / --no-mutations` | Load mutation data (default: enabled) |
| `--cna / --no-cna` | Load CNA data (default: enabled) |
| `--sv / --no-sv` | Load structural variant data (default: disabled) |
| `--timeline / --no-timeline` | Load timeline data (default: disabled) |

### `db remove <study_id>`

Remove all tables for a study from DuckDB and drop it from the studies table.

```bash
uv run cbioportal db remove <study_id>
```

### `db load-lfs <study_id>`

Re-load a single study's genomic data (mutations, CNA, SV) — useful after `git lfs pull`
fetches the large data files that weren't available during initial `db add`.

```bash
uv run cbioportal db load-lfs <study_id> [OPTIONS]
```

Accepts the same `--mutations`, `--cna`, `--sv`, `--timeline` flags as `db add`.

### `db sync-oncotree`

Fetch the latest OncoTree cancer type hierarchy from the MSKCC API and store it in DuckDB.
Required for correct study categorization on the homepage.

```bash
uv run cbioportal db sync-oncotree
```

### `db sync-gene-reference`

Load/reload the `gene_reference` table from `$CBIO_DATAHUB/.circleci/portalinfo/genes.json`.
Maps Entrez Gene ID → canonical HGNC Hugo symbol. Used by Pass 1 of Hugo normalization.

```bash
uv run cbioportal db sync-gene-reference
```

### `db sync-gene-symbol-updates`

Load/reload the `gene_symbol_updates` table from
`$CBIO_DATAHUB/seedDB/gene-update-list/gene-update.md`.
Covers ~75 explicitly renamed genes (e.g. C10ORF12 → LCOR). Used by Pass 2 of Hugo normalization.

```bash
uv run cbioportal db sync-gene-symbol-updates
```

### `db sync-gene-aliases`

Load/reload the `gene_alias` table from the cBioPortal seed SQL
(`$CBIO_DATAHUB/seedDB/seed-cbioportal_hg19_hg38_*.sql.gz`).
Contains ~55k NCBI alias entries. Required for Pass 3 of Hugo normalization (KMT2 family etc).

```bash
uv run cbioportal db sync-gene-aliases
```

### `db sync-gene-panels`

Load/reload gene panel definitions from
`$CBIO_DATAHUB/.circleci/portalinfo/gene-panels.json`.

```bash
uv run cbioportal db sync-gene-panels
```

## Running the Web App

```bash
uv run cbioportal serve
```

The server starts on `http://localhost:8000` by default.

## Testing

### Unit tests (fast, no real data)

```bash
uv run pytest tests/unit/ -v
```

Uses in-memory DuckDB. No real study data needed. Covers repository functions,
loader logic, and edge cases (NULLs, empty tables, missing columns).

### Integration / golden tests

```bash
uv run pytest tests/test_study_view_charts.py -v
```

Requires a real DuckDB with `msk_chord_2024` loaded. Compares chart data against
JSON fixtures in `tests/fixtures/` captured from the public cBioPortal.

To refresh golden fixtures:
```bash
uv run python tests/capture_golden.py
```

## Institutional Knowledge

### Gene counts must match cBioPortal's logic exactly

**1. Variant Classification filtering at load time**

8 variant classifications are excluded at import:
`Silent`, `Intron`, `IGR`, `3'UTR`, `5'UTR`, `3'Flank`, `5'Flank`

This matches cBioPortal File-Formats.md. Rows with these VCs are never stored in DuckDB.

**Exception:** TERT `5'Flank` (promoter mutations) is kept. Note: ONLY `5'Flank`, not all
TERT variants. Using a broad `Hugo_Symbol='TERT'` exception would include TERT `5'UTR`
rows, overcounting mutated samples by ~1.

**2. Hugo symbol normalization is required for accurate gene counts**

Many studies ship stale Hugo symbols paired with correct Entrez IDs. Without normalization,
e.g. MLL2 and KMT2D count as separate genes, causing significant undercounting. Three
reference tables are needed (all sourced from the datahub):

| Table | Source | Purpose |
|-------|--------|---------|
| `gene_reference` | `genes.json` | Entrez ID → canonical Hugo symbol (~40k entries) |
| `gene_symbol_updates` | `gene-update.md` | Explicitly renamed genes (~75 entries) |
| `gene_alias` | seed SQL `gene_alias` table | NCBI aliases, needed when `Entrez_Gene_Id=0` (~55k entries) |

The alias table is specifically needed for the KMT2 family: studies like `msk_chord_2024`
encode these genes with `Entrez_Gene_Id=0` using old names (MLL→KMT2A, MLL2→KMT2D,
MLL3→KMT2C, MLL4→KMT2B).

**3. CBIO_DATAHUB must be set when loading studies**

Without it, `ensure_gene_reference()` is skipped and gene counts will diverge from the
public portal. No error is raised — the load succeeds silently with unnormalized symbols.

**4. Mutation_Status=UNCALLED filtering happens at query time, not load time**

UNCALLED rows are kept in DuckDB. The repository layer filters them out when counting
mutated samples. This matches cBioPortal's behavior and allows future query flexibility.
