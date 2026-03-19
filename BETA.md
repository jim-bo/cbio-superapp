# [Beta] Web App & Local Database

The experimental web app provides a local UI, and relies on an extensive offline ingestion process utilizing a local clone of the cBioPortal datahub.

## Beta Prerequisites
- A local clone of [cBioPortal/datahub](https://github.com/cBioPortal/datahub)

## Beta Setup

1. Set environment variables:
   ```bash
   export CBIO_DATAHUB=/path/to/your/datahub
   # Optional: override DuckDB path (default: data/cbioportal.duckdb)
   export CBIO_DB_PATH=/path/to/cbioportal.duckdb
   ```

2. Install web extras:
   ```bash
   uv sync --extra web
   ```

## Running the Web App

The web application requires the `web` optional dependencies (FastAPI, uvicorn, etc.).

```bash
# Launch the server
uv run cbio serve
```

The server starts on `http://localhost:8000` by default.

## Advanced Database Commands (`cbio beta db`)

If you need to manually load data from the local datahub clone (bypassing the interactive API puller):

- `uv run cbio beta db load-all`: Load all studies from the datahub into DuckDB.
- `uv run cbio beta db add <study_id>`: Load a single study.
- `uv run cbio beta db sync-oncotree`: Fetch the OncoTree hierarchy.
- `uv run cbio beta db sync-gene-reference`: Load the `gene_reference` table.

## Testing (Web)

### Web & Study View Tests

These tests require the `[web]` optional dependencies (specifically `scipy` for genomic statistics).

```bash
uv run pytest tests/web/ -v
```

### Golden Integration Tests

Compares Study View chart data against JSON fixtures from the public portal. Requires a real DuckDB with `msk_chord_2024` loaded.

```bash
uv run pytest tests/web/test_study_view_charts.py -v
```

## Institutional Knowledge (Beta Loaders)

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

---

## Engineering Decision: CNA Loading Strategy

### Background

CNA files are wide matrices — one row per gene, one column per sample, values of
-2, -1, 0, 1, 2. The loader converts them to long-format `(study_id, hugo_symbol,
sample_id, cna_value)`, keeping only non-zero rows.

The original implementation used DuckDB's `UNPIVOT` statement. This is fast but
crashed with an out-of-memory error on `msk_impact_50k_2026` (54,332 sample
columns, 57 MB source file), exhausting all 38.3 GB of system RAM.

### Why UNPIVOT runs out of memory

DuckDB's UNPIVOT materialises the entire wide matrix as an intermediate result
before filtering. With `all_varchar=True` (required to handle mixed-type columns
and non-standard values like `-1.5`), every cell is stored as a string. For a
54k-column file that intermediate buffer is tens of gigabytes — allocated entirely
inside DuckDB's C engine, invisible to Python's memory profiler.

### Benchmarks

Measured with `psutil.Process().memory_info().rss` (captures Python heap +
DuckDB C-level allocations). Tests live in `tests/performance/` and run with:

```bash
uv run pytest tests/performance/ -v --run-perf -s
```

| Study | Source file | Samples | Strategy | Wall time | Peak RSS delta |
|---|---|---|---|---|---|
| `acyc_fmi_2014` | 2.1 KB | 28 | UNPIVOT | 0.004s | ~2 MB |
| `acyc_fmi_2014` | 2.1 KB | 28 | Python | 0.005s | ~0 MB |
| `hcc_msk_2024` | 1.4 MB | 1,371 | UNPIVOT | 0.69s | 53 MB |
| `hcc_msk_2024` | 1.4 MB | 1,371 | Python | 0.91s | ~0 MB |
| `msk_met_2021` | 27 MB | 25,776 | UNPIVOT | 14.9s | **619 MB** |
| `msk_met_2021` | 27 MB | 25,776 | Python | 21.2s | **2 MB** |
| `msk_impact_50k_2026` | 57 MB | 54,332 | UNPIVOT | **OOM** | 38.3 GB ❌ |
| `msk_impact_50k_2026` | 57 MB | 54,332 | Python | 50s | 78 MB ✓ |

Key observations:
- **UNPIVOT RSS scales with column count** — 53 MB at 1k samples, 619 MB at 25k,
  fatal at 54k. Roughly O(n_samples).
- **Python RSS is flat** — the row-by-row loop holds one gene row in memory at a
  time regardless of matrix width.
- **Speed is comparable** at moderate sizes (0.69s vs 0.91s at 1,371 samples).
  Python is ~40% slower at 25k samples but does not crash.
- Both strategies produce **identical output** — verified by
  `test_cna_strategies_produce_identical_output`.

### Decision

Switch CNA loading to the Python row-by-row approach. The speed difference is
acceptable; crashing the bulk loader for all 508 studies is not.

The Python approach also eliminates several UNPIVOT edge cases that each required
a separate fix:
- Files with both `Hugo_Symbol` and `Entrez_Gene_Id` (EXCLUDE clause must be dynamic)
- Files with only `Entrez_Gene_Id` (requires `gene_reference` join)
- Files with mixed BIGINT/VARCHAR sample columns (`all_varchar=True` workaround)
- Files with float values like `-1.5` (`round(float(...))` to match DuckDB rounding)
- Files with very long lines that confuse DuckDB's CSV sniffer

### Float rounding

Some CNA files contain values like `-1.5`. DuckDB's `TRY_CAST('-1.5' AS INTEGER)`
rounds to `-2`. The Python loader matches this with `round(float(raw_val))`,
verified to produce identical row counts.

---

## Full Load Run: All 508 Studies

### Result

All 508 studies loaded successfully in **32 minutes** (1,927 seconds). Peak RSS: 8.8 GB.
DB size: 11.7 GB. 314 CNA tables, 503 mutation tables.

### Errors Encountered and Fixes

#### 1. CNA loading too slow — Python row-by-row for all files

**Symptom:** First load-all attempt ran for 10+ hours and only reached ~1.5 GB. Root cause:
the Python row-by-row CNA loader (safe for 50k-sample files) was used for *all* CNA files,
including small ones (818 samples × 22k genes = 18M Python iterations per study).

**Fix:** Hybrid strategy based on sample column count:
- **≤ 5,000 samples → DuckDB UNPIVOT** (fast, sub-second for typical studies)
- **> 5,000 samples → Python row-by-row** (O(1) memory, required for msk_impact_50k_2026)

Only 5 of 508 studies exceed 5,000 samples: `msk_impact_50k_2026` (54k),
`msk_met_2021` (25k), `msk_chord_2024` (25k), `msk_impact_2017` (10k),
`pan_origimed_2020` (10k). All others use UNPIVOT.

**File:** `src/cbioportal/core/loader/__init__.py` (CNA loading block)

#### 2. Study categories not applied — `study_categories.yaml` wrong path

**Symptom:** All studies categorized as cancer types (e.g. "Lung", "Breast") instead of
special collections (PanCancer Studies, Pediatric Cancer Studies, etc.). `msk_chord_2024`
and 10 other PanCancer studies showed up under "Other" or their cancer type.

**Cause:** `schema.py` resolved `study_categories.yaml` to the worktree root (5 parent dirs
up from `schema.py`). The file only existed in `tests/fixtures/`, not the worktree root.

**Fix:**
1. Moved `study_categories.yaml` to `src/cbioportal/core/loader/study_categories.yaml`
   (alongside the code that reads it).
2. Changed path resolution to `Path(__file__).resolve().parent / "study_categories.yaml"`.
3. Changed YAML loading to **first-wins** — if a study appears in multiple categories,
   the first category in the YAML file takes precedence. `tmb_mskcc_2018` appears in both
   PanCancer and Immunogenomic; first-wins assigns it to PanCancer.

**Known minor discrepancy:** Immunogenomic Studies shows 7 vs 8 on the public portal.
The public portal counts `tmb_mskcc_2018` in both PanCancer and Immunogenomic simultaneously.
Our single-category schema can only assign it to one. PanCancer=11 is prioritised.

**File:** `src/cbioportal/core/loader/schema.py`, `src/cbioportal/core/loader/study_categories.yaml`

#### 3. `luad_cptac_2020` — non-standard mutation column names

**Symptom:** `BinderError: Table "luad_cptac_2020_mutations" does not have a column named "Entrez_Gene_Id". Candidate bindings: "Gene"`

**Cause:** `luad_cptac_2020/data_mutations.txt` uses `Gene` instead of `Entrez_Gene_Id`.
The `normalize_hugo_symbols` function hardcoded `Entrez_Gene_Id` in all three UPDATE passes.

**Fix:** `normalize_hugo_symbols` now calls `DESCRIBE "{table}"` to detect available columns
before each pass. If `Entrez_Gene_Id` is absent, Passes 1 and 3 (Entrez-based) are skipped;
Pass 2 (symbol rename map) still runs if `Hugo_Symbol` is present.

**File:** `src/cbioportal/core/loader/hugo.py`
