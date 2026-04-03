# DB Optimization Plan: DOUBLE → FLOAT for Genomic Matrix Tables

## Why this is the biggest win

The 23 GB database is dominated by genomic matrix data stored as DOUBLE (8 bytes per value):

| Tables | Row count | Theoretical savings |
|---|---|---|
| 206 `{study_id}_expression` | 1.55 billion | ~6 GB |
| 320 `{study_id}_cna` | 574 million | ~2 GB |
| 64 `{study_id}_methylation` | 294 million | ~1 GB |
| 86 `{study_id}_protein` | 17 million | ~0.1 GB |

FLOAT (4 bytes) is sufficient for all of these:
- CNA values are only 5 discrete values: `-2.0, -1.5, -1.0, 1.0, 2.0` — all exactly representable in float32
- Expression (RSEM, log2 RPKM) and methylation (beta values) don't need 15 decimal digits of precision
- All query thresholds (`cna_value <= -1.5`, `cna_value >= 2`) continue to work identically

**Note:** The consolidated `cna`, `expression`, `methylation`, `protein` are already DuckDB VIEWs
(not physical tables) — they cost 0 bytes. The 23 GB is entirely from per-study tables.

---

## Approach: in-place migration script + loader fix

### Step 1: Migration script (`scripts/optimize_db.py`)

Transforms the existing database without needing source study files.

For each per-study table matching `*_cna`, `*_expression`, `*_methylation`, `*_protein`:
1. Check if value column is already FLOAT → skip (resume support)
2. `CREATE TABLE "{name}_opt" AS SELECT study_id, hugo_symbol, sample_id, CAST({val_col} AS FLOAT) AS {val_col} FROM "{name}"`
3. `DROP TABLE "{name}"`
4. `ALTER TABLE "{name}_opt" RENAME TO "{name}"`
5. `CHECKPOINT` every 25 tables

After all tables: compact the file via DuckDB export/reimport (required to actually shrink the file on disk — DROP TABLE alone does not truncate):
```python
conn.execute("EXPORT DATABASE '/tmp/cbio_export' (FORMAT PARQUET)")
conn.close()
shutil.move(db_path, db_path + ".bak")
new_conn = duckdb.connect(str(db_path))
new_conn.execute("IMPORT DATABASE '/tmp/cbio_export'")
```

**Skip:** `_ga_*` (generic assay), `_patient`, `_sample`, `_sv`, `_gene_panel`, `_timeline_*`,
`_genomic_event_derived`, `_profiled_counts`, `_mutations` — these don't benefit from this change.

### Step 2: Fix the loader so future loads use FLOAT

**File:** `src/cbioportal/core/loader/__init__.py`

Two locations in `_load_wide_matrix()`:
- Line 199 (UNPIVOT fast path): `TRY_CAST({value_col} AS DOUBLE)` → `FLOAT`
- Line 214 (slow path DDL): `{value_col} DOUBLE NOT NULL` → `FLOAT NOT NULL`

Two locations in the CNA block (~lines 370–410):
- Fast path `TRY_CAST(cna_value AS DOUBLE)` → `FLOAT`
- Slow path DDL `cna_value DOUBLE NOT NULL` → `FLOAT NOT NULL`

### Step 3: Update test fixtures (DOUBLE → FLOAT in DDL strings)

These are mechanical string changes in unit test fixture setup:

- `tests/unit/test_get_cna_genes.py:25`
- `tests/unit/test_cna_loader.py:71,93`
- `tests/unit/test_plots_repository.py:58,116,133,146`
- `tests/unit/test_annotation_annotators.py:47`
- `tests/unit/test_oncoprint_repository.py:38` (was INTEGER — change to FLOAT)
- `tests/unit/test_build_filter_subquery.py:102` (was INTEGER — change to FLOAT)
- `tests/repro_gene_alias.py:213,241`
- `tests/performance/test_cna_load_strategies.py` (all DOUBLE DDLs)

No golden test changes needed — CNA counts are integers, `-1.5` threshold is exact in float32,
scatter correlations use FGA not expression.

---

## Verification

```bash
# 1. Baseline
ls -lh data/cbioportal.duckdb

# 2. Run unit tests (before touching the real DB)
uv run pytest tests/unit/ -x

# 3. Run migration against the small test DB first
cp intialized_cbioportal.duckdb /tmp/test_optimize.duckdb
uv run python scripts/optimize_db.py /tmp/test_optimize.duckdb
uv run pytest tests/web/ -x --db /tmp/test_optimize.duckdb  # golden tests

# 4. Spot-check types
uv run python -c "
import duckdb; c = duckdb.connect('/tmp/test_optimize.duckdb', read_only=True)
print(c.execute('DESCRIBE msk_chord_2024_cna').fetchall())
print(c.execute('SELECT DISTINCT cna_value FROM msk_chord_2024_cna ORDER BY 1').fetchall())
"
# cna_value should be FLOAT; values should be (-2.0, -1.5, -1.0, 1.0, 2.0)

# 5. Run against the 23 GB production copy
uv run python scripts/optimize_db.py data/cbioportal.duckdb
ls -lh data/cbioportal.duckdb  # expect ~12–14 GB
```

---

## Order of implementation

1. `scripts/optimize_db.py` — the migration script
2. Loader changes in `__init__.py` (4 locations)
3. Test fixture updates (mechanical)
4. Run full test suite
5. Run migration against `intialized_cbioportal.duckdb` + golden tests
6. Run migration against `data/cbioportal.duckdb`
