# tests/unit/

Fast unit tests for all `core/` functions. No real study data — all tests use
in-memory DuckDB (`duckdb.connect(":memory:")`).

## Engineering context

- `conftest.py` provides shared fixtures (in-memory DuckDB, minimal schema setup).
- Each test file corresponds to one repository or loader function.
- Tests must cover: happy path, exclusion/inclusion logic, edge cases (NULLs, empty tables).
- Never change a golden value to make a test pass — a failing numeric assertion means
  the algorithm diverges from the legacy portal.

## Adding a new test file

1. Create `test_{function_name}.py` in this directory.
2. Use the `conn` fixture from `conftest.py` for in-memory DuckDB.
3. Seed the minimal table structure needed (e.g. `"{study_id}_mutations"`).
4. Call the repository function and assert on the result.

## Test file inventory

| File | Covers |
|---|---|
| `test_build_filter_subquery.py` | Filter subquery logic, INTERSECT behavior |
| `test_get_age_histogram.py` | Age binning, NA handling |
| `test_get_charts_meta.py` | Chart priority, pie/bar/table thresholds |
| `test_get_cna_genes.py` | CNA frequency, AMP/HOMDEL distinction |
| `test_get_mutated_genes.py` | Mutation frequency, VC exclusions, UNCALLED |
| `test_loader_fga.py` | FGA computation from SEG files |
| `test_loader_normalization.py` | Hugo normalization, TERT special case |
| `test_get_sv_genes.py` | SV frequency, neutral value exclusion |
| `test_get_km_data.py` | KM step function, censored events, empty cohort |
| `test_get_tmb_fga_scatter.py` | Scatter binning, correlation sign, empty data |
| `test_get_clinical_counts.py` | Reserved colors, NA handling, patient/sample level |
| `test_get_clinical_data_table.py` | Pagination, sort direction, search filter |
| `test_get_data_types_chart.py` | Data type labeling, profiled sample counts |
| `test_error_handling.py` | Malformed filter JSON, missing tables, NULL columns |
