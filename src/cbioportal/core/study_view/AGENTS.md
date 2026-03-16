# core/study_view/

Query functions that power the Study View dashboard — clinical charts, genomic tables,
survival analysis, and scatter plots.

## Biology context

The Study View dashboard lets researchers explore a cohort by filtering on clinical
attributes (cancer type, sex, stage) and genomic events (mutations in TP53, CNA in
ERBB2, SV in BRCA1). Each filter narrows the sample set; all charts update to reflect
the filtered cohort. This is analogous to a clinical data warehouse faceted search.

Mutation/CNA/SV frequencies must be computed against the number of samples **profiled**
for each gene via a specific panel, not the total sample count. A gene not covered by a
panel appears in 0% of samples — omitting it from the denominator prevents artificially
low frequencies.

Survival analysis uses the Kaplan-Meier estimator, which handles right-censored
observations (patients still alive at last follow-up whose true survival time is unknown).

## Engineering context

- All functions take `(conn, study_id, filter_json)` — `filter_json` is a JSON string
  matching the `DashboardFilters` schema in `web/schemas.py`.
- `_build_filter_subquery()` in `filters.py` is the central filter engine — it converts
  the JSON filter state into a SQL subquery returning the filtered sample ID set.
- Gene panel availability is cached per `(study_id, filter_json)` call in `_get_panel_availability()`.
- Color assignment uses a deterministic hash so chart colors are stable across filter changes.
- `meta.py` contains all chart layout logic: priority ordering, pie/table/bar thresholds,
  genomic chart injection, and the 12-column bin-packer position algorithm.

## Key files

- `__init__.py` — Public API re-exports
- `filters.py` — `_build_filter_subquery()`, `get_clinical_attributes()` (DEEP DOCS)
- `colors.py` — `get_value_color()`, `CBIOPORTAL_D3_COLORS`, `RESERVED_COLORS`
- `clinical.py` — `get_clinical_counts()`, `get_all_clinical_counts()`, `get_clinical_data_table()`
- `genomic.py` — `get_mutated_genes()`, `get_cna_genes()`, `get_sv_genes()` (DEEP DOCS)
- `survival.py` — `get_km_data()`, `compute_km_curve()`, `get_tmb_fga_scatter()` (DEEP DOCS)
- `meta.py` — `get_study_metadata()`, `get_charts_meta()`, `get_data_types_chart()`

## When to cite legacy code

- Filter logic mirrors `StudyViewFilterUtil.java` and `StudyViewMyBatisRepository.java`.
- Frequency denominator (profiled samples per gene) mirrors `StudyViewUtils.tsx:getFrequencyStr()`.
- Chart priority values and layout algorithm mirror `StudyViewUtils.tsx:calculateLayout()`.
- KM estimator mirrors the Kaplan-Meier implementation in `SurvivalUtil.tsx`.
