# Study View Gaps Analysis

**Date:** 2026-03-19
**Methodology:** Side-by-side comparison of `localhost:8000` (acyc_fmi_2014, msk_chord_2024)
against `cbioportal.org`. JS console inspection of `DashboardState.chartsMeta`. Source
review of `web-review-worktree/src/cbioportal/`.

---

## Executive Summary

| # | Severity | Area | Description |
|---|----------|------|-------------|
| 1 | 🔴 Critical | Filters | ✅ CNA and SV gene filters silently ignored — clicking a gene has no effect |
| 2 | 🔴 Critical | Layout | ✅ Chart priority broken for studies without clinical meta — all charts at priority=1 |
| 3 | 🟠 High | CNA table | Cytoband column missing — excluded at load time, no gene→cytoband mapping |
| 4 | 🟠 High | Navigation | CN Segments tab always shown on every study, links to `#`, no route handler |
| 5 | 🟡 Medium | Charts | ✅ Pie charts missing center count label (e.g. "7.5K" in public portal) |
| 6 | 🟡 Medium | Study header | ✅ PubMed link not rendered despite `pmid` column being populated |
| 7 | 🟡 Medium | Clinical table | Column-header sort may not be wired to re-fetch |
| 8 | 🟡 Medium | Data Types | Checkboxes shown but non-functional |
| 12 | 🟡 Medium | Charts | ✅ Numeric (float) clinical attributes render as raw-value bar chart, not a binned histogram |
| 9 | 🟢 Low | Toolbar | ✅ "Study Page Help" link absent |
| 10 | 🟢 Low | Charts | "vs. Compare" button not implemented |
| 11 | 🟢 Low | Navigation | Plots Beta tab links to `#`, no content |

---

## Critical Bugs

### ✅ 1. CNA and SV Gene Filters Silently Ignored

**File:** `src/cbioportal/core/study_view/filters.py`, line 102
**File:** `src/cbioportal/web/schemas.py`, lines 43–51

#### What happens

When a user clicks a gene in the CNA Genes or SV Genes table, the JS pushes the gene
into the filter state and dispatches `cbio-filter-changed`. All chart endpoints are
re-fetched with an updated `filter_json`. The payload contains:

```json
{
  "clinicalDataFilters": [],
  "mutationFilter": { "genes": [] },
  "svFilter": { "genes": ["ERBB2"] }
}
```

The `DashboardFilters` Pydantic schema (`schemas.py:43–51`) accepts `svFilter` but has
**no `cnaFilter` field at all**:

```python
class DashboardFilters(BaseModel):
    clinicalDataFilters: list[ClinicalDataFilter] = []
    mutationFilter: MutationFilter = MutationFilter()
    svFilter: SvFilter = SvFilter()
    # cnaFilter: missing entirely
```

In `filters.py`, `_build_filter_subquery()` only reads `mutationFilter.genes`:

```python
mutation_filter_genes = f.get("mutationFilter", {}).get("genes", [])
# Line 104: only checks clinical_filters and mutation_filter_genes
if not clinical_filters and not mutation_filter_genes:
    return f'SELECT SAMPLE_ID FROM "{study_id}_sample"', []
```

`svFilter.genes` and any `cnaFilter.genes` are never read. The function short-circuits
before reaching gene-filter logic if only an SV/CNA filter is active (no clinical filters,
no mutation genes → returns all samples).

#### Impact

Clicking a gene in the CNA Genes or SV Genes table sends a filter request, but **all
charts continue to show the full unfiltered cohort**. A core interaction is silently
broken — no error, no loading spinner change, no visible feedback that filtering failed.

#### Fix outline

1. Add `cnaFilter: CnaFilter = CnaFilter()` to `DashboardFilters` in `schemas.py`.
2. In `filters.py`, after the mutation gene block, add parallel blocks for
   `cnaFilter.genes` (query `{study_id}_cna WHERE Hugo_Symbol = ?`) and
   `svFilter.genes` (query `{study_id}_sv WHERE Hugo_Symbol = ?`).
3. Add the cleared-filter JS line for `_cna_genes` alongside the existing
   `_sv_genes` clear in `study_view.js:176`.

---

### ✅ 2. Chart Priority Ordering Broken for Studies Without Clinical Meta

**File:** `src/cbioportal/core/study_view/meta.py`, lines 16–24 (`_PRIORITY_OVERRIDES`)

#### What happens

`get_charts_meta()` has two code paths:

- **Primary:** reads `clinical_attribute_meta` table, populated at load time from the
  `#Priority` header row in clinical data files.
- **Fallback:** when no meta rows exist, synthesises chart metadata from DuckDB column
  type introspection. Priority comes from `_PRIORITY_OVERRIDES`:

```python
_PRIORITY_OVERRIDES: dict[str, int] = {
    "CANCER_TYPE":          3000,
    "CANCER_TYPE_DETAILED": 2000,
    "GENDER":               9,
    "SEX":                  9,
    "AGE":                  9,
    "CURRENT_AGE_DEID":     9,
    "DIAGNOSIS_AGE":        9,
}
```

Any attribute not in this dict gets `priority = 1` (the default at `meta.py:202`).

The public portal's Java layer (`StudyViewUtils.tsx`) uses a much richer default
priority table:

| Attribute | Public portal default priority |
|-----------|-------------------------------|
| `OS_STATUS` | 1000 |
| `OS_MONTHS` | 990 |
| `SAMPLE_TYPE` | 950 |
| `RACE` | 900 |
| `AGE` | 800 |
| `GENDER` / `SEX` | 700 |
| `STAGE` (any) | 600 |
| `ETHNICITY` | 550 |
| `SMOKING` (any) | 500 |
| `MSI_TYPE` | 450 |

#### Observed evidence

In `acyc_fmi_2014`, the clinical data files have no `#Priority` metadata rows (common
for older/smaller studies). The fallback path runs. All charts except `CANCER_TYPE`
and `CANCER_TYPE_DETAILED` get `priority=1`. The public portal shows `SAMPLE_TYPE`,
`SEX`, and `AGE` prominently in the first two rows; our implementation shows them
buried alphabetically among all other priority=1 charts.

#### Impact

Studies without clinical file priority headers (older studies, community studies,
many FMI studies) display charts in wrong order. Key clinical charts like OS_STATUS,
SAMPLE_TYPE, and RACE appear at the bottom instead of prominently.

#### Fix outline

Expand `_PRIORITY_OVERRIDES` to match the Java `StudyViewUtils` defaults for common
clinical attributes. The dict already has the right shape — it just needs 15–20 more
entries covering survival, sample type, race, ethnicity, stage, MSI, smoking, etc.

---

## High Priority Gaps

### 3. CNA Genes Table: Missing Cytoband Column

**File:** `src/cbioportal/core/loader/__init__.py`, line 144

Public portal CNA Genes table columns: **Gene | Cytoband | CNA | # | Freq**
Our CNA Genes table columns: **Gene | CNA | # | Freq**

At CNA load time, the loader explicitly excludes `Cytoband` from the UNPIVOT operation:

```python
_NON_SAMPLE_COLS = {"Hugo_Symbol", "Entrez_Gene_Id", "Cytoband"}
```

`Cytoband` is treated as a header annotation, not a sample-level value, so it is never
stored in the `{study_id}_cna` long-format table. No other table provides a
gene→cytoband mapping (the `gene_reference` table stores Entrez and Hugo symbol but
not cytoband).

#### Fix outline

Two options:

1. **During CNA load:** store `(Hugo_Symbol, Cytoband)` pairs from the CNA file header
   into a small `gene_cytoband` table (or add a `cytoband` column to `gene_reference`).
2. **Join at query time:** `get_cna_genes()` in `genomic.py` would LEFT JOIN to the
   cytoband table to attach the column before returning results.

Option 1 is preferred: cytoband data is available at load time; re-deriving it at query
time is redundant work.

---

### 4. CN Segments Tab: Always Visible, Non-Functional

**File:** `src/cbioportal/web/templates/study_view/partials/study_navbar.html`, line 21

```html
<a href="#" class="study-tab">CN Segments</a>  <!-- hardcoded, no route, no condition -->
```

Problems:

1. **Always shown** — tab appears on every study regardless of whether segment data
   (`data_cna_hg19.seg`) was loaded. The `study_data_types` table already tracks
   `"segment"` as a data type and can be used as a condition.
2. **No route handler** — clicking navigates to `href="#"`, nothing happens. No server
   route exists for a CN Segments view.
3. **No IGV viewer integration** — the public portal embeds an IGV.js browser for the
   segment view. We have no such integration.

#### Fix outline

Short-term: add `{% if 'segment' in study_data_types %}` guard so the tab only appears
when segment data exists. Route to a stub 501 page until the feature is built.

Long-term: implement segment viewer using IGV.js (embed in the tab content area).

---

## Medium Priority Gaps

### ✅ 5. Pie Chart Center Count Labels

The public portal renders a total-count label in the center of each pie chart ring
(e.g., "7.5K" for large studies, "143" for small studies). Our ECharts pie
`setOption` calls do not include a `graphic` center-text element or `series[0].label`
center configuration.

**File:** `src/cbioportal/web/templates/study_view/study_view.js` — `updatePieWidget()`

#### Fix outline

Add a `graphic` overlay to the ECharts pie option:

```js
graphic: [{
  type: 'text',
  left: 'center', top: 'middle',
  style: { text: formatCount(total), fontSize: 14, fontWeight: 'bold' }
}]
```

`total` is the sum of all `count` values in the response data (or pass it explicitly
from the server as `response.total`).

---

### ✅ 6. Missing PubMed Link in Study Header

**File:** `src/cbioportal/web/templates/study_view/partials/study_navbar.html`

The `studies` table has `pmid` and `citation` columns populated at load time. The
`get_study_metadata()` function in `meta.py` already returns `pmid`:

```python
return {
    ...
    "pmid": pmid or "",
    ...
}
```

`study_navbar.html` renders `meta.description` but never renders `meta.pmid`.
The public portal shows a "PubMed" hyperlink next to the study description when
`pmid` is non-empty.

#### Fix outline

Add to `study_navbar.html` after the description:

```html
{% if meta.pmid %}
  <a href="https://www.ncbi.nlm.nih.gov/pubmed/{{ meta.pmid }}"
     target="_blank" class="study-pmid-link">PubMed</a>
{% endif %}
```

---

### 7. Clinical Data Table: Column Sort Not Wired

**File:** `src/cbioportal/web/templates/study_view/` (clinical data table template)

The `/study/clinicalData` endpoint accepts `sort_col` and `sort_dir` POST parameters
in the repository layer. The public portal allows clicking any column header to re-sort.
Our table headers appear to be static HTML — there is no JS click handler that re-POSTs
with `sort_col` and `sort_dir` populated.

This needs verification: load the clinical data tab, open the Network panel, and click
a column header to confirm whether a POST is sent with sort parameters.

---

### 8. Data Types Checkboxes: Non-Functional

The Data Types chart table renders checkboxes next to each data type row. These
checkboxes do not trigger any filter — `_build_filter_subquery()` has no handler for
data type filters, and `DashboardFilters` has no corresponding filter field.

The public portal's Data Types widget is purely informational (no filtering). Our
checkboxes imply interactivity that doesn't exist and may confuse users.

**Options:**
- Remove checkboxes from the Data Types table (matches public portal behavior).
- OR implement data-type filtering (select which molecular profiles are active).

---

### ✅ 12. Numeric Clinical Attributes Render as Raw-Value Bar Charts, Not Histograms

**File:** `src/cbioportal/web/templates/study_view/study_view.js` — `updateBarWidget()`, line 259
**File:** `src/cbioportal/core/study_view/clinical.py` — `get_clinical_counts()`, line 22

Any clinical attribute with `#Datatype: NUMBER` (e.g. PD-L1 score, TMB, tumour purity)
gets `chart_type: 'bar'` from `get_charts_meta()` and is routed to `updateBarWidget()`.
Inside that function, the `/chart/age` endpoint (which does proper 5-year binning) is
only called for a hardcoded allowlist:

```js
const AGE_COLS = new Set(['AGE', 'CURRENT_AGE_DEID', 'DIAGNOSIS_AGE', 'AGE_AT_DIAGNOSIS']);

if (AGE_COLS.has(attrId)) {
    // → /chart/age  (5-year bins)
} else {
    // → /chart/clinical  (raw GROUP BY, no binning)
    bins = (json.data || []).map(d => ({ x: d.value, y: d.count }));
}
```

For any `NUMBER` attribute not in `AGE_COLS`, the `else` branch calls `/chart/clinical`,
which runs a plain `GROUP BY val LIMIT 100` in `get_clinical_counts()`. For a float like
PD-L1 (0.0–100.0), this produces up to 100 bars of raw distinct values (`0.0`, `1.5`,
`2.3`, ...) — not binned ranges. The chart renders but is clinically unreadable.

#### Fix outline

`AGE_COLS` is a special case of a general "numeric binning" requirement. The fix has
two parts:

1. **Generalize `/chart/age` into `/chart/numeric`** — accept `attribute_id` and an
   optional `bin_size` param. Compute bin width automatically from the data range when
   `bin_size` is not supplied (e.g. `(max - min) / 20`, rounded to a clean interval).
   The age-specific 5-year logic becomes a special case of this.

2. **In `updateBarWidget`, always use the numeric endpoint** — drop the `AGE_COLS`
   check entirely:
   ```js
   formData.append('attribute_id', attrId);
   const response = await fetch('/study/summary/chart/numeric?format=json', { method: 'POST', body: formData });
   ```

The `/chart/age` route can remain as a backward-compatible alias that calls the same
underlying function with `bin_size=5`.

---

## Low Priority / Cosmetic

### ✅ 9. Missing "Study Page Help" Link

The public portal shows a "Study Page Help" link in the top-right toolbar area of the
study view dashboard. Not present in our implementation. Low priority — a docs link.

---

### 10. Missing "vs. Compare" Button

The public portal's group-valued table charts (e.g., Clinical Group, Cancer Type) show
a "vs. Compare" button that triggers survival comparison across selected groups. Not
implemented. This is a significant feature but depends on the KM plot infrastructure
already being in place.

---

### 11. Plots Beta Tab Is a Stub

`study_navbar.html:22`:
```html
<a href="#" class="study-tab">Plots <span class="beta-tag">Beta!</span></a>
```

Links to `href="#"`, no route handler. The public portal's Plots tab shows scatter plots
of any two clinical or genomic attributes. This is a known placeholder — tracked here
for completeness.

---

## API Design Assessment

### What Works Well

| Aspect | Assessment |
|--------|-----------|
| POST form-based pattern | Correct — avoids URL length limits with large `filter_json` |
| `charts-meta` GET endpoint | Clean separation of layout from data; single source of truth for chart ordering |
| Per-chart POST endpoints | Well-scoped, easy to parallelize on the client side |
| `INTERSECT` semantics in filter engine | Correct AND-across-attributes, OR-within-attribute behavior |
| Self-exclusion not needed (each chart queries full filter) | Correct design — no special-casing needed |
| `_build_filter_subquery` central engine | Right pattern; extending it for new filter types is straightforward |

### Issues and Recommendations

| Issue | File | Severity | Recommendation |
|-------|------|----------|----------------|
| `cnaFilter` not in schema or filter engine | `schemas.py`, `filters.py` | Bug | Add `cnaFilter` field; add CNA subquery block |
| `svFilter.genes` parsed but never applied | `filters.py:102` | Bug | Add SV subquery block alongside mutation block |
| `age` endpoint special-cased; `NUMBER` attrs outside `AGE_COLS` get raw GROUP BY, not bins | `study_view.js:259`, `study_view.py` | Bug | Generalize `/chart/age` → `/chart/numeric`; drop `AGE_COLS` special-case |
| `scatter` endpoint hardcoded to TMB/FGA axes | `study_view.py` | Design limit | Acceptable for now; parameterize if Plots tab is implemented |
| No segment viewer endpoint despite tab existing | `study_view.py` | Missing feature | Add stub 404 endpoint; gate tab on data type |
| `filter_json` schema has no version field | `schemas.py` | Forward-compat risk | Low priority; add `schema_version: int = 1` |
| `_data_types` filter payload accepted but ignored | `filters.py` | Inconsistency | Either filter by data type or drop the implication of interactivity |

---

## Full Chart Inventory: msk_chord_2024 (localhost:8000)

27 charts rendered by `GET /study/summary/charts-meta?id=msk_chord_2024`:

| Priority | Type | Attr ID | Display Name |
|----------|------|---------|--------------|
| 3000 | table | CANCER_TYPE | Cancer Type |
| 3000 | table | CLINICAL_GROUP | Clinical Group |
| 3000 | pie | CLINICAL_SUMMARY | Clinical Summary |
| 3000 | pie | DIAGNOSIS_DESCRIPTION | Diagnosis Description |
| 3000 | table | ICD_O_HISTOLOGY_DESCRIPTION | ICD-O Histology Description |
| 3000 | table | PATHOLOGICAL_GROUP | Pathological Group |
| 2000 | table | CANCER_TYPE_DETAILED | Cancer Type Detailed |
| 1000 | _data_types | _data_types | Data Types |
| 1000 | pie | OS_STATUS | Overall Survival Status |
| 990 | pie | SAMPLE_TYPE | Sample Type |
| 980 | pie | RACE | Race |
| 970 | pie | GENDER | Sex |
| 960 | pie | STAGE_HIGHEST_RECORDED | Stage (Highest Recorded) |
| 950 | pie | ETHNICITY | Ethnicity |
| 910 | pie | MSI_TYPE | MSI Type |
| 900 | pie | GENE_PANEL | Gene Panel |
| 880 | bar | CURRENT_AGE_DEID | Current Age |
| 870 | pie | SMOKING_PREDICTIONS_3_CLASSES | Smoking History (NLP) |
| 860 | pie | SOMATIC_STATUS | Somatic Status |
| 760 | pie | PRIOR_MED_TO_MSK | Prior Treatment to MSK (NLP) |
| 400 | _km | _km | KM Plot: Overall (months) |
| 90 | _mutated_genes | _mutated_genes | Mutated Genes |
| 80 | _cna_genes | _cna_genes | CNA Genes |
| 70 | _sv_genes | _sv_genes | Structural Variant Genes |
| 50 | _scatter | _scatter | Mutation Count vs FGA |
| 35 | _patient_treatments | _patient_treatments | Treatment per Patient |
| 30 | _sample_treatments | _sample_treatments | Treatment per Sample |

**Note:** `msk_chord_2024` has a full `clinical_attribute_meta` table (priorities come
from the actual clinical file headers), so the priority ordering is correct for this
study. The priority bug (gap #2) is only triggered for studies like `acyc_fmi_2014`
that lack the `#Priority` metadata row in their clinical files.

---

## Appendix: Key Source Locations

| Finding | File | Line(s) |
|---------|------|---------|
| `mutationFilter` only — CNA/SV ignored | `core/study_view/filters.py` | 102–163 |
| `DashboardFilters` missing `cnaFilter` | `web/schemas.py` | 43–51 |
| `_PRIORITY_OVERRIDES` sparse dict | `core/study_view/meta.py` | 16–24 |
| Fallback assigns `priority=1` default | `core/study_view/meta.py` | 202 |
| `Cytoband` excluded from CNA UNPIVOT | `core/loader/__init__.py` | 144 |
| `CN Segments` tab hardcoded + `href="#"` | `web/templates/study_view/partials/study_navbar.html` | 21 |
| `svFilter.genes` JS toggle (but no cnaFilter) | `web/templates/study_view/study_view.js` | 510–512 |
| `AGE_COLS` hardcoded; non-age `NUMBER` attrs skip binning | `web/templates/study_view/study_view.js` | 183, 259–271 |
