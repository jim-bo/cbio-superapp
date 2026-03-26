# Plots Tab — Comprehensive Legacy Reference

> This document describes every aspect of the legacy cBioPortal Plots tab, with citations to exact source files and line numbers. Use it as a completeness checklist for the reimplementation.
>
> **Citation format:** `FileName.tsx:123` is relative to `cbioportal-frontend/src/shared/components/plots/` unless otherwise noted. Files outside that directory use longer paths.

---

## 1. Form Controls

### 1.1 Horizontal / Vertical Axis Panels

Each axis panel has identical controls. The vertical panel adds a "Same gene" option in the gene selector.

#### Data Type Dropdown
Options in display order (`PlotsTabUtils.tsx:119-129` — `dataTypeDisplayOrder`):

| # | Display Label | Internal Value | Source |
|---|--------------|----------------|--------|
| 1 | Clinical Attribute | `clinical_attribute` | `PlotsTabUtils.tsx:88` (`CLIN_ATTR_DATA_TYPE`) |
| 2 | Custom Data | `custom_attribute` | `PlotsTabUtils.tsx:89` (`CUSTOM_ATTR_DATA_TYPE`) |
| 3 | Mutation | `MUTATION_EXTENDED` | |
| 4 | Structural Variant | `STRUCTURAL_VARIANT` | |
| 5 | Copy Number | `COPY_NUMBER_ALTERATION` | |
| 6 | mRNA Expression | `MRNA_EXPRESSION` | |
| 7 | Gene Sets | `GENESET_SCORE` | `PlotsTabUtils.tsx:90` |
| 8 | Protein Level | `PROTEIN_LEVEL` | |
| 9 | DNA Methylation | `METHYLATION` | |
| 10+ | Generic Assay types | dynamic | Configured per-study |
| — | Ordered samples | `none` | Only shown when other axis is LIMIT_VALUE generic assay |

Display name mapping: `PlotsTabUtils.tsx:91-101` (`dataTypeToDisplayType`)

**Dropdown styling:** width 240px (`styles.scss:17`)

#### Gene Selector
- Shown when data type requires a gene (mutation, SV, CNA, mRNA, protein, methylation)
- AsyncSelect component, lists queried genes in OQL order
- Vertical axis includes "Same gene (GENEX)" option when horizontal has a gene selected
- Width: 350px (`styles.scss:158`)

#### Data Source (Profile) Selector
- Shown for molecular profile data types
- Populated from `dataTypeToDataSourceOptions` (`PlotsTab.tsx:2748-2800`)
- For CNA: DISCRETE profiles sort first

#### Mutation Count By
Shown when data type is `MUTATION_EXTENDED` (`PlotsTab.tsx:219-224`):

| Value | Label |
|-------|-------|
| `MutationType` | Mutation Type |
| `MutatedVsWildType` | Mutated vs Wild-type |
| `DriverVsVUS` | Driver vs VUS |
| `VariantAlleleFrequency` | Variant Allele Frequency |

#### Structural Variant Count By
Shown when data type is `STRUCTURAL_VARIANT` (`PlotsTab.tsx:226-229`):

| Value | Label |
|-------|-------|
| `MutatedVsWildType` | Variant vs No Variant |
| `VariantType` | Variant Type |

`VariantType` is excluded if the other axis is a clinical attribute.

#### Log Scale Checkbox
- Shown when: numeric data with no negative values, OR profile ID matches `/rna_seq/i` and not `/zscore/i` (`PlotsTab.tsx:65` — `maybeSetLogScale` import)
- Auto-enabled for RNA-seq profiles

### 1.2 Swap Axes Button
- Centered between axis panels (`styles.scss:32-35`)
- Transposes horizontal ↔ vertical configurations

### 1.3 Coloring Menu
Rendered above the chart, labeled "Color samples by:" (`PlotsTab.tsx:5961-5962`)

**Not shown** for DiscreteVsDiscrete plots.

| Control | Scatter/Box | Waterfall | Source |
|---------|------------|-----------|--------|
| Gene/Clinical omnibar | Search dropdown (grouped: "Genes" then "Clinical Attributes") | Same | |
| Mutation Type | Checkbox (can combine) | Radio (mutually exclusive) | |
| Copy Number | Checkbox | Radio | |
| Structural Variant | Checkbox | Radio | |
| Log Scale | Shown for numerical clinical attribute | Same | |

**CSS:** margin-bottom -20px, margin-right 50px (`styles.scss:102-104`). Checkbox margin: `0 8px 1px 0` (`styles.scss:121`). Radio margin: `0px 8px 0px 2px` (`styles.scss:127`).

### 1.4 Plot Options Bar

#### Discrete Plot Type Selector
Only shown for DiscreteVsDiscrete plots (`PlotsTab.tsx:207-212`, options at `PlotsTab.tsx:434-445`):

| Value | Label |
|-------|-------|
| `Bar` | Bar chart |
| `StackedBar` | Stacked bar chart |
| `PercentageStackedBar` | 100% stacked bar chart |
| `Table` | Table |

#### Sort Order
`PlotsTab.tsx:214-217` (`SortByOptions`):

| Value | Label | When Shown |
|-------|-------|------------|
| `Alphabetically` | Alphabetically | Always |
| `SortByTotalSum` | Number of samples | Bar charts |
| `SortByMedian` | Sort by median | Box plots |

Additionally, each minor category can be chosen as a sort target (dynamic).

#### Other Toggles

| Toggle | When Shown | Source |
|--------|-----------|--------|
| Horizontal bars | DiscreteVsDiscrete only | |
| Regression line | ScatterPlot only | |
| View limit values | Generic assay (LIMIT_VALUE) only | |
| Connect samples | Box plot, when patients have multiple samples | |

### 1.5 Quick Plots Pills
Conditional example links (`PlotsTab.tsx:6378-6380`):

| Label | Configuration | Condition |
|-------|--------------|-----------|
| Mut# vs Dx | MUTATION_COUNT vs CANCER_TYPE_DETAILED | >1 and <16 cancer types |
| CNA vs Exp | CNA vs mRNA for same gene | Both profiles available |
| Mut vs Exp | Mutation vs mRNA for same gene | Both profiles available |
| Methylation vs Exp | Methylation vs mRNA | Both profiles available |

### 1.6 Left Column Layout
- Width: 304px (`styles.scss:12`)
- Axis block: background `#eee`, border-radius 4px, padding 10px (`styles.scss:37-41`)
- Axis title (h4): vertical text, writing-mode vertical-lr, rotated 180°, width 23px (`styles.scss:46-55`)
- Label text: color `#333333`, margin-bottom 5px (`styles.scss:58-62`)
- Form group spacing: margin-bottom 5px (`styles.scss:64-66`)

> **Verification (2026-03-26):** Section 1 reviewed against our implementation in
> `templates/results_view/page.html` (lines 695–2482) and `plots_repository.py`.
> All form controls for implemented data types are present and correct:
> - H/V axis panels with Data Type dropdown (4 implemented types), Gene selector, CNA profile selector
> - Mutation "Plot by" (mutated_vs_wildtype, type) and SV "Plot by" (variant_vs_no_variant) match backend
> - Swap Axes button works correctly
> - Plot Options: stacked/grouped/pct bar types, alpha/count sort, horizontal bars toggle
> - Quick Plots pills adapted for our available data (Mut# vs Dx, FGA vs Dx, Mut# vs FGA)
>
> Known gaps already tracked in Section 12: DriverVsVUS, VAF, VariantType count-by modes;
> log scale checkbox; coloring menu; Table plot type; regression line / connect samples toggles;
> "Same gene" option in vertical gene selector. No action needed — these depend on unimplemented data types or features.

---

## 2. Plot Type Determination

Decision logic at `PlotsTab.tsx:4846-4876` (`plotType` computed property):

| Horizontal Axis | Vertical Axis | Plot Type |
|----------------|--------------|-----------|
| string | string | **DiscreteVsDiscrete** |
| number | number | **ScatterPlot** |
| string | number | **BoxPlot** |
| number | string | **BoxPlot** |
| None | any | **WaterfallPlot** |
| any | None | **WaterfallPlot** |

**Enum definition:** `PlotsTab.tsx:200-212`
```typescript
enum PlotType { ScatterPlot, WaterfallPlot, BoxPlot, DiscreteVsDiscrete }
```

**Category limit:** `DISCRETE_CATEGORY_LIMIT = 150` — if a discrete variable has more categories, chart is not rendered and "too many categories" message shown (`PlotsTab.tsx:5612-5625`).

---

## 3. Data Flow Per Data Type

All axis data flows through `makeAxisDataPromise()` (`PlotsTabUtils.tsx:1493-1627`).

### 3.1 Clinical Attribute
- **Handler:** `makeAxisDataPromise_Clinical` (`PlotsTabUtils.tsx:928-977`)
- **API:** `clinicalDataCache` → `/api/clinical-data/fetch`
- **Transform:**
  - Patient attributes expanded to all patient's samples (lines 946-957)
  - Sample attributes used directly (lines 959-965)
  - Numeric datatype: values parsed with `parseFloat()` (lines 967-970)
  - String datatype: values kept as strings
- **Our impl:** `_get_clinical_axis()` in `plots_repository.py` — **IMPLEMENTED**

### 3.2 Mutation (MUTATION_EXTENDED)
- **Handler:** `makeAxisDataPromise_Molecular` → `makeAxisDataPromise_Molecular_MakeMutationData` (`PlotsTabUtils.tsx:1145-1291`)
- **API:** `annotatedMutationCache` → `/api/molecular-profiles/{id}/mutations`
- **Profiling:** `isSampleProfiledInMultiple()` checks gene panel coverage (line 1171)
- **Count-by modes:**

  **MutationType** (lines 1181-1200):
  - Maps each mutation via `getOncoprintMutationType(m)` → `mutationTypeToDisplayName[...]`
  - Multiple types → `"Multiple"`
  - No mutations + profiled → `"No mutation"`
  - No mutations + not profiled → `"Not profiled"`
  - Category order: `mutTypeCategoryOrder` (`PlotsTabUtils.tsx:2087-2097`)

  **MutatedVsWildType** (lines 1237-1246):
  - Has mutations → `"Mutated"`
  - No mutations + profiled → `"No mutation"`
  - Category order: `mutVsWildCategoryOrder` (`PlotsTabUtils.tsx:2098-2102`)

  **DriverVsVUS** (lines 1202-1212):
  - Any `putativeDriver === true` → `"Driver"`, else `"VUS"`
  - Category order: `mutDriverVsVUSCategoryOrder` (`PlotsTabUtils.tsx:2103-2108`)

  **VariantAlleleFrequency** (lines 1214-1235):
  - Numeric: `tumorAltCount / (tumorAltCount + tumorRefCount)`
  - Filters out non-mutated and not-profiled samples
  - Returns empty if no mutations have count data (lines 1259-1265)

- **Our impl:** MutationType and MutatedVsWildType — **IMPLEMENTED**. DriverVsVUS and VAF — **NOT IMPLEMENTED**

### 3.3 Copy Number Alteration (Discrete CNA)
- **Handler:** `makeAxisDataPromise_Molecular` (lines 1103-1139 in `PlotsTabUtils.tsx`)
- **API:** `numericGeneMolecularDataCache` → `/api/molecular-profiles/{id}/molecular-data`
- **Detection:** Discrete when `profile.molecularAlterationType === 'COPY_NUMBER_ALTERATION' && profile.datatype === 'DISCRETE'` (lines 1103-1109)
- **Transform:** Maps integer values to labels via `cnaToAppearance[value].legendLabel` (lines 1123-1128):
  - `-2` → `"Deep Deletion"`
  - `-1` → `"Shallow Deletion"`
  - `0` → `"Diploid"`
  - `1` → `"Gain"`
  - `2` → `"Amplification"`
- **Category order:** `cnaCategoryOrder` (`PlotsTabUtils.tsx:2070-2072`) = Deep Deletion, Shallow Deletion, Diploid, Gain, Amplification
- **Our impl:** **IMPLEMENTED** (integer-only filtering, profiled-only semantics)

### 3.4 Structural Variant
- **Handler:** `makeAxisDataPromise_Molecular_MakeStructuralVariantData` (`PlotsTabUtils.tsx:1292-1356`)
- **API:** `structuralVariantCache` → `/api/structural-variant/fetch`
- **Count-by modes:**

  **MutatedVsWildType** (lines 1340-1342):
  - Has variants → `"With Structural Variants"`
  - No variants + profiled → `"No Structural Variants"`
  - Not profiled → `"Not profiled for structural variants"`

  **VariantType** (lines 1332-1338):
  - One class → variant class name
  - Multiple → `"Multiple structural variants"`

- **Our impl:** MutatedVsWildType — **IMPLEMENTED**. VariantType — **NOT IMPLEMENTED**

### 3.5 mRNA Expression / Protein Level / DNA Methylation
- **Handler:** `makeAxisDataPromise_Molecular` (lines 1034-1044 in `PlotsTabUtils.tsx`)
- **API:** `numericGeneMolecularDataCache` → `/api/molecular-profiles/{id}/molecular-data`
- **Transform:** Raw numeric values, always returns `number` datatype
- **Our impl:** **NOT IMPLEMENTED** (no continuous molecular data tables yet)

### 3.6 Gene Sets
- **Handler:** `makeAxisDataPromise_Geneset` (`PlotsTabUtils.tsx:1358-1410`)
- **API:** `genesetMolecularDataCache` → `/api/molecular-profiles/{id}/geneset-molecular-data`
- **Transform:** Numeric conversion of enrichment scores
- **Our impl:** **NOT IMPLEMENTED** (low priority)

### 3.7 Generic Assay
- **Handler:** `makeAxisDataPromise_GenericAssay` (`PlotsTabUtils.tsx:1412-1491`)
- **API:** `genericAssayMolecularDataCache` → `/api/generic-assay-data/fetch`
- **Datatype:** LIMIT_VALUE → numeric; CATEGORICAL/BINARY → string
- **Special:** Limit values have threshold types (`>`, `<`) enabling waterfall plots
- **Our impl:** **NOT IMPLEMENTED** (low priority)

---

## 4. Color Schemes

### 4.1 Mutation Type Colors — Default (no driver annotations)
Source: `AlterationColors.ts` (full path: `packages/cbioportal-frontend-commons/src/lib/AlterationColors.ts`)

Appearance map: `PlotsTabUtils.tsx:1909-1960` (`oncoprintMutationTypeToAppearanceDefault`)

| Type | Constant | Hex | Symbol | Legend Label |
|------|----------|-----|--------|-------------|
| Missense | `MUT_COLOR_MISSENSE` | `#008000` | circle | Missense |
| Inframe | `MUT_COLOR_INFRAME` | `#993404` | circle | Inframe |
| Truncating | `MUT_COLOR_TRUNC` | `#000000` | circle | Truncating |
| Splice | `MUT_COLOR_SPLICE` | `#e5802b` | circle | Splice |
| Promoter | `MUT_COLOR_PROMOTER` | `#00B7CE` | circle | Promoter |
| Other | `MUT_COLOR_OTHER` | `#cf58bc` | circle | Other |
| Fusion | `STRUCTURAL_VARIANT_COLOR` | `#8B00C9` | circle | Fusion |

All mutation types use stroke `#000000`, strokeOpacity `0.5` (`NON_CNA_STROKE_OPACITY`, `PlotsTabUtils.tsx:1805`)

### 4.2 Mutation Type Colors — Driver-Annotated
Appearance map: `PlotsTabUtils.tsx:1815-1907` (`oncoprintMutationTypeToAppearanceDrivers`)

| Type | Constant | Hex | Legend Label |
|------|----------|-----|-------------|
| Missense (Driver) | `MUT_COLOR_MISSENSE` | `#008000` | Missense (Driver) |
| Missense (VUS) | `MUT_COLOR_MISSENSE_PASSENGER` | `#53D400` | Missense (VUS) |
| Inframe (Driver) | `MUT_COLOR_INFRAME` | `#993404` | Inframe (Driver) |
| Inframe (VUS) | `MUT_COLOR_INFRAME_PASSENGER` | `#a68028` | Inframe (VUS) |
| Truncating (Driver) | `MUT_COLOR_TRUNC` | `#000000` | Truncating (Driver) |
| Truncating (VUS) | `MUT_COLOR_TRUNC_PASSENGER` | `#708090` | Truncating (VUS) |
| Splice (Driver) | `MUT_COLOR_SPLICE` | `#e5802b` | Splice (Driver) |
| Splice (VUS) | `MUT_COLOR_SPLICE_PASSENGER` | `#f0b87b` | Splice (VUS) |
| Promoter (Driver) | `MUT_COLOR_PROMOTER` | `#00B7CE` | Promoter (Driver) |
| Promoter (VUS) | `MUT_COLOR_PROMOTER_PASSENGER` | `#8cedf9` | Promoter (VUS) |
| Other (Driver) | `MUT_COLOR_OTHER` | `#cf58bc` | Other (Driver) |
| Other (VUS) | `MUT_COLOR_OTHER_PASSENGER` | `#f96ae3` | Other (VUS) |

### 4.3 CNA Overlay Colors (stroke on scatter/box plots)
Source: `PlotsTabUtils.tsx:2020-2046` (`cnaToAppearance`)

| CNA Value | Label | Stroke Hex | Constant | strokeOpacity |
|-----------|-------|-----------|----------|---------------|
| -2 | Deep Deletion | `#0000ff` | `CNA_COLOR_HOMDEL` (`AlterationColors.ts:27`) | 1 |
| -1 | Shallow Deletion | `#2aced4` | hardcoded (`PlotsTabUtils.tsx:2028`) | 1 |
| 0 | Diploid | `#BEBEBE` | `DEFAULT_GREY` (`Colors.ts:19`) | 1 |
| 1 | Gain | `#ff8c9f` | hardcoded (`PlotsTabUtils.tsx:2038`) | 1 |
| 2 | Amplification | `#ff0000` | `CNA_COLOR_AMP` (`AlterationColors.ts:23`) | 1 |

`CNA_STROKE_WIDTH = 1.8` (`PlotsTabUtils.tsx:149`)

### 4.4 CNA Bar Chart Colors
Same hex values as Section 4.3, used as **fill** instead of stroke in DiscreteVsDiscrete bar charts.

### 4.5 Structural Variant Overlay
Source: `PlotsTabUtils.tsx:2048-2052`
- Stroke: `#8B00C9` (`STRUCTURAL_VARIANT_COLOR`)
- strokeOpacity: 1
- Legend label: `"Structural Variant ¹"` (with superscript 1)

### 4.6 Clinical Reserved Colors
Source: `Colors.ts:39-129` (full path: `src/shared/lib/Colors.ts`)

| Values | Constant | Hex |
|--------|----------|-----|
| true, yes, positive, alive, living, disease free, tumor free, not progressed | `CLI_YES_COLOR` | `#1b9e77` |
| false, no, negative, deceased, recurred, progressed, recurred/progressed, with tumor | `CLI_NO_COLOR` | `#d95f02` |
| female, f | `CLI_FEMALE_COLOR` | `#E0699E` |
| male, m | `CLI_MALE_COLOR` | `#2986E2` |
| unknown, na | `LIGHT_GREY` | `#D3D3D3` |
| other | `DARK_GREY` | `#A9A9A9` |

Survival data mappings (`Colors.ts:90-118`):
- `"1:deceased"`, `"1:recurred/progressed"`, etc. → `CLI_NO_COLOR` (`#d95f02`)
- `"0:living"`, `"0:alive"`, `"0:diseasefree"`, etc. → `CLI_YES_COLOR` (`#1b9e77`)

Alteration-as-clinical mappings (`Colors.ts:120-128`):
- `"wild type"`, `"no mutation"`, `"diploid"`, `"unchanged"` → `DEFAULT_GREY` (`#BEBEBE`)
- `"amplification"` → `CNA_COLOR_AMP` (`#ff0000`)
- `"gain"` → `CNA_COLOR_GAIN` (`#ffb6c1`)
- `"shallow deletion"`, `"loss"` → `CNA_COLOR_HETLOSS` (`#8fd8d8`)
- `"deep deletion"` → `CNA_COLOR_HOMDEL` (`#0000ff`)

**Matching:** Case-insensitive, space-normalized via `getClinicalValueColor()` (`Colors.ts:152-156`). Also expands to TitleCase, UPPERCASE, and no-space variants (`Colors.ts:131-150`).

### 4.7 D3 Categorical Palette (for bar charts and auto-coloring)
Source: `PlotUtils.ts:221-253` (`makeUniqueColorGetter`)

31 colors:
```
#3366cc  #dc3912  #ff9900  #109618  #990099  #0099c6  #dd4477  #66aa00
#b82e2e  #316395  #994499  #22aa99  #aaaa11  #6633cc  #e67300  #8b0707
#651067  #329262  #5574a6  #3b3eac  #b77322  #16d620  #b91383  #f4359e
#9c5935  #a9c413  #2a778d  #668d1c  #bea413  #0c5922  #743411
```

When exhausted: darken each hex channel by `* 0.95` (`PlotUtils.ts:199-217` — `darkenHexColor`). Already-used colors (from `categoryToColor` props) are skipped.

### 4.8 Default Point Appearance (scatter/box)
Source: `PlotsTabUtils.tsx:1799-1803` (`basicAppearance`)
- fill: `#00AAF8`
- stroke: `#0089C6`
- strokeOpacity: 1

`NON_CNA_STROKE_OPACITY = 0.5` (`PlotsTabUtils.tsx:1805`) — used for all non-CNA overlays (mutations, SV, clinical)

### 4.9 Special Appearances

| Appearance | fill | stroke | strokeOpacity | Other | Source |
|-----------|------|--------|---------------|-------|--------|
| Not mutated | `#c4e5f5` | `#000000` | 0.3 | legendLabel: "Not mutated" | `PlotsTabUtils.tsx:2012-2018` |
| Not profiled (mutations) | `#ffffff` | `#D3D3D3` (LIGHT_GREY) | 1 | | `PlotsTabUtils.tsx:1967-1972` |
| Not profiled (CNA/SV) | — | `#000000` (BLACK) | 1 | | `PlotsTabUtils.tsx:1962-1966` |
| No data (clinical) | `#D3D3D3` | `#D3D3D3` | 1 | | `PlotsTabUtils.tsx:1973-1977` |
| Limit value | — | — | — | symbol: diamond | `PlotsTabUtils.tsx:2054-2057` |
| Waterfall search | white | red | 1 | symbol: plus, strokeWidth: 1, size: 3 | `PlotsTabUtils.tsx:2059-2068` |

---

## 5. Typography & Theme

Source: `cBioPortalTheme.ts` (full path: `packages/cbioportal-frontend-commons/src/theme/cBioPortalTheme.ts`)

### Font Family
`'Arial, Helvetica'` — used everywhere (`cBioPortalTheme.ts:27`)

### Font Sizes

| Element | Size | Padding | Other | Source Line |
|---------|------|---------|-------|------------|
| Base labels | 13px | 8 | stroke transparent, strokeWidth 0 | `cBioPortalTheme.ts:29,33,43-50` |
| Axis labels | 13px | 8 | centered | `cBioPortalTheme.ts:54-57` |
| Axis tick labels | 13px | 2 | fill: black | `cBioPortalTheme.ts:58-61` |
| Legend labels | 13px | 8 | | `cBioPortalTheme.ts:63` |
| Legend title | 13px | 5 | | `cBioPortalTheme.ts:282` |

### Axis Styling

| Element | Style | Source Line |
|---------|-------|------------|
| Axis line | stroke black, strokeWidth 1 | `cBioPortalTheme.ts:88-89` |
| Grid | stroke `#ECEFF1`, dashed `10, 5`, opacity 0 at origin | `cBioPortalTheme.ts:95-99` |
| Ticks | size 4, stroke black, strokeWidth 1 | `cBioPortalTheme.ts:104-108` |

### Legend Styling

| Property | Value | Source Line |
|----------|-------|------------|
| Symbol type | circle | `cBioPortalTheme.ts:276` |
| Symbol size | 3 | `cBioPortalTheme.ts:277` |
| Symbol strokeWidth | 1 | `cBioPortalTheme.ts:278` |
| Symbol stroke | black | `cBioPortalTheme.ts:279` |
| Gutter | 10 | `cBioPortalTheme.ts:271` |
| Orientation | vertical | `cBioPortalTheme.ts:272` |
| Title orientation | top | `cBioPortalTheme.ts:273` |

**Bar chart legend** uses different symbols (`MultipleCategoryBarPlot.tsx:230-238`):
- Symbol type: **square**
- Symbol size: **5**
- strokeOpacity: **0** (no stroke)

### Tooltip Styling
- Background: `#f0f0f0`, stroke `#212121`, strokeWidth 1, cornerRadius 5 (`cBioPortalTheme.ts:233-246`)

---

## 6. Sizing Constants

### Plot Area Dimensions

| Constant | Value | Source |
|----------|-------|--------|
| `PLOT_SIDELENGTH` | 650 | `PlotsTabUtils.tsx:150` |
| `WATERFALLPLOT_SIDELENGTH` | 500 | `PlotsTabUtils.tsx:151` |
| `WATERFALLPLOT_BASE_SIDELENGTH` | 480 | `PlotsTabUtils.tsx:152` |
| `WATERFALLPLOT_SIDELENGTH_SAMPLE_MULTIPLICATION_FACTOR` | 1.6 | `PlotsTabUtils.tsx:153` |

### Bar Chart Constants

| Constant | Value | Source |
|----------|-------|--------|
| `PLOT_DATA_PADDING_PIXELS` | 100 | `MultipleCategoryBarPlot.tsx:76` |
| `RIGHT_GUTTER` | 120 | `MultipleCategoryBarPlot.tsx:74` |
| `NUM_AXIS_TICKS` | 8 | `MultipleCategoryBarPlot.tsx:75` |
| `CATEGORY_LABEL_HORZ_ANGLE` | 50° | `MultipleCategoryBarPlot.tsx:77` |
| `DEFAULT_LEFT_PADDING` | 25 | `MultipleCategoryBarPlot.tsx:78` |
| `DEFAULT_BOTTOM_PADDING` | 10 | `MultipleCategoryBarPlot.tsx:79` |
| `LEGEND_ITEMS_PER_ROW` | 4 | `MultipleCategoryBarPlot.tsx:80` |
| `BOTTOM_LEGEND_PADDING` | 15 | `MultipleCategoryBarPlot.tsx:81` |
| `RIGHT_PADDING_FOR_LONG_LABELS` | 50 | `MultipleCategoryBarPlot.tsx:82` |

### Scatter Plot Constants

| Constant | Value | Source |
|----------|-------|--------|
| `PLOT_DATA_PADDING_PIXELS` | 50 | `ScatterPlot.tsx:98` |
| Chart width/height | 650 (= `PLOT_SIDELENGTH`) | `PlotsTab.tsx:5727-5728` |

### Box Plot Constants

| Constant | Value | Source |
|----------|-------|--------|
| `PLOT_DATA_PADDING_PIXELS` | 100 | `BoxScatterPlot.tsx:127` |
| `chartBase` | 550 | `BoxScatterPlot.tsx` prop |
| Box width | max 80, min 18 | `getBoxWidth()` calculation |
| `domainPadding` | 50 | `BoxScatterPlot.tsx` default |

### CSS Layout

| Property | Value | Source |
|----------|-------|--------|
| Left column width | 304px | `styles.scss:12` |
| Dropdown (Select) width | 240px | `styles.scss:17` |
| Gene select width | 350px | `styles.scss:158` |
| Axis block background | `#eee` | `styles.scss:38` |
| Axis block border-radius | 4px | `styles.scss:39` |
| Axis block padding | 10px | `styles.scss:40` |
| Data availability alert bg | `#eee` | `styles.scss:187` |

### Point Sizes (Scatter)
Source: `PlotUtils.ts:182-197` (`scatterPlotSize`)

| State | Radius |
|-------|--------|
| Default | 4 |
| Active (hovered) | 6 |
| Line highlighted | 7 |
| Legend highlighted | 8 |

When no highlight function provided (`PlotUtils.ts:172-178`): default 3, active 6.

---

## 7. Bar Chart Sizing Formulas

Source: `MultipleCategoryBarPlot.tsx:388-520`

### Bar Width
`MultipleCategoryBarPlot.tsx:422-429`
```
barWidth = props.barWidth    (default: 20, passed from PlotsTab.tsx:5690)
if grouped AND >10 minor categories:
    barWidth = barWidth / 2   (= 10)
```

### Bar Separation
`MultipleCategoryBarPlot.tsx:418-420`
```
barSeparation = stacked ? 0.2 * barWidth : 0
```

### Category Coordinate
`MultipleCategoryBarPlot.tsx:751-752`
```
categoryCoord(index) = index * (barWidth + barSeparation)
```

### Chart Extent (bar-axis dimension)
`MultipleCategoryBarPlot.tsx:388-408`
```
miscPadding = 100

if stacked:
    numBars = majorCategories.length    (= data[0].counts.length)
else (grouped):
    numBars = majorCategories.length * minorCategories.length
    miscPadding += categoryCoord(majorCategories.length)

chartExtent = categoryCoord(numBars - 1) + 2 * domainPadding + miscPadding
```

Where `domainPadding` defaults to `PLOT_DATA_PADDING_PIXELS` (100) (`MultipleCategoryBarPlot.tsx:346-352`).

### Chart Width & Height
`MultipleCategoryBarPlot.tsx:161-195`
```
if horizontalBars:
    chartWidth  = chartBase (= PLOT_SIDELENGTH = 650)
    chartHeight = chartExtent
else (vertical bars):
    chartWidth  = chartExtent
    chartHeight = chartBase (= PLOT_SIDELENGTH = 650)

// Ensure room for axis labels:
chartWidth  = max(chartWidth,  textWidth(axisLabelX))
chartHeight = max(chartHeight, textWidth(axisLabelY))
```

### SVG Dimensions
`MultipleCategoryBarPlot.tsx:410-416`
```
svgWidth  = leftPadding + chartWidth  + rightPadding
svgHeight = topPadding  + chartHeight + bottomPadding
```

### Legend Location
`MultipleCategoryBarPlot.tsx:201-211`
```
if chartWidth > legendLocationWidthThreshold (550)   OR   legendData.length > 15:
    legend on BOTTOM
else:
    legend on RIGHT (at x = chartWidth - 20, y = 100)
```

`legendLocationWidthThreshold` passed as 550 from `PlotsTab.tsx:5695-5696`.

### Bottom Legend Height
`MultipleCategoryBarPlot.tsx:213-223`
```
bottomLegendHeight = 23.7 * ceil(legendData.length / LEGEND_ITEMS_PER_ROW)
```

### Additional Padding (grouped bars)
`MultipleCategoryBarPlot.tsx:380-386`
```
if stacked: additionalPadding = 0
else: additionalPadding = categoryCoord(minorCategories.length / 2) + minorCategories.length
```

### Zero-Count Offset (grouped bars only)
`MultipleCategoryBarPlot.tsx:485-504`
```
if any count === 0 AND not stacked:
    zeroCountOffset = 0.01 * (maxMajorCount / numberOfTicks)
else:
    zeroCountOffset = 0
```

### Grouped Bar Axis Style
`MultipleCategoryBarPlot.tsx:506-510` — When not stacked, axis stroke is `#b3b3b3` instead of black.

---

## 8. Label Generation

### Axis Labels
Source: `PlotsTabUtils.tsx:1637-1757` (`getAxisLabel`)

| Data Type | Mode | Label Format |
|-----------|------|-------------|
| Clinical attribute | — | `attribute.displayName` |
| Mutation | MutationType | `"GENE: profile.name"` |
| Mutation | MutatedVsWildType | `"GENE: Mutated vs Wild Type"` |
| Mutation | DriverVsVUS | `"GENE: Driver vs VUS Mutations"` |
| Mutation | VAF | `"GENE: Variant Allele Frequency"` |
| SV | MutatedVsWildType | `"GENE: Variant vs No Variant"` |
| SV | VariantType | `"GENE: Variant Type"` |
| CNA (discrete) | — | `"GENE: profile.name"` |
| mRNA / protein / methylation | — | `"GENE: profile.name"` |
| Gene set | — | `"genesetId: profile.name"` |
| Generic assay | — | `"entityName: profile.name"` |
| + log scale | — | append ` (log2(value + 1))` or ` (log10)` |

### Axis Description (tooltip)
Source: `PlotsTabUtils.tsx:1759+` (`getAxisDescription`) — returns profile description or attribute description

### Bar Chart Count Axis Label
Default: `"# samples"` (`MultipleCategoryBarPlot.tsx:94-96`)

### Bar Chart Category Label Angle
Vertical bars: 50° rotation (`MultipleCategoryBarPlot.tsx:77`)

---

## 9. Category Ordering

### CNA Discrete Categories
Source: `PlotsTabUtils.tsx:2070-2072` (`cnaCategoryOrder`)
```
Deep Deletion, Shallow Deletion, Diploid, Gain, Amplification
```

### Mutation Type Categories
Source: `PlotsTabUtils.tsx:2087-2097` (`mutTypeCategoryOrder`)
```
Missense, Inframe, Truncating, Splice, Promoter, Other, Multiple, No mutation, Not profiled
```

### Mutation MutatedVsWildType Categories
Source: `PlotsTabUtils.tsx:2098-2102` (`mutVsWildCategoryOrder`)
```
Mutated, No mutation, Not profiled
```

### Mutation DriverVsVUS Categories
Source: `PlotsTabUtils.tsx:2103-2108` (`mutDriverVsVUSCategoryOrder`)
```
Driver, VUS, No mutation, Not profiled
```

### SV MutatedVsWildType Categories
Source: `PlotsTabUtils.tsx:2079-2086`
```
With Structural Variants, No Structural Variants, Not profiled for structural variants
```

### Mutation Legend Render Order (for overlays on scatter/box)
Source: `PlotsTabUtils.tsx:1979-1993` (`mutationLegendOrder`)
```
fusion, promoter.driver, promoter, splice.driver, splice, trunc.driver, trunc,
inframe.driver, inframe, missense.driver, missense, other.driver, other
```

### Mutation Render Priority (z-index for overlapping points)
Source: `PlotsTabUtils.tsx:1994-2010` (`mutationRenderPriority`)
```
fusion, promoter.driver, splice.driver, trunc.driver, inframe.driver, missense.driver,
other.driver, promoter, splice, trunc, inframe, missense, other, not_mutated, not_profiled
```

### CNA Render Priority (z-index)
```
-2, 2, -1, 1, 0, not_profiled
```
(Highest priority = drawn on top)

### Bar Chart Major Category Sort
- Default: alphabetical (`MultipleCategoryBarPlot.tsx:446-453`)
- By sample count: `SortByTotalSum` sorts by total bar height descending
- Per-category: sort by specific minor category count

### Clinical Categories
Alphabetical sort of unique values (no special ordering)

---

## 10. Log Scale Rules

### When to Show
- Shown when: numeric data available with no negative values
- Auto-enabled when: profile ID matches `/rna_seq/i` and not `/zscore/i` (`PlotsTab.tsx:65` — `maybeSetLogScale`)

### Transform Functions
- **Non-generic-assay:** `log2(value + 1)`, inverse: `2^x - 1`
- **Generic assay:** `log10(value + offset)`, inverse: `10^(x - offset)`

### Axis Label Modifier
Appends the transform description to the axis label:
- `" (log2(value + 1))"` for standard
- `" (log10)"` for generic assay

---

## 11. Profiling & Filtering

### Profiling Status
- Each sample can be independently profiled or not for mutations, CNA, and SV
- Determined via gene panel data (`CoverageInformation`) — `isSampleProfiledInMultiple()` (`PlotsTabUtils.tsx:1170-1178`)
- A sample is "profiled" if it has coverage for the queried gene in any of the relevant molecular profiles

### Visual Treatment of Unprofiled Samples

| Context | Appearance | Source |
|---------|-----------|--------|
| Not profiled (mutations) | fill `#ffffff`, stroke `#D3D3D3` | `PlotsTabUtils.tsx:1967-1972` |
| Not profiled (CNA/SV) | stroke `#000000` | `PlotsTabUtils.tsx:1962-1966` |
| No data (clinical) | fill `#D3D3D3` | `PlotsTabUtils.tsx:1973-1977` |

### "Not profiled" as Category
In discrete axes (mutation, SV), unprofiled samples get the category:
- Mutations: `"Not profiled"` (`PlotsTabUtils.tsx:2078`)
- SV: `"Not profiled for structural variants"` (`PlotsTabUtils.tsx:2085-2086`)

### Category Limit
`DISCRETE_CATEGORY_LIMIT = 150` — chart not rendered if exceeded (`PlotsTab.tsx:5612-5625`)

### `hideUnprofiledSamples`
Can be `false`, `'any'`, or `'totally'` — controls whether unprofiled samples appear in the data.

### Data Availability Banner
Shows sample counts for each axis and their intersection. Styled with background `#eee` (`styles.scss:185-195`).

---

## 12. Implementation Status

| Feature | Legacy | Our Impl | Gap / Notes |
|---------|--------|----------|-------------|
| **Axis Data Types** | | | |
| Clinical attribute axis | Yes | Yes | |
| Mutation (MutationType) | Yes | Yes | |
| Mutation (MutatedVsWildType) | Yes | Yes | |
| Mutation (DriverVsVUS) | Yes | No | Needs `putativeDriver` field in data |
| Mutation (VAF) | Yes | No | Needs `tumorAltCount`/`tumorRefCount` |
| CNA axis (discrete) | Yes | Yes | Integer-only + profiled-only semantics |
| SV (MutatedVsWildType) | Yes | Yes | |
| SV (VariantType) | Yes | No | Needs `variantClass` grouping |
| mRNA Expression | Yes | No | Needs continuous molecular data tables |
| Protein Level | Yes | No | Low priority |
| DNA Methylation | Yes | No | Low priority |
| Gene Sets | Yes | No | Low priority |
| Generic Assay | Yes | No | Low priority |
| **Plot Types** | | | |
| Bar chart (stacked) | Yes | Yes | |
| Bar chart (grouped) | Yes | Yes | |
| Bar chart (100% stacked) | Yes | Yes | |
| Table plot | Yes | Yes | |
| Scatter plot | Yes | Yes | Missing coloring overlay |
| Box plot | Yes | Yes | Missing coloring overlay |
| Waterfall plot | Yes | No | Only used with generic assay |
| **Coloring Overlays** | | | |
| Color by mutation type | Yes | No | |
| Color by CNA | Yes | No | |
| Color by SV | Yes | No | |
| Color by clinical attribute | Yes | No | |
| **UI Features** | | | |
| Swap axes | Yes | Yes | |
| Quick plots pills | Yes | Yes | Different presets (Mut# vs Dx, FGA vs Dx, Mut# vs FGA) — adapted for available data |
| Log scale | Yes | Yes | log2(value + 1) transform for scatter and box plots |
| Regression line | Yes | Yes | Client-side least-squares linear regression on scatter |
| Horizontal bars toggle | Yes | Yes | |
| Sort by options | Yes | Yes | Alphabetical + sample count (bar); alphabetical + median (box) |
| Connect samples (box) | Yes | No | Multi-sample patients |
| Data availability banner | Yes | No | Sample count display |
| **Export** | | | |
| Download SVG | Yes | No | |
| Download PNG | Yes | No | |
| Download data (TSV) | Yes | No | See `PlotsTabUtils.tsx:3338-3526` |

---

## Appendix A: Backend API Endpoints

The legacy Java backend serves data via these REST endpoints (used by the frontend's data caches).

### Endpoints Used by the Plots Tab

| Endpoint | Method | Purpose | Used By |
|----------|--------|---------|---------|
| `/api/molecular-profiles/fetch` | POST | Fetch molecular profiles by study IDs | Profile dropdown |
| `/api/studies/{id}/molecular-profiles` | GET | Study-specific profiles | Profile dropdown |
| `/api/molecular-profiles/{id}/molecular-data/fetch` | POST | Fetch molecular data (expression, CNA) | CNA, mRNA, protein, methylation axes |
| `/api/molecular-data/fetch` | POST | Multi-study molecular data | Cross-study plots |
| `/api/mutations/fetch` | POST | Fetch mutations | Mutation axis |
| `/api/clinical-data/fetch` | POST | Clinical data across studies | Clinical axis |
| `/api/studies/{id}/clinical-data/fetch` | POST | Study-specific clinical data | Clinical axis |
| `/api/studies/{id}/clinical-attributes` | GET | Study-specific attributes | Attribute dropdowns |
| `/api/clinical-attributes/fetch` | POST | Attributes by study IDs | Attribute dropdowns |
| `/api/structural-variant/fetch` | POST | Structural variant data | SV axis |
| `/api/gene-panel-data/fetch` | POST | Gene panel coverage | Profiling status |
| `/api/genes/fetch` | POST | Gene lookup | Gene selectors |
| `/api/molecular-profiles/{id}/discrete-copy-number/fetch` | POST | Discrete CNA data | CNA axis |
| `/api/sample-lists/fetch` | POST | Sample list IDs | Sample selection |
| `/api/gene-panels/fetch` | POST | Gene panel definitions | Profiling status |

### Live API Examples (captured from cbioportal.org, study: msk_chord_2024, gene: BRAF)

#### Molecular Profiles (`POST /api/molecular-profiles/fetch`)
**Request:** `{"studyIds": ["msk_chord_2024"]}`
**Response (3 profiles):**
```json
[
  {
    "molecularAlterationType": "COPY_NUMBER_ALTERATION",
    "datatype": "DISCRETE",
    "name": "Putative copy-number alterations from GISTIC",
    "description": "Putative copy-number from GISTIC 2.0. Values: -2 = homozygous deletion; -1 = hemizygous deletion; 0 = neutral / no change; 1 = gain; 2 = high level amplification.",
    "showProfileInAnalysisTab": true,
    "patientLevel": false,
    "molecularProfileId": "msk_chord_2024_cna",
    "studyId": "msk_chord_2024"
  },
  {
    "molecularAlterationType": "MUTATION_EXTENDED",
    "datatype": "MAF",
    "name": "Mutations",
    "description": "Mutation data.",
    "showProfileInAnalysisTab": true,
    "molecularProfileId": "msk_chord_2024_mutations",
    "studyId": "msk_chord_2024"
  },
  {
    "molecularAlterationType": "STRUCTURAL_VARIANT",
    "datatype": "SV",
    "name": "Structural Variants",
    "description": "Structural Variant Data.",
    "showProfileInAnalysisTab": true,
    "molecularProfileId": "msk_chord_2024_structural_variants",
    "studyId": "msk_chord_2024"
  }
]
```

#### CNA Molecular Data (`POST /api/molecular-profiles/msk_chord_2024_cna/molecular-data/fetch?projection=DETAILED`)
**Request:** `{"entrezGeneIds": [673], "sampleListId": "msk_chord_2024_all"}`
**Response (25,034 rows, sample):**
```json
{
  "uniqueSampleKey": "UC0wMDAwMDEyLVQwMi1JTTM6bXNrX2Nob3JkXzIwMjQ",
  "uniquePatientKey": "UC0wMDAwMDEyOm1za19jaG9yZF8yMDI0",
  "entrezGeneId": 673,
  "gene": {"entrezGeneId": 673, "hugoGeneSymbol": "BRAF", "type": "protein-coding"},
  "molecularProfileId": "msk_chord_2024_cna",
  "sampleId": "P-0000012-T02-IM3",
  "patientId": "P-0000012",
  "studyId": "msk_chord_2024",
  "value": 0
}
```

#### Clinical Attributes (`GET /api/studies/msk_chord_2024/clinical-attributes`)
**Response (50 attributes, sample):**
```json
{
  "displayName": "Cancer Type",
  "description": "The main cancer type as defined by the Oncotree cancer classification system...",
  "datatype": "STRING",
  "patientAttribute": false,
  "priority": "3000",
  "clinicalAttributeId": "CANCER_TYPE",
  "studyId": "msk_chord_2024"
}
```

#### Gene Panel Data (`POST /api/gene-panel-data/fetch?projection=SUMMARY`)
**Request:** `{"sampleMolecularIdentifiers": [{"molecularProfileId": "msk_chord_2024_cna", "sampleId": "P-0000012-T02-IM3"}, {"molecularProfileId": "msk_chord_2024_mutations", "sampleId": "P-0000012-T02-IM3"}]}`
**Response:**
```json
[
  {
    "molecularProfileId": "msk_chord_2024_mutations",
    "sampleId": "P-0000012-T02-IM3",
    "patientId": "P-0000012",
    "studyId": "msk_chord_2024",
    "genePanelId": "IMPACT341",
    "profiled": true
  },
  {
    "molecularProfileId": "msk_chord_2024_cna",
    "sampleId": "P-0000012-T02-IM3",
    "patientId": "P-0000012",
    "studyId": "msk_chord_2024",
    "genePanelId": "IMPACT341",
    "profiled": true
  }
]
```

### Request Sequence on Plots Tab Load
Captured via playwright-cli on `cbioportal.org` — the Plots tab makes these API calls in order:
1. `POST /api/genes/fetch` — resolve gene symbols (BRAF → entrezGeneId 673)
2. `POST /api/molecular-profiles/fetch` — get available profiles for the study
3. `POST /api/sample-lists/fetch` — get sample list for the study
4. `POST /api/column-store/samples/fetch` — get sample details
5. `POST /api/gene-panel-data/fetch` — get profiling status per sample per profile
6. `POST /api/molecular-data/fetch` — fetch CNA data for BRAF (all samples)
7. `POST /api/mutations/fetch` — fetch mutation data for BRAF
8. `POST /api/patients/fetch` — get patient metadata
9. `POST /api/molecular-profiles/{id}/molecular-data/fetch` — CNA data (META projection for counts)
10. `POST /api/gene-panels/fetch` — gene panel definitions
11. `POST /api/clinical-attributes/fetch` — available clinical attributes
12. `POST /api/clinical-attributes/counts/fetch` — attribute value counts
13. `POST /api/studies/{id}/clinical-data/fetch` — actual clinical values (CANCER_TYPE for default vertical axis)
14. `POST /api/molecular-profiles/{id}/molecular-data/fetch` — CNA data (for the chart)

---

## Appendix B: Mutation Type Display Name Map

Source: `PlotsTabUtils.tsx:108-117` (`mutationTypeToDisplayName`)

| OncoprintMutationType | Display Name |
|----------------------|-------------|
| `missense` | Missense |
| `inframe` | Inframe |
| `splice` | Splice |
| `promoter` | Promoter |
| `trunc` | Truncating |
| `other` | Other |

---

## Appendix C: Profile Status Labels

Source: `PlotsTabUtils.tsx:2073-2086`

**Mutation:**
| Constant | Label |
|----------|-------|
| `MUT_PROFILE_COUNT_DRIVER` | Driver |
| `MUT_PROFILE_COUNT_VUS` | VUS |
| `MUT_PROFILE_COUNT_MUTATED` | Mutated |
| `MUT_PROFILE_COUNT_MULTIPLE` | Multiple |
| `MUT_PROFILE_COUNT_NOT_MUTATED` | No mutation |
| `MUT_PROFILE_COUNT_NOT_PROFILED` | Not profiled |

**Structural Variant:**
| Constant | Label |
|----------|-------|
| `STRUCTURAL_VARIANT_PROFILE_COUNT_MUTATED` | With Structural Variants |
| `STRUCTURAL_VARIANT_PROFILE_COUNT_MULTIPLE` | Multiple structural variants |
| `STRUCTURAL_VARIANT_PROFILE_COUNT_NOT_MUTATED` | No Structural Variants |
| `STRUCTURAL_VARIANT_PROFILE_COUNT_NOT_PROFILED` | Not profiled for structural variants |
