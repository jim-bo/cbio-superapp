# OncoPrint Menu Gaps vs Legacy cBioPortal

## Status Key
- [x] Implemented
- [ ] Not yet implemented
- [SKIP] Requires significant backend work or user input

## 1. Tracks Menu
- [x] Clinical track list with checkboxes and frequency
- [ ] Search box to filter clinical tracks
- [ ] Select all / Deselect all buttons
- [SKIP] Heatmap tab (requires heatmap data endpoints)
- [SKIP] Save tracks button (requires user preferences persistence)

## 2. Sort Menu
Current: simple "Sort by data" / "Sort alphabetically" links
Legacy: radio buttons with sub-options

- [x] Sort by data (default)
- [ ] Sort by data sub-checkboxes: Mutation Type, Driver/Passenger
- [x] Sort by case id (alphabetical)
- [SKIP] Sort by case list order (requires case list from query)
- [SKIP] Sorted by heatmap clustering order (requires heatmap)

## 3. Mutations Menu (Color By)
Current: checkboxes for "Distinguish mutation type" and "Distinguish driver mutations"
Legacy: "Color by" header with Type + Somatic vs Germline, plus Annotate and Filter section

- [x] Color by Type (distinguish mutation type) — switches ruleset
- [x] Color by Driver/Passenger — switches ruleset
- [ ] Somatic vs Germline checkbox (separate from driver)
- [SKIP] Annotate and Filter (OncoKB integration, hotspot filtering, hide VUS/germline)

## 4. View Menu
Current: checkboxes for unaltered columns, whitespace, minimap
Legacy: Data type radio, plus multiple view options

- [ ] Data type: Events per sample / Events per patient radio
- [x] Show unaltered columns — needs wiring to `hideIds()`
- [x] Show whitespace between columns — needs wiring to `setCellPaddingOn()`
- [x] Show minimap — already wired to `toggleMinimapVisibility()`
- [ ] Show legends for clinical tracks
- [SKIP] Only show clinical track legends for altered patients
- [SKIP] Show OQL filters
- [SKIP] Use white background for glyphs

## 5. Download Menu
Current: placeholder links for PDF/PNG/SVG/Tabular
Legacy: functional buttons for PDF, PNG, SVG, Patient order, Tabular, Open in Oncoprinter

- [ ] PDF download — `oncoprint.toSVG()` → svgToPdf
- [ ] PNG download — `oncoprint.toCanvas()` → blob download
- [ ] SVG download — `oncoprint.toSVG()` → serialize
- [ ] Patient/sample order download — `oncoprint.getIdOrder()`
- [SKIP] Tabular download (requires server-side data assembly)
- [SKIP] Open in Oncoprinter
- [SKIP] Open in Jupyter Notebook

## Priority Order for Implementation
1. **Sort menu** — wire sub-checkboxes for Mutation Type / Driver sort
2. **Mutations menu** — wire ruleset switching (the 4 rulesets already exist in geneticrules.js)
3. **View menu** — wire hideIds, setCellPaddingOn, legend toggling
4. **Download menu** — wire SVG/PNG/PDF export using oncoprintjs API
5. **Tracks menu** — add search box and select all/deselect all
