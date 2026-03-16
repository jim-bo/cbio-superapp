# templates/home/

Homepage templates — the cBioPortal study browser.

## Biology context

The homepage lets users search and select studies by cancer type, data type, and
keyword. Studies are grouped by cancer type (organ system) using the OncoPrint color
palette. Selecting a study navigates to the Study View dashboard.

## Engineering context

- `page.html` extends `base.html` and is the full-page template for `/`.
- All dynamic content uses HTMX (`hx-post`, `hx-trigger`, `hx-target`) — no JS.
- `partials/cancer_type_list.html` — HTMX trigger source; posts to `/studies` on change.
- `partials/cancer_study_list.html` — HTMX swap target (`#study-list-wrapper`).
- `partials/study_selector_header.html` — Filter bar (search, data type dropdown).
- `partials/study_selector_footer.html` — Action buttons (Query / Explore).
- `partials/right_sidebar.html` — What's New, Example Queries.
- `partials/subheader.html` — Tab bar shell (Query / Quick Search).

## When to cite legacy code

Study grouping and cancer type color palette mirror `StudySelectorPage.tsx` in the
React frontend. The quick search behavior mirrors `QuickSearch.tsx`.
