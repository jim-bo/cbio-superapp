# web/static/

Static assets served directly by FastAPI at `/static/`.

## Engineering context

- `css/styles.css` — Global styles (Bootstrap overrides, typography, nav, footer).
- `css/prefixed-bootstrap.min.css` — Scoped Bootstrap 3 to avoid conflicts with
  page-specific styles.
- `img/` — Logos and icons.
- No build step for standard CSS/images.

## Conventions

- Page-specific CSS lives co-located with its template, not here.
  (e.g. `templates/study_view/study_view.css` — included via Jinja2 `{% include %}`.)
- New global styles go in `css/styles.css`; keep selectors scoped to avoid bleed.
- Avoid adding large JS bundles here — CDN links are preferred for third-party libs.
