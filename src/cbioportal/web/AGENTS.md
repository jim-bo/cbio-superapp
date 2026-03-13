# web/

FastAPI app, routes, templates, and static assets.

- `app.py` — App factory, registers all routers
- `routes/` — One file per page (home.py, study_view.py)
- `templates/` — Jinja2 templates (see templates/AGENTS.md for layout conventions)
- `static/` — CSS, JS, images (no build step for most; dashboard/ has a Vite build)
