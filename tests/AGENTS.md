# tests/

- `unit/` — Fast tests, in-memory DuckDB only. No real study data needed.
- `test_study_view_charts.py` — Golden/integration tests. Requires real DuckDB with msk_chord_2024 loaded.
- `fixtures/` — JSON baselines captured from the public cBioPortal for comparison.
- `capture_golden.py` — Script to refresh fixture data from the public portal.

Run before any PR:
  uv run pytest tests/unit/ -v
  uv run pytest tests/test_study_view_charts.py -v
