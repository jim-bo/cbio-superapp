# tests/

- `unit/` — Fast tests, in-memory DuckDB only. No real study data needed.
- `test_study_view_charts.py` — Golden/integration tests. Requires real DuckDB with msk_chord_2024 loaded.
- `fixtures/` — JSON baselines captured from the public cBioPortal for comparison.
- `capture_golden.py` — Script to refresh fixture data from the public portal.

Run before any PR:
  uv run pytest tests/unit/ -v
  uv run pytest tests/test_study_view_charts.py -v

## Golden value protection

Golden values in `test_study_view_charts.py` (exact numeric assertions like Pearson/Spearman
correlations, gene counts, survival values) are pinned against the public cBioPortal portal.

**Never change a golden value to make a failing test pass.** A failing golden test means our
computation diverges from the legacy portal — that is a bug to fix, not a number to update.
Only change a golden value when the user explicitly approves it after reviewing the discrepancy.
