"""Study listing / metadata tools."""
from __future__ import annotations

import asyncio

from cli_textual.tools.base import ToolResult

from cbioportal.cli.tools._db import open_conn
from cbioportal.core import study_repository


def _format_table(rows: list[dict], cols: list[tuple[str, str]]) -> str:
    """Render a list of dicts as a GitHub-flavored markdown table."""
    if not rows:
        return "_(no rows)_"
    header = "| " + " | ".join(label for _, label in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    lines = [header, sep]
    for r in rows:
        cells = []
        for key, _ in cols:
            v = r.get(key, "")
            if isinstance(v, list):
                v = ", ".join(str(x) for x in v)
            cells.append(str(v) if v is not None else "")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _list_studies_sync(
    cancer_type: str | None,
    data_type: str | None,
    limit: int,
) -> ToolResult:
    with open_conn() as conn:
        try:
            names = study_repository.load_study_names(conn)
            catalog = study_repository.get_study_catalog(
                conn,
                names,
                cancer_type=cancer_type,
                data_types=[data_type] if data_type else None,
            )
        except Exception as exc:  # DB not initialized, view missing, etc.
            return ToolResult(
                output=f"Error querying studies: {exc}",
                is_error=True,
                exit_code=1,
            )

    total = len(catalog)
    truncated = catalog[:limit]
    table = _format_table(
        truncated,
        cols=[
            ("id", "study_id"),
            ("name", "name"),
            ("cancer_type", "cancer_type"),
            ("sample_count", "samples"),
            ("data_types", "data_types"),
        ],
    )
    footer = (
        f"\n\n_Showing {len(truncated)} of {total} studies._"
        if total > limit
        else f"\n\n_{total} studies._"
    )
    return ToolResult(output=table + footer)


async def list_studies(
    cancer_type: str | None = None,
    data_type: str | None = None,
    limit: int = 50,
) -> ToolResult:
    """List studies in the local cbioportal database.

    Args:
        cancer_type: Filter by category (e.g. ``"Breast"``, ``"PanCancer Studies"``).
            Use ``None`` or ``"All"`` for no filter.
        data_type: Filter by a single data type (``"mutation"``, ``"cna"``, ``"sv"``,
            ``"mrna"``, etc.). Only studies providing this data type are returned.
        limit: Maximum number of rows to return. Defaults to 50.
    """
    return await asyncio.to_thread(_list_studies_sync, cancer_type, data_type, limit)


def _describe_study_sync(study_id: str) -> ToolResult:
    with open_conn() as conn:
        try:
            row = conn.execute(
                "SELECT study_id, name, type_of_cancer, category, description, "
                "pmid, citation FROM studies WHERE study_id = ?",
                [study_id],
            ).fetchone()
        except Exception as exc:
            return ToolResult(
                output=f"Error: {exc}", is_error=True, exit_code=1
            )
        if not row:
            return ToolResult(
                output=f"No study found with id={study_id!r}.",
                is_error=True,
                exit_code=1,
            )
        try:
            dt_rows = conn.execute(
                "SELECT data_type FROM study_data_types WHERE study_id = ?",
                [study_id],
            ).fetchall()
        except Exception:
            dt_rows = []
        try:
            n_samples = conn.execute(
                f'SELECT COUNT(*) FROM "{study_id}_sample"'
            ).fetchone()[0]
        except Exception:
            n_samples = 0

    sid, name, cancer, category, desc, pmid, citation = row
    data_types = ", ".join(sorted({r[0] for r in dt_rows})) or "(none)"
    lines = [
        f"# {name or sid}",
        "",
        f"- **study_id**: `{sid}`",
        f"- **cancer type**: {cancer or '—'}",
        f"- **category**: {category or '—'}",
        f"- **samples**: {n_samples}",
        f"- **data types**: {data_types}",
        f"- **pmid**: {pmid or '—'}",
    ]
    if citation:
        lines.append(f"- **citation**: {citation}")
    if desc:
        lines += ["", desc]
    return ToolResult(output="\n".join(lines))


async def describe_study(study_id: str) -> ToolResult:
    """Show metadata for a single study.

    Args:
        study_id: The study identifier, e.g. ``"msk_chord_2024"``.
    """
    return await asyncio.to_thread(_describe_study_sync, study_id)
