"""Gene frequency tools — panel-coverage-aware denominators.

These wrap ``core.study_view.genomic`` which uses the pre-computed
``{study_id}_profiled_counts`` table so freq = n_samples / n_profiled,
matching the legacy cBioPortal Study View dashboard exactly.
"""
from __future__ import annotations

import asyncio
from typing import Iterable

from cli_textual.tools.base import ToolResult

from cbioportal.cli.tools._db import open_conn
from cbioportal.core.study_view import genomic as sv_genomic


def _normalize_genes(genes: str | Iterable[str] | None) -> set[str] | None:
    if not genes:
        return None
    if isinstance(genes, str):
        parts = genes.replace(",", " ").split()
    else:
        parts = list(genes)
    out = {g.strip().upper() for g in parts if g and g.strip()}
    return out or None


def _format_rows(rows: list[dict], cols: list[tuple[str, str]]) -> str:
    if not rows:
        return "_(no matching genes)_"
    header = "| " + " | ".join(label for _, label in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    lines = [header, sep]
    for r in rows:
        lines.append(
            "| " + " | ".join(str(r.get(k, "")) for k, _ in cols) + " |"
        )
    return "\n".join(lines)


def _run_freq(
    fetch_fn,
    study_id: str,
    genes: str | None,
    limit: int,
    count_col: tuple[str, str],
) -> ToolResult:
    gene_filter = _normalize_genes(genes)
    # If the caller named specific genes, fetch a larger slice so we don't miss
    # them past the default top-N cutoff.
    fetch_limit = max(limit, 2000) if gene_filter else limit
    with open_conn() as conn:
        try:
            rows = fetch_fn(conn, study_id, filter_json=None, limit=fetch_limit)
        except Exception as exc:
            return ToolResult(
                output=f"Error querying {study_id}: {exc}",
                is_error=True,
                exit_code=1,
            )

    if gene_filter:
        rows = [r for r in rows if str(r.get("gene", "")).upper() in gene_filter]
        missing = gene_filter - {str(r.get("gene", "")).upper() for r in rows}
    else:
        missing = set()

    rows = rows[:limit]
    cols = [
        ("gene", "gene"),
        count_col,
        ("n_samples", "n_samples"),
        ("n_profiled", "n_profiled"),
        ("freq", "freq %"),
    ]
    table = _format_rows(rows, cols)
    footer_parts = [f"_Study: `{study_id}` · freq is % of profiled samples._"]
    if missing:
        footer_parts.append(
            f"_Not found (no events or not profiled): {', '.join(sorted(missing))}_"
        )
    return ToolResult(output=table + "\n\n" + "\n".join(footer_parts))


async def gene_mutation_frequency(
    study_id: str,
    genes: str | None = None,
    limit: int = 25,
) -> ToolResult:
    """Mutation frequency per gene, using panel-coverage-aware denominators.

    The frequency column is ``n_samples_mutated / n_samples_profiled_for_gene * 100``,
    matching the cBioPortal Study View dashboard. Genes not covered by a sample's
    panel are excluded from that sample's denominator.

    Args:
        study_id: Study identifier, e.g. ``"msk_chord_2024"``.
        genes: Optional comma- or space-separated gene symbols to filter the result
            (e.g. ``"TP53 KRAS EGFR"``). If omitted, returns the top-``limit`` genes
            by sample count.
        limit: Maximum rows to return. Defaults to 25.
    """
    return await asyncio.to_thread(
        _run_freq,
        sv_genomic.get_mutated_genes,
        study_id,
        genes,
        limit,
        ("n_mut", "n_mut"),
    )


async def gene_cna_frequency(
    study_id: str,
    genes: str | None = None,
    limit: int = 25,
) -> ToolResult:
    """Copy-number alteration frequency per gene (panel-aware).

    Args:
        study_id: Study identifier.
        genes: Optional gene symbol filter (comma- or space-separated).
        limit: Maximum rows to return.
    """
    return await asyncio.to_thread(
        _run_freq,
        sv_genomic.get_cna_genes,
        study_id,
        genes,
        limit,
        ("cna_type", "cna_type"),
    )


async def gene_sv_frequency(
    study_id: str,
    genes: str | None = None,
    limit: int = 25,
) -> ToolResult:
    """Structural variant frequency per gene (panel-aware).

    Args:
        study_id: Study identifier.
        genes: Optional gene symbol filter.
        limit: Maximum rows to return.
    """
    return await asyncio.to_thread(
        _run_freq,
        sv_genomic.get_sv_genes,
        study_id,
        genes,
        limit,
        ("n_sv", "n_sv"),
    )
