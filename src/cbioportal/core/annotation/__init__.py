"""Variant annotation pipeline.

Public API:
    annotate_study(conn, study_id, cache_db_path, force, skip_vibe_vep)
    refresh_reference_data()
"""
from __future__ import annotations

import logging

from ..cache import CACHE_DB_PATH, get_cache_connection
from .annotators import annotate_cna, annotate_mutations, annotate_sv
from .reference import ensure_all_reference_data, refresh_all_reference_data
from .vep import annotate_with_vep, is_vep_available
from .writer import TABLE_SUFFIX, write_variant_annotations  # noqa: F401

logger = logging.getLogger(__name__)

__all__ = ["annotate_study", "refresh_reference_data", "TABLE_SUFFIX"]


def _is_annotated(conn, study_id: str) -> bool:
    """Return True if a non-empty annotations table already exists."""
    table = f"{study_id}_variant_annotations"
    exists = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        (table,),
    ).fetchone()
    if not exists:
        return False
    count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    return count > 0


def annotate_study(
    conn,
    study_id: str,
    cache_db_path: str | None = None,
    force: bool = False,
    skip_vibe_vep: bool = False,
) -> dict:
    """Annotate all alterations for a study and write variant_annotations table.

    Args:
        conn:           Open connection to the study DuckDB.
        study_id:       Study identifier.
        cache_db_path:  Path to cache DuckDB; defaults to CACHE_DB_PATH.
        force:          Drop and rebuild even if already annotated.
        skip_vibe_vep:  Skip vibe-vep even if available.

    Returns:
        dict with keys: mutations, cna, sv, total (row counts), skipped (bool).
    """
    if cache_db_path is None:
        cache_db_path = str(CACHE_DB_PATH)

    if not force and _is_annotated(conn, study_id):
        logger.info("Study %s already annotated; use force=True to rebuild", study_id)
        count = conn.execute(
            f'SELECT COUNT(*) FROM "{study_id}_variant_annotations"'
        ).fetchone()[0]
        return {"mutations": 0, "cna": 0, "sv": 0, "total": count, "skipped": True}

    # ── 1. Ensure reference data is up-to-date ───────────────────────────────
    conn_cache = get_cache_connection()
    try:
        ensure_all_reference_data(conn_cache)
    finally:
        conn_cache.close()

    # ── 2. vibe-vep (optional) ───────────────────────────────────────────────
    vep_lookup: dict | None = None
    if not skip_vibe_vep and is_vep_available():
        logger.info("Running vibe-vep for %s", study_id)
        vep_lookup = annotate_with_vep(conn, study_id)

    # ── 3. Annotators ────────────────────────────────────────────────────────
    logger.info("Annotating mutations for %s", study_id)
    mut_rows = annotate_mutations(conn, study_id, cache_db_path, vep_lookup)

    logger.info("Annotating CNAs for %s", study_id)
    cna_rows = annotate_cna(conn, study_id, cache_db_path)

    logger.info("Annotating SVs for %s", study_id)
    sv_rows = annotate_sv(conn, study_id, cache_db_path)

    all_rows = mut_rows + cna_rows + sv_rows

    # ── 4. Write ──────────────────────────────────────────────────────────────
    total = write_variant_annotations(conn, study_id, all_rows)

    # ── 5. Compute cbp_driver on the mutations table ─────────────────────────
    _compute_cbp_driver(conn, study_id)

    summary = {
        "mutations": len(mut_rows),
        "cna": len(cna_rows),
        "sv": len(sv_rows),
        "total": total,
        "skipped": False,
    }
    logger.info(
        "Annotation complete for %s: %d mutations, %d cna, %d sv (%d total rows)",
        study_id,
        summary["mutations"],
        summary["cna"],
        summary["sv"],
        total,
    )
    return summary


def _compute_cbp_driver(conn, study_id: str) -> None:
    """Add cbp_driver column to the mutations table from variant_annotations.

    Heuristic classification (will be replaced by OncoKB integration):
      - Putative_Driver if ANY of:
          * hotspot_type IS NOT NULL (mutation is a known hotspot)
          * intogen_role IN ('Act', 'LoF') AND variant_classification indicates
            functional impact (Missense, Nonsense, Frameshift, Splice, Inframe)
          * moalmanac_oncogenic ILIKE '%oncogenic%'
      - Putative_Passenger otherwise

    NOTE: This is a heuristic approximation. The legacy cBioPortal uses OncoKB's
    oncogenic field as the primary driver signal. When OncoKB integration is added
    (oncokb_oncogenic column), this logic should be updated to:
        Driver = oncokb_oncogenic IN ('Oncogenic', 'Likely Oncogenic', 'Resistance')
                 OR hotspot OR cbp_driver_binary
    """
    mut_table = f'"{study_id}_mutations"'
    ann_table = f'"{study_id}_variant_annotations"'

    # Check both tables exist
    for tbl in [f"{study_id}_mutations", f"{study_id}_variant_annotations"]:
        exists = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            (tbl,),
        ).fetchone()[0]
        if not exists:
            logger.warning("Cannot compute cbp_driver: table %s missing", tbl)
            return

    # Add cbp_driver column if it doesn't exist
    has_col = conn.execute(
        "SELECT count(*) FROM information_schema.columns "
        f"WHERE table_name = '{study_id}_mutations' AND column_name = 'cbp_driver'"
    ).fetchone()[0]
    if not has_col:
        conn.execute(f"ALTER TABLE {mut_table} ADD COLUMN cbp_driver VARCHAR")

    # Functional variant classifications that support driver calls
    # (non-functional VCs like Silent, IGR are already excluded at load time)
    _FUNCTIONAL_VCS = (
        "Missense_Mutation", "Nonsense_Mutation",
        "Frame_Shift_Del", "Frame_Shift_Ins",
        "Splice_Site", "Splice_Region",
        "In_Frame_Del", "In_Frame_Ins",
        "Translation_Start_Site", "Nonstop_Mutation",
    )
    vc_list = ", ".join(f"'{vc}'" for vc in _FUNCTIONAL_VCS)

    conn.execute(f"""
        UPDATE {mut_table} AS m
        SET cbp_driver = CASE
            WHEN a.hotspot_type IS NOT NULL
                THEN 'Putative_Driver'
            WHEN a.intogen_role IN ('Act', 'LoF')
                AND a.variant_classification IN ({vc_list})
                THEN 'Putative_Driver'
            WHEN a.moalmanac_oncogenic IS NOT NULL
                AND LOWER(a.moalmanac_oncogenic) LIKE '%oncogenic%'
                THEN 'Putative_Driver'
            ELSE 'Putative_Passenger'
        END
        FROM {ann_table} AS a
        WHERE a.alteration_type = 'MUTATION'
            AND a.sample_id = m.Tumor_Sample_Barcode
            AND a.hugo_symbol = m.Hugo_Symbol
            AND COALESCE(a.hgvsp_short, '') = COALESCE(m.HGVSp_Short, '')
    """)

    # Count results
    counts = conn.execute(
        f"SELECT cbp_driver, count(*) FROM {mut_table} "
        f"WHERE cbp_driver IS NOT NULL GROUP BY cbp_driver"
    ).fetchall()
    for label, cnt in counts:
        logger.info("cbp_driver %s: %d mutations (%s)", label, cnt, study_id)


def refresh_reference_data() -> None:
    """Force re-download all reference data."""
    conn_cache = get_cache_connection()
    try:
        refresh_all_reference_data(conn_cache)
    finally:
        conn_cache.close()
