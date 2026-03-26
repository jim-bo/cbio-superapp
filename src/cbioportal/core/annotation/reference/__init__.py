"""Reference data loaders for the annotation pipeline.

Public API:
    ensure_all_reference_data(conn_cache)   — idempotent; refreshes if stale
    refresh_all_reference_data(conn_cache)  — force refresh regardless of TTL

Each source is fetched independently; failures in one source are logged as
warnings but do not abort the pipeline — the pipeline continues with
NULL columns for that source.
"""
from __future__ import annotations

import logging

from .civic import ensure_civic, refresh_civic
from .intogen import ensure_intogen, refresh_intogen
from .moalmanac import ensure_moalmanac, refresh_moalmanac

logger = logging.getLogger(__name__)

__all__ = [
    "ensure_all_reference_data",
    "refresh_all_reference_data",
    "ensure_moalmanac",
    "ensure_civic",
    "ensure_intogen",
]


def ensure_all_reference_data(conn_cache) -> None:
    """Ensure all reference tables are present and up-to-date (respects TTL).

    Each source failure is non-fatal: a warning is printed and the pipeline
    continues with NULL values for that source's columns.
    """
    for label, fn, fallback in [
        ("MOAlmanac", lambda: ensure_moalmanac(conn_cache), lambda: _ensure_moalmanac_tables(conn_cache)),
        ("CIViC", lambda: ensure_civic(conn_cache), lambda: _ensure_civic_tables(conn_cache)),
        ("IntOGen", lambda: ensure_intogen(conn_cache), lambda: _ensure_intogen_tables(conn_cache)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.warning("%s reference refresh failed (non-fatal): %s", label, e)
            print(f"WARNING: {label} reference data unavailable ({e}). Continuing without it.")
            try:
                fallback()
            except Exception:
                pass


def _ensure_moalmanac_tables(conn) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS moalmanac_features_bulk (gene VARCHAR, alteration VARCHAR, feature_id INTEGER, feature_type VARCHAR, alt_type VARCHAR, payload JSON)")
    conn.execute("CREATE TABLE IF NOT EXISTS moalmanac_assertions_bulk (feature_id INTEGER, clinical_significance VARCHAR, drug VARCHAR, disease VARCHAR, score_bin VARCHAR, oncogenic VARCHAR, payload JSON)")


def _ensure_civic_tables(conn) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS civic_evidence (evidence_id INTEGER, gene VARCHAR, variant_name VARCHAR, hgvsp_short VARCHAR, evidence_type VARCHAR, clinical_significance VARCHAR, evidence_level VARCHAR, drugs VARCHAR, disease VARCHAR, oncotree_code VARCHAR, fetched_at TIMESTAMP)")


def _ensure_intogen_tables(conn) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS intogen_drivers (symbol VARCHAR, tumor_type VARCHAR, oncotree_code VARCHAR, role VARCHAR, methods VARCHAR, qvalue_combination DOUBLE, fetched_at TIMESTAMP)")


def refresh_all_reference_data(conn_cache) -> None:
    """Force re-download all reference data regardless of TTL."""
    for label, fn in [
        ("MOAlmanac", lambda: refresh_moalmanac(conn_cache)),
        ("CIViC", lambda: refresh_civic(conn_cache)),
        ("IntOGen", lambda: refresh_intogen(conn_cache)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.warning("%s refresh failed: %s", label, e)
            print(f"WARNING: {label} refresh failed: {e}")
