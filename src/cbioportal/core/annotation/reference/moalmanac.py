"""MOAlmanac reference data loader for the annotation cache DB.

Supersedes core/annotators/moalmanac.py — adds feature_type/alt_type columns
for CNA and fusion matching in addition to somatic variants.

The old module is kept for backward-compat with data_puller.py.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import httpx

MOALMANAC_FEATURES_API = "https://moalmanac.org/api/features"
MOALMANAC_ASSERTIONS_API = "https://moalmanac.org/api/assertions"
TTL_DAYS = 7


def _infer_feature_type(feature: dict) -> tuple[str, str | None]:
    """Infer (feature_type, alt_type) from a MOAlmanac feature dict."""
    attrs = feature.get("attributes", [{}])
    attr = attrs[0] if attrs else {}

    feat_type_raw = feature.get("feature_type", "") or attr.get("feature_type", "")
    feat_type_lower = feat_type_raw.lower()

    if "copy" in feat_type_lower or "cna" in feat_type_lower or "amplification" in feat_type_lower or "deletion" in feat_type_lower:
        # Determine direction from alteration or feature_type string
        alt_type = None
        if "amplification" in feat_type_lower or attr.get("direction", "") == "Amplification":
            alt_type = "Amplification"
        elif "deletion" in feat_type_lower or "del" in feat_type_lower or attr.get("direction", "") in ("Deletion", "Deep Deletion"):
            alt_type = "Deletion"
        return "copy_number", alt_type

    if "fusion" in feat_type_lower or "rearrangement" in feat_type_lower:
        partner = attr.get("partner_gene") or attr.get("rearrangement_type")
        return "fusion", partner

    # Default: somatic variant
    return "somatic_variant", None


def _process_feature(f: dict) -> tuple:
    """Return (gene, alteration, feature_id, feature_type, alt_type, payload_json)."""
    attrs = f.get("attributes", [{}])
    attr = attrs[0] if attrs else {}
    gene = attr.get("gene") or f.get("gene")
    alt = attr.get("protein_change") or attr.get("alteration")
    if alt and alt.startswith("p."):
        alt = alt[2:]
    feature_type, alt_type = _infer_feature_type(f)
    return (gene, alt, f.get("feature_id"), feature_type, alt_type, json.dumps(f))


def refresh_moalmanac(conn) -> None:
    """Download MoAlmanac features + assertions into cache DB tables."""
    print("Refreshing MOAlmanac reference data...")
    with httpx.Client(timeout=60.0) as client:
        r_f = client.get(MOALMANAC_FEATURES_API)
        r_f.raise_for_status()
        features = r_f.json()

        r_a = client.get(MOALMANAC_ASSERTIONS_API)
        r_a.raise_for_status()
        assertions = r_a.json()

    # ── Features table ───────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS moalmanac_features_bulk (
            gene VARCHAR,
            alteration VARCHAR,
            feature_id INTEGER,
            feature_type VARCHAR,
            alt_type VARCHAR,
            payload JSON
        )
    """)
    # Non-destructive schema migration for existing tables missing new columns
    for col, coltype in [("feature_type", "VARCHAR"), ("alt_type", "VARCHAR")]:
        try:
            conn.execute(f"ALTER TABLE moalmanac_features_bulk ADD COLUMN IF NOT EXISTS {col} {coltype}")
        except Exception:
            pass
    conn.execute("DELETE FROM moalmanac_features_bulk")

    processed: dict[int, tuple] = {}
    for f in (features or []):
        processed[f.get("feature_id")] = _process_feature(f)

    # Also extract features only defined inside assertions
    for a in (assertions or []):
        for f in a.get("features", []):
            fid = f.get("feature_id")
            if fid not in processed:
                processed[fid] = _process_feature(f)

    if processed:
        conn.executemany(
            "INSERT INTO moalmanac_features_bulk (gene, alteration, feature_id, feature_type, alt_type, payload) VALUES (?, ?, ?, ?, ?, ?)",
            list(processed.values()),
        )

    # ── Assertions table ─────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS moalmanac_assertions_bulk (
            feature_id INTEGER,
            clinical_significance VARCHAR,
            drug VARCHAR,
            disease VARCHAR,
            score_bin VARCHAR,
            oncogenic VARCHAR,
            payload JSON
        )
    """)
    for col, coltype in [("score_bin", "VARCHAR"), ("oncogenic", "VARCHAR")]:
        try:
            conn.execute(f"ALTER TABLE moalmanac_assertions_bulk ADD COLUMN IF NOT EXISTS {col} {coltype}")
        except Exception:
            pass
    conn.execute("DELETE FROM moalmanac_assertions_bulk")

    if assertions:
        rows = []
        for a in assertions:
            significance = a.get("predictive_implication") or a.get("clinical_significance")
            drug = a.get("therapy_name") or a.get("drug")
            disease = a.get("oncotree_term") or a.get("disease")
            score_bin = a.get("score_bin") or a.get("predictive_implication")
            oncogenic = a.get("oncogenic")
            for f_brief in a.get("features", []):
                rows.append((
                    f_brief.get("feature_id"),
                    significance,
                    drug,
                    disease,
                    score_bin,
                    oncogenic,
                    json.dumps(a),
                ))
        conn.executemany(
            "INSERT INTO moalmanac_assertions_bulk (feature_id, clinical_significance, drug, disease, score_bin, oncogenic, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    # ── Status sentinel ──────────────────────────────────────────────────────
    conn.execute("CREATE TABLE IF NOT EXISTS moalmanac_status (last_refresh TIMESTAMP)")
    conn.execute("DELETE FROM moalmanac_status")
    conn.execute("INSERT INTO moalmanac_status VALUES (CURRENT_TIMESTAMP)")
    print("MOAlmanac reference data refreshed.")


def _needs_schema_migration(conn) -> bool:
    """Return True if moalmanac_features_bulk is missing the feature_type column."""
    try:
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'moalmanac_features_bulk'"
            ).fetchall()
        }
        return "feature_type" not in cols
    except Exception:
        return True


def ensure_moalmanac(conn) -> None:
    """Refresh MoAlmanac data if missing, schema-outdated, or older than TTL_DAYS."""
    if _needs_schema_migration(conn):
        refresh_moalmanac(conn)
        return
    try:
        row = conn.execute("SELECT last_refresh FROM moalmanac_status").fetchone()
        if row and (datetime.now() - row[0]) < timedelta(days=TTL_DAYS):
            return
    except Exception:
        pass
    refresh_moalmanac(conn)
