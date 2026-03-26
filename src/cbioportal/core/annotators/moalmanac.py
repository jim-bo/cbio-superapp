"""Molecular Almanac (MoAlmanac) variant annotator with bulk DuckDB caching.

DEPRECATED: This module is kept for backward compatibility with data_puller.py.
New code should use cbioportal.core.annotation.reference.moalmanac instead,
which adds feature_type/alt_type support for CNA and fusion matching.
"""
import httpx
import json
from datetime import datetime, timedelta

MOALMANAC_FEATURES_API = "https://moalmanac.org/api/features"


def _refresh_moalmanac_db(conn) -> None:
    """Download the entire MoAlmanac features AND assertions databases."""
    print("Refreshing local MoAlmanac database...")
    with httpx.Client(timeout=30.0) as client:
        # 1. Fetch Features
        r_f = client.get(MOALMANAC_FEATURES_API)
        r_f.raise_for_status()
        features = r_f.json()

        # 2. Fetch Assertions (The clinical evidence)
        r_a = client.get("https://moalmanac.org/api/assertions")
        r_a.raise_for_status()
        assertions = r_a.json()

    # Create/Clear the bulk features table
    conn.execute("CREATE TABLE IF NOT EXISTS moalmanac_features_bulk (gene VARCHAR, alteration VARCHAR, feature_id INTEGER, payload JSON)")
    conn.execute("DELETE FROM moalmanac_features_bulk")

    def _process_feat(f):
        attrs = f.get("attributes", [{}])[0]
        gene = attrs.get("gene")
        alt = attrs.get("protein_change")
        if alt and alt.startswith("p."):
            alt = alt[2:]
        return (gene, alt, f.get("feature_id"), json.dumps(f))

    processed_features = {} # feature_id -> tuple

    if features:
        for f in features:
            processed_features[f.get("feature_id")] = _process_feat(f)

    # ALSO extract features that are only defined inside assertions
    if assertions:
        for a in assertions:
            for f in a.get("features", []):
                f_id = f.get("feature_id")
                if f_id not in processed_features:
                    processed_features[f_id] = _process_feat(f)

    if processed_features:
        conn.executemany("INSERT INTO moalmanac_features_bulk (gene, alteration, feature_id, payload) VALUES (?, ?, ?, ?)", list(processed_features.values()))

    # Create/Clear the bulk assertions table
    conn.execute("CREATE TABLE IF NOT EXISTS moalmanac_assertions_bulk (feature_id INTEGER, clinical_significance VARCHAR, drug VARCHAR, disease VARCHAR, payload JSON)")
    conn.execute("DELETE FROM moalmanac_assertions_bulk")

    if assertions:
        assertion_tuples = []
        for a in assertions:
            # Assertions have a list of features they apply to
            feat_list = a.get("features", [])
            for f_brief in feat_list:
                f_id = f_brief.get("feature_id")
                # Extract clinical details
                significance = a.get("predictive_implication") or a.get("clinical_significance")
                drug = a.get("therapy_name") or a.get("drug")
                disease = a.get("oncotree_term") or a.get("disease")

                assertion_tuples.append((
                    f_id,
                    significance,
                    drug,
                    disease,
                    json.dumps(a)
                ))
        conn.executemany("INSERT INTO moalmanac_assertions_bulk (feature_id, clinical_significance, drug, disease, payload) VALUES (?, ?, ?, ?, ?)", assertion_tuples)
    # Track when we last refreshed
    conn.execute("CREATE TABLE IF NOT EXISTS moalmanac_status (last_refresh TIMESTAMP)")
    conn.execute("DELETE FROM moalmanac_status")
    conn.execute("INSERT INTO moalmanac_status VALUES (CURRENT_TIMESTAMP)")


def annotate_variants(conn, unique_variants: list[tuple[str, str]]) -> None:
    """
    Ensure all provided (gene, alteration) pairs are annotated via the bulk cache.
    Refreshes the bulk cache if it's older than 7 days.
    """
    # Check if we need to refresh the bulk features list (e.g., once a week)
    try:
        last_refresh = conn.execute("SELECT last_refresh FROM moalmanac_status").fetchone()
        if not last_refresh or (datetime.now() - last_refresh[0]) > timedelta(days=7):
            _refresh_moalmanac_db(conn)
    except Exception:
        _refresh_moalmanac_db(conn)

    # We don't actually need to do anything else here anymore because the final export query 
    # in data_puller.py will now JOIN against our moalmanac_features_bulk table.
    # However, to maintain the API contract, we can confirm the table exists.
    conn.execute("SELECT count(*) FROM moalmanac_features_bulk").fetchone()
