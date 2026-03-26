"""CIViC (Clinical Interpretation of Variants in Cancer) reference data loader.

Downloads nightly TSV from CIViC, normalizes hgvsp_short to 1-letter amino acid
codes, and loads into civic_evidence table in the cache DB.

CIViC nightly TSV columns (as of 2025):
    molecular_profile, molecular_profile_id, disease, doid, phenotypes,
    therapies, therapy_interaction_type, evidence_type, evidence_direction,
    evidence_level, significance, evidence_statement, citation_id, source_type,
    asco_abstract_id, citation, nct_ids, rating, evidence_status, evidence_id,
    variant_origin, last_review_date, evidence_civic_url, molecular_profile_civic_url,
    is_flagged
"""
from __future__ import annotations

import io
import re
from datetime import datetime, timedelta

import httpx

CIVIC_NIGHTLY_URL = (
    "https://civicdb.org/downloads/nightly/nightly-ClinicalEvidenceSummaries.tsv"
)
TTL_DAYS = 7

# Three-letter to one-letter amino acid mapping
AA3_TO_1: dict[str, str] = {
    "Ala": "A", "Arg": "R", "Asn": "N", "Asp": "D", "Cys": "C",
    "Glu": "E", "Gln": "Q", "Gly": "G", "His": "H", "Ile": "I",
    "Leu": "L", "Lys": "K", "Met": "M", "Phe": "F", "Pro": "P",
    "Ser": "S", "Thr": "T", "Trp": "W", "Tyr": "Y", "Val": "V",
    "Ter": "*", "Stop": "*",
}

_AA3_PAT = re.compile(r"(" + "|".join(AA3_TO_1.keys()) + r")")

# Matches a variant name that looks like a protein change (e.g. V600E, G12D, R175H)
_PROTEIN_CHANGE_PAT = re.compile(r"^[A-Z]\d+[A-Z*]")


def _normalize_hgvsp(variant_name: str) -> str | None:
    """Convert 3-letter or mixed amino acid notation to 1-letter p.X#Y form.

    Examples:
        Val600Glu  -> p.V600E
        p.V600E    -> p.V600E
        G12D       -> p.G12D
        Gly12Asp   -> p.G12D
    """
    if not variant_name:
        return None
    s = variant_name.strip()
    # Convert 3-letter codes to 1-letter
    s = _AA3_PAT.sub(lambda m: AA3_TO_1[m.group(0)], s)
    # Strip leading "p." if present then re-add
    s = re.sub(r"^p\.", "", s)
    # Sanity check: must look like a protein change (letter + digit)
    if not re.match(r"^[A-Z*]\d", s):
        return None
    return f"p.{s}"


def _parse_molecular_profile(mp: str) -> tuple[str, str | None]:
    """Parse CIViC molecular_profile field into (gene, hgvsp_short).

    Examples:
        "BRAF V600E"    -> ("BRAF", "p.V600E")
        "JAK2 V617F"    -> ("JAK2", "p.V617F")
        "TP53 R175H"    -> ("TP53", "p.R175H")
        "KRAS G12D"     -> ("KRAS", "p.G12D")
        "MYC AMPLIFICATION" -> ("MYC", None)
        "BCR::ABL1 e13a2/b2a2" -> ("BCR", None)  # fusion, skip
    """
    if not mp or "::" in mp:
        return "", None
    parts = mp.strip().split(None, 1)
    gene = parts[0] if parts else ""
    variant = parts[1] if len(parts) > 1 else None
    hgvsp = _normalize_hgvsp(variant) if variant else None
    return gene, hgvsp


def _create_tables(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS civic_evidence (
            evidence_id INTEGER PRIMARY KEY,
            gene VARCHAR,
            variant_name VARCHAR,
            hgvsp_short VARCHAR,
            evidence_type VARCHAR,
            clinical_significance VARCHAR,
            evidence_level VARCHAR,
            drugs VARCHAR,
            disease VARCHAR,
            oncotree_code VARCHAR,
            fetched_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS civic_status (
            last_refresh TIMESTAMP
        )
    """)


def refresh_civic(conn) -> None:
    """Download CIViC nightly TSV and populate civic_evidence table."""
    print("Refreshing CIViC reference data...")
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        r = client.get(CIVIC_NIGHTLY_URL)
        r.raise_for_status()
        tsv_text = r.text

    _create_tables(conn)
    conn.execute("DELETE FROM civic_evidence")

    now = datetime.now().replace(microsecond=0)
    rows = []
    reader = io.StringIO(tsv_text)
    header = None
    for line in reader:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        if header is None:
            header = [h.lower().strip() for h in parts]
            continue
        if len(parts) < len(header):
            parts += [""] * (len(header) - len(parts))
        row = dict(zip(header, parts))

        # Parse evidence_id
        try:
            eid = int(row.get("evidence_id") or "0")
        except ValueError:
            continue
        if eid == 0:
            continue

        # Parse gene + variant from molecular_profile
        mp = row.get("molecular_profile", "") or ""
        gene, hgvsp = _parse_molecular_profile(mp)
        # Also keep the variant part as variant_name
        mp_parts = mp.strip().split(None, 1)
        variant_name = mp_parts[1] if len(mp_parts) > 1 else ""

        ev_type = row.get("evidence_type", "")
        # CIViC uses 'significance' not 'clinical_significance'
        significance = row.get("significance") or row.get("clinical_significance") or ""
        level = row.get("evidence_level", "")
        drugs = row.get("therapies") or row.get("drugs") or row.get("therapy_name") or ""
        disease = row.get("disease") or row.get("disease_name") or ""
        # doid can serve as an ontology code; CIViC doesn't expose oncotree directly
        oncotree = row.get("doid") or ""

        rows.append((eid, gene, variant_name, hgvsp, ev_type, significance, level, drugs.strip(), disease, oncotree, now))

    if rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO civic_evidence
            (evidence_id, gene, variant_name, hgvsp_short, evidence_type,
             clinical_significance, evidence_level, drugs, disease, oncotree_code, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    conn.execute("DELETE FROM civic_status")
    conn.execute("INSERT INTO civic_status VALUES (CURRENT_TIMESTAMP)")
    print(f"CIViC: loaded {len(rows)} evidence records.")


def ensure_civic(conn) -> None:
    """Refresh CIViC data if missing or older than TTL_DAYS."""
    _create_tables(conn)
    try:
        row = conn.execute("SELECT last_refresh FROM civic_status").fetchone()
        if row and (datetime.now() - row[0]) < timedelta(days=TTL_DAYS):
            return
    except Exception:
        pass
    refresh_civic(conn)
