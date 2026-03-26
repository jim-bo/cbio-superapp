"""IntOGen driver gene reference data loader.

Downloads the IntOGen Compendium_Cancer_Genes.tsv from the release ZIP,
maps IntOGen tumor-type codes to OncoTree codes, and loads into
intogen_drivers table in the cache DB.
"""
from __future__ import annotations

import io
import zipfile
from datetime import datetime, timedelta

import httpx

INTOGEN_ZIP_URL = "https://www.intogen.org/download?file=intogen_drivers-2023.1.zip"
COMPENDIUM_FILE = "Compendium_Cancer_Genes.tsv"
TTL_DAYS = 30

# Static mapping: IntOGen TUMOR_TYPE → OncoTree code(s)
# Covers ~30 divergent cases. Unmapped types remain as-is.
INTOGEN_TO_ONCOTREE: dict[str, str] = {
    "LAML": "AML",
    "LIHC": "HCC",
    "DLBC": "DLBCL",
    "GBM": "GBM",
    "KIRC": "CCRCC",
    "KIRP": "PRCC",
    "KICH": "CHRCC",
    "LGG": "DIFG",
    "LUAD": "LUAD",
    "LUSC": "LUSC",
    "OV": "HGSOC",
    "STAD": "STAD",
    "SKCM": "SKCM",
    "COAD": "COAD",
    "READ": "READ",
    "PRAD": "PRAD",
    "BRCA": "BRCA",
    "HNSC": "HNSC",
    "BLCA": "BLCA",
    "UCEC": "UCEC",
    "CESC": "CESC",
    "THCA": "THCA",
    "LICA": "HCC",       # Liver Cancer → HCC
    "ESAD": "EAC",       # Esophageal Adenocarcinoma
    "ESSC": "ESCA",      # Esophageal SCC
    "MELA": "MEL",       # Melanoma
    "NACA": "PAAD",      # Pancreatic
    "PAAD": "PAAD",
    "CM": "MEL",
    "ALL": "ALL",
    "CLL": "CLL",
    "MM": "MM",
    "LYMPH_NOS": "BCL",
}


def _map_tumor_type(intogen_type: str) -> str:
    """Map IntOGen tumor type to OncoTree code (best effort)."""
    return INTOGEN_TO_ONCOTREE.get(intogen_type, intogen_type)


def _create_tables(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intogen_drivers (
            symbol VARCHAR,
            tumor_type VARCHAR,
            oncotree_code VARCHAR,
            role VARCHAR,
            methods VARCHAR,
            qvalue_combination DOUBLE,
            fetched_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intogen_status (
            last_refresh TIMESTAMP
        )
    """)


def refresh_intogen(conn) -> None:
    """Download IntOGen ZIP and load drivers into intogen_drivers table."""
    print("Refreshing IntOGen reference data...", flush=True)
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        r = client.get(INTOGEN_ZIP_URL)
        r.raise_for_status()
        zip_bytes = r.content

    _create_tables(conn)
    conn.execute("DELETE FROM intogen_drivers")

    rows = []
    now = datetime.now().replace(microsecond=0)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Find the compendium file (may be nested in a subdirectory)
        target = None
        for name in zf.namelist():
            if name.endswith(COMPENDIUM_FILE):
                target = name
                break
        if target is None:
            raise FileNotFoundError(f"{COMPENDIUM_FILE} not found in IntOGen ZIP")

        with zf.open(target) as f:
            text = f.read().decode("utf-8")

    reader = io.StringIO(text)
    header = None
    for line in reader:
        line = line.rstrip("\n")
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if header is None:
            header = [h.strip() for h in parts]
            continue
        if len(parts) < 3:
            continue
        row = dict(zip(header, parts))

        symbol = row.get("SYMBOL") or row.get("gene") or ""
        tumor_type = row.get("TUMOR_TYPE") or row.get("cancer_type") or ""
        role = row.get("ROLE") or row.get("role") or ""
        methods = row.get("METHODS") or row.get("methods") or ""
        try:
            qval = float(row.get("QVALUE_COMBINATION") or row.get("qvalue") or "1.0")
        except ValueError:
            qval = 1.0

        oncotree = _map_tumor_type(tumor_type)
        rows.append((symbol, tumor_type, oncotree, role, methods, qval, now))

    if rows:
        conn.executemany(
            """
            INSERT INTO intogen_drivers
            (symbol, tumor_type, oncotree_code, role, methods, qvalue_combination, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    conn.execute("DELETE FROM intogen_status")
    conn.execute("INSERT INTO intogen_status VALUES (CURRENT_TIMESTAMP)")
    print(f"IntOGen: loaded {len(rows)} driver gene records.")


def ensure_intogen(conn) -> None:
    """Refresh IntOGen data if missing or older than TTL_DAYS."""
    _create_tables(conn)
    try:
        row = conn.execute("SELECT last_refresh FROM intogen_status").fetchone()
        if row and (datetime.now() - row[0]) < timedelta(days=TTL_DAYS):
            return
    except Exception:
        pass
    refresh_intogen(conn)
