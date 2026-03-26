"""Gene reference tables: Entrez→Hugo mapping, gene aliases, and OncoTree."""
import gzip
import json
import os
import re
import time
from pathlib import Path

import requests
import typer
from tqdm import tqdm

# ---------------------------------------------------------------------------
# URLs for web fallbacks
# ---------------------------------------------------------------------------
_CBIO_GENES_URL = "https://www.cbioportal.org/api/genes?pageSize=100000&projection=SUMMARY"
_HGNC_TSV_URL = "https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt"
_GENE_UPDATE_MD_URL = "https://raw.githubusercontent.com/cBioPortal/datahub/master/seedDB/gene-update-list/gene-update.md"
_GENE_PANELS_JSON_URL = "https://raw.githubusercontent.com/cBioPortal/datahub/master/.circleci/portalinfo/gene-panels.json"

_DATAHUB_CACHE_DIR = Path.home() / ".cbio" / "cache" / "datahub"
_CACHE_TTL_DAYS = 30


def _fetch_datahub_file(url: str, cache_name: str, ttl_days: int = _CACHE_TTL_DAYS) -> Path:
    """Download a reference file from the web, caching it locally with a TTL.

    Cache location: ~/.cbio/cache/datahub/<cache_name>
    If the cached file exists and is younger than ttl_days, return it immediately.
    Otherwise download from url with a tqdm progress bar and write to cache.
    """
    _DATAHUB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dest = _DATAHUB_CACHE_DIR / cache_name

    if dest.exists():
        age_days = (time.time() - dest.stat().st_mtime) / 86400
        if age_days < ttl_days:
            return dest

    typer.echo(f"Downloading {cache_name} from {url}...")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        desc=cache_name,
        total=total_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            size = f.write(chunk)
            bar.update(size)

    return dest


# ---------------------------------------------------------------------------
# OncoTree
# ---------------------------------------------------------------------------

def retrieve_oncotree_cancer_types():
    """Retrieve cancer types from OncoTree API."""
    request_url = 'http://oncotree.mskcc.org/api/tumorTypes/tree?version=oncotree_latest_stable'
    request_headers = {'Accept': 'application/json'}
    response = requests.get(url=request_url, headers=request_headers)
    response.raise_for_status()
    return response.json()


def flatten_oncotree(node, node_name, cancer_types):
    """Recursive function to flatten the JSON formatted cancer types."""
    type_of_cancer_id = node_name.lower()
    name = node['name']
    dedicated_color = node['color']
    short_name = node_name
    parent = node['parent'].lower()
    cancer_types.append((type_of_cancer_id, name, dedicated_color, short_name, parent))
    if 'children' in node and node['children']:
        for child_node_name, child_node in node['children'].items():
            flatten_oncotree(child_node, child_node_name, cancer_types)


def sync_oncotree(conn):
    """Fetch latest OncoTree data and sync to DuckDB."""
    typer.echo("Fetching OncoTree data from API...")
    data = retrieve_oncotree_cancer_types()
    cancer_types = []
    if 'TISSUE' in data:
        for child_name, child_node in data['TISSUE']['children'].items():
            flatten_oncotree(child_node, child_name, cancer_types)
    conn.execute("DROP TABLE IF EXISTS cancer_types")
    conn.execute("""
        CREATE TABLE cancer_types (
            type_of_cancer_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            dedicated_color VARCHAR,
            short_name VARCHAR,
            parent VARCHAR
        )
    """)
    conn.executemany("INSERT INTO cancer_types VALUES (?, ?, ?, ?, ?)", cancer_types)
    typer.echo(f"Successfully synced {len(cancer_types)} cancer types from OncoTree.")


def get_oncotree_root(conn, type_of_cancer_id: str):
    """Traverse up the OncoTree to find the root organ node (parent is 'tissue')."""
    if not type_of_cancer_id:
        return "Other"
    current_id = type_of_cancer_id.lower()
    for _ in range(10):
        res = conn.execute("SELECT name, parent FROM cancer_types WHERE type_of_cancer_id = ?", (current_id,)).fetchone()
        if not res:
            return current_id.capitalize()
        name, parent = res
        if parent == "tissue":
            return name
        current_id = parent
    return "Other"


# ---------------------------------------------------------------------------
# Gene reference (Entrez → Hugo)
# ---------------------------------------------------------------------------

def load_gene_reference(conn, genes_json_path: Path = None):
    """Load gene reference table (entrezGeneId → hugoGeneSymbol).

    Resolution order:
      1. Explicit genes_json_path argument
      2. $CBIO_DATAHUB/.circleci/portalinfo/genes.json
      3. cBioPortal public API (cached 30 days in ~/.cbio/cache/datahub/genes.json)
    """
    if genes_json_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if datahub:
            genes_json_path = Path(datahub) / ".circleci" / "portalinfo" / "genes.json"
        else:
            genes_json_path = _fetch_datahub_file(_CBIO_GENES_URL, "genes.json")

    if not genes_json_path.exists():
        typer.echo(f"Error: genes.json not found at {genes_json_path}")
        raise typer.Exit(code=1)

    typer.echo(f"Loading gene reference from {genes_json_path}...")
    with open(genes_json_path, "r") as f:
        genes = json.load(f)

    conn.execute("DROP TABLE IF EXISTS gene_reference")
    conn.execute("""
        CREATE TABLE gene_reference (
            entrez_gene_id INTEGER PRIMARY KEY,
            hugo_gene_symbol VARCHAR,
            gene_type VARCHAR,
            cytoband VARCHAR DEFAULT ''
        )
    """)

    rows = [
        (g["entrezGeneId"], g["hugoGeneSymbol"], g.get("type"), "")
        for g in genes
        if g.get("entrezGeneId") is not None
    ]
    conn.executemany("INSERT OR REPLACE INTO gene_reference VALUES (?, ?, ?, ?)", rows)
    typer.echo(f"Successfully loaded {len(rows)} gene reference entries.")


# ---------------------------------------------------------------------------
# Gene symbol updates (~75 renames)
# ---------------------------------------------------------------------------

def load_gene_symbol_updates(conn, gene_update_md: Path = None):
    """Parse gene-update.md and populate gene_symbol_updates table.

    Resolution order:
      1. Explicit gene_update_md argument
      2. $CBIO_DATAHUB/seedDB/gene-update-list/gene-update.md
      3. GitHub raw URL (cached 30 days in ~/.cbio/cache/datahub/gene-update.md)
    """
    if gene_update_md is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if datahub:
            gene_update_md = Path(datahub) / "seedDB" / "gene-update-list" / "gene-update.md"
        else:
            gene_update_md = _fetch_datahub_file(_GENE_UPDATE_MD_URL, "gene-update.md")

    if not gene_update_md.exists():
        typer.echo(f"Warning: gene-update.md not found at {gene_update_md}, skipping.")
        return

    conn.execute("DROP TABLE IF EXISTS gene_symbol_updates")
    conn.execute("""
        CREATE TABLE gene_symbol_updates (
            old_symbol VARCHAR PRIMARY KEY,
            new_symbol VARCHAR
        )
    """)

    pattern = re.compile(r'^(\S+)\s+-?\d+\s+->\s+(\S+)\s+-?\d+')
    rows = {}
    in_code_block = False
    with open(gene_update_md, "r") as f:
        for line in f:
            line = line.rstrip()
            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                m = pattern.match(line.strip())
                if m:
                    old_sym, new_sym = m.group(1), m.group(2)
                    if old_sym != new_sym:
                        rows[old_sym] = new_sym

    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO gene_symbol_updates VALUES (?, ?)",
            list(rows.items())
        )
    typer.echo(f"Loaded {len(rows)} gene symbol update entries.")


# ---------------------------------------------------------------------------
# Gene aliases (~55k rows, from HGNC)
# ---------------------------------------------------------------------------

def load_gene_aliases(conn, seed_sql_path: Path = None):
    """Load gene alias table.

    Resolution order:
      1. Explicit seed_sql_path argument (original seed SQL format, preserved for compat)
      2. $CBIO_DATAHUB/seedDB/seed-cbioportal_hg19_hg38_*.sql.gz (LFS)
      3. HGNC official TSV download (cached 30 days in ~/.cbio/cache/datahub/hgnc_complete_set.txt)

    The HGNC TSV is the upstream source that the cBioPortal seed SQL is built from.
    It provides alias_symbol and prev_symbol columns, which map directly to the
    gene_alias table. Rows with no entrez_id are skipped.

    Key mappings resolved:
      MLL  → KMT2A (Entrez 4297)
      MLL2 → KMT2D (Entrez 8085)
      MLL3 → KMT2C (Entrez 58508)
      MLL4 → KMT2B (Entrez 9757)
    """
    if seed_sql_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if datahub:
            seed_dir = Path(datahub) / "seedDB"
            candidates = sorted(seed_dir.glob("seed-cbioportal_hg19_hg38_*.sql.gz"))
            if candidates:
                seed_sql_path = candidates[-1]

    conn.execute("DROP TABLE IF EXISTS gene_alias")
    conn.execute("""
        CREATE TABLE gene_alias (
            entrez_gene_id INTEGER,
            alias_symbol VARCHAR,
            PRIMARY KEY (entrez_gene_id, alias_symbol)
        )
    """)

    if seed_sql_path is not None and seed_sql_path.exists():
        _load_gene_aliases_from_sql(conn, seed_sql_path)
    else:
        if seed_sql_path is not None:
            typer.echo(f"Warning: seed SQL not found at {seed_sql_path}. Falling back to HGNC TSV.")
        _load_gene_aliases_from_hgnc(conn)


def _load_gene_aliases_from_sql(conn, seed_sql_path: Path):
    """Load gene aliases from cBioPortal seed SQL (original format)."""
    typer.echo(f"Loading gene aliases from {seed_sql_path.name}...")
    insert_re = re.compile(r"INSERT INTO `gene_alias` VALUES (.+);")
    pair_re = re.compile(r"\((\d+),'([^']+)'\)")

    alias_rows: list[tuple[int, str]] = []
    opener = gzip.open if seed_sql_path.suffix == ".gz" else open
    with opener(seed_sql_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = insert_re.match(line.strip())
            if m:
                for entrez_str, alias in pair_re.findall(m.group(1)):
                    alias_rows.append((int(entrez_str), alias))

    conn.executemany("INSERT OR REPLACE INTO gene_alias VALUES (?, ?)", alias_rows)
    typer.echo(f"Loaded {len(alias_rows)} gene alias entries.")


def _load_gene_aliases_from_hgnc(conn):
    """Load gene aliases from HGNC official TSV download."""
    hgnc_path = _fetch_datahub_file(_HGNC_TSV_URL, "hgnc_complete_set.txt")
    typer.echo("Parsing HGNC gene aliases...")

    alias_rows: list[tuple[int, str]] = []
    cytoband_rows: list[tuple[str, int]] = []  # (location, entrez_id)

    with open(hgnc_path, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        try:
            entrez_col = header.index("entrez_id")
            alias_col = header.index("alias_symbol")
            prev_col = header.index("prev_symbol")
        except ValueError as e:
            raise RuntimeError(f"HGNC TSV missing expected column: {e}") from e

        location_col = header.index("location") if "location" in header else None

        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) <= max(entrez_col, alias_col, prev_col):
                continue
            entrez_raw = fields[entrez_col].strip()
            if not entrez_raw:
                continue
            try:
                entrez_id = int(entrez_raw)
            except ValueError:
                continue

            # alias_symbol: pipe-separated list, e.g. "MLL|MLL1A"
            for symbol in fields[alias_col].split("|"):
                symbol = symbol.strip()
                if symbol:
                    alias_rows.append((entrez_id, symbol))

            # prev_symbol: pipe-separated list of historical names
            for symbol in fields[prev_col].split("|"):
                symbol = symbol.strip()
                if symbol:
                    alias_rows.append((entrez_id, symbol))

            # location: cytogenetic band, e.g. "17p13.1"
            if location_col is not None and len(fields) > location_col:
                loc = fields[location_col].strip()
                if loc and loc != "not applicable":
                    cytoband_rows.append((loc, entrez_id))

    if alias_rows:
        conn.executemany("INSERT OR REPLACE INTO gene_alias VALUES (?, ?)", alias_rows)
    typer.echo(f"Loaded {len(alias_rows)} gene alias entries from HGNC.")

    if cytoband_rows:
        conn.execute("CREATE TEMP TABLE _tmp_cytoband (cytoband VARCHAR, entrez_gene_id INTEGER)")
        conn.executemany("INSERT INTO _tmp_cytoband VALUES (?, ?)", cytoband_rows)
        conn.execute("""
            UPDATE gene_reference SET cytoband = t.cytoband
            FROM _tmp_cytoband t
            WHERE gene_reference.entrez_gene_id = t.entrez_gene_id
        """)
        conn.execute("DROP TABLE _tmp_cytoband")
        typer.echo(f"Updated {len(cytoband_rows)} gene cytoband entries from HGNC.")


# ---------------------------------------------------------------------------
# Gene panel definitions
# ---------------------------------------------------------------------------

def load_gene_panel_definitions(conn, json_path: Path = None):
    """Load gene panel definitions from gene-panels.json.

    Resolution order:
      1. Explicit json_path argument
      2. $CBIO_DATAHUB/.circleci/portalinfo/gene-panels.json
      3. GitHub raw URL (cached 30 days in ~/.cbio/cache/datahub/gene-panels.json)
    """
    if json_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if datahub:
            json_path = Path(datahub) / ".circleci" / "portalinfo" / "gene-panels.json"
        else:
            json_path = _fetch_datahub_file(_GENE_PANELS_JSON_URL, "gene-panels.json")

    if not json_path.exists():
        typer.echo(f"Error: Gene panels JSON not found at {json_path}")
        raise typer.Exit(code=1)

    typer.echo(f"Loading gene panel definitions from {json_path}...")
    with open(json_path, "r") as f:
        panels = json.load(f)

    conn.execute("DROP TABLE IF EXISTS gene_panel_definitions")
    conn.execute("""
        CREATE TABLE gene_panel_definitions (
            panel_id VARCHAR,
            description VARCHAR,
            hugo_gene_symbol VARCHAR,
            entrez_gene_id INTEGER,
            PRIMARY KEY (panel_id, hugo_gene_symbol)
        )
    """)

    rows = []
    for panel in panels:
        panel_id = panel["genePanelId"]
        description = panel.get("description", "")
        for gene in panel.get("genes", []):
            rows.append((panel_id, description, gene["hugoGeneSymbol"], gene.get("entrezGeneId")))

    conn.executemany("INSERT OR REPLACE INTO gene_panel_definitions VALUES (?, ?, ?, ?)", rows)
    panel_count = len(panels)
    gene_count = len(rows)
    typer.echo(f"Successfully loaded {panel_count} gene panels ({gene_count} gene entries).")


# ---------------------------------------------------------------------------
# Cytoband population from HGNC
# ---------------------------------------------------------------------------

def populate_cytoband_from_hgnc(conn):
    """Populate cytoband column in gene_reference from HGNC location data.

    This runs independently of alias loading — even when aliases come from
    the seed SQL, cytoband data still comes from HGNC's `location` column.
    """
    hgnc_path = _fetch_datahub_file(_HGNC_TSV_URL, "hgnc_complete_set.txt")
    typer.echo("Populating cytoband from HGNC location data...")

    cytoband_rows: list[tuple[str, int]] = []

    with open(hgnc_path, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        try:
            entrez_col = header.index("entrez_id")
            location_col = header.index("location")
        except ValueError:
            typer.echo("Warning: HGNC TSV missing entrez_id or location column, skipping cytoband.")
            return

        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) <= max(entrez_col, location_col):
                continue
            entrez_raw = fields[entrez_col].strip()
            if not entrez_raw:
                continue
            try:
                entrez_id = int(entrez_raw)
            except ValueError:
                continue

            loc = fields[location_col].strip()
            if loc and loc != "not applicable":
                cytoband_rows.append((loc, entrez_id))

    if cytoband_rows:
        # Batch update via temp table to avoid executemany overhead on large UPDATE
        conn.execute("CREATE TEMP TABLE _tmp_cytoband (cytoband VARCHAR, entrez_gene_id INTEGER)")
        conn.executemany("INSERT INTO _tmp_cytoband VALUES (?, ?)", cytoband_rows)
        conn.execute("""
            UPDATE gene_reference SET cytoband = t.cytoband
            FROM _tmp_cytoband t
            WHERE gene_reference.entrez_gene_id = t.entrez_gene_id
        """)
        conn.execute("DROP TABLE _tmp_cytoband")
    typer.echo(f"Updated {len(cytoband_rows)} gene cytoband entries from HGNC.")


# ---------------------------------------------------------------------------
# Auto-load on study import
# ---------------------------------------------------------------------------

def ensure_gene_reference(conn):
    """Auto-load gene reference tables if not already present.

    Called before every study load to ensure normalize_hugo_symbols() has the
    data it needs. Uses web fallbacks when CBIO_DATAHUB is not set, so this
    works from a clean checkout with only internet access.

    Each table is checked independently so adding gene_alias later doesn't
    require re-loading gene_reference.
    """
    existing = {t[0] for t in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    try:
        if "gene_reference" not in existing:
            load_gene_reference(conn)
        if "gene_symbol_updates" not in existing:
            load_gene_symbol_updates(conn)
        if "gene_alias" not in existing:
            load_gene_aliases(conn)
        # Populate cytoband if gene_reference exists but cytoband is empty
        try:
            has_cytoband = conn.execute(
                "SELECT COUNT(*) FROM gene_reference WHERE cytoband IS NOT NULL AND cytoband != ''"
            ).fetchone()[0]
            if has_cytoband == 0:
                populate_cytoband_from_hgnc(conn)
        except Exception:
            pass
    except SystemExit:
        typer.echo("Warning: Could not load gene reference tables. Hugo symbol normalization will be skipped.")
    except Exception as e:
        typer.echo(f"Warning: Could not load gene reference tables: {e}. Hugo symbol normalization will be skipped.")
