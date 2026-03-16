"""Gene reference tables: Entrez→Hugo mapping, gene aliases, and OncoTree."""
import gzip
import json
import os
import re
from pathlib import Path

import requests
import typer


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


def load_gene_reference(conn, genes_json_path: Path = None):
    """Load gene reference table from genes.json (entrezGeneId → hugoGeneSymbol).

    Source: $CBIO_DATAHUB/.circleci/portalinfo/genes.json (from cBioPortal/datahub repo).
    Maps Entrez Gene ID → canonical HGNC Hugo symbol.
    Required for Pass 1 of Hugo symbol normalization — the most accurate pass because
    Entrez IDs are stable identifiers even when the Hugo symbol changes.
    """
    if genes_json_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no path provided.")
            raise typer.Exit(code=1)
        genes_json_path = Path(datahub) / ".circleci" / "portalinfo" / "genes.json"

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
            gene_type VARCHAR
        )
    """)

    rows = [
        (g["entrezGeneId"], g["hugoGeneSymbol"], g.get("type"))
        for g in genes
        if g.get("entrezGeneId") is not None
    ]
    conn.executemany("INSERT OR REPLACE INTO gene_reference VALUES (?, ?, ?)", rows)
    typer.echo(f"Successfully loaded {len(rows)} gene reference entries.")


def load_gene_symbol_updates(conn, gene_update_md: Path = None):
    """Parse gene-update.md and populate gene_symbol_updates table.

    Source: $CBIO_DATAHUB/seedDB/gene-update-list/gene-update.md
    Covers genes that were *renamed* (not just aliased) — e.g., C10ORF12 → LCOR.
    Only ~75 entries; does NOT cover KMT2 family aliases (those need gene_alias).
    Used as Pass 2 fallback when Entrez ID is wrong or missing.
    """
    if gene_update_md is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no path provided.")
            raise typer.Exit(code=1)
        gene_update_md = Path(datahub) / "seedDB" / "gene-update-list" / "gene-update.md"

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


def load_gene_aliases(conn, seed_sql_path: Path = None):
    """Extract gene_alias table from seed SQL and create alias→canonical mapping.

    Solves the problem of studies with Entrez_Gene_Id=0 for genes that have been renamed.
    For example, msk_chord_2024 ships MLL2/MLL3/MLL/MLL4 with Entrez_Gene_Id=0; these are
    historical aliases that HGNC renamed to KMT2D/KMT2C/KMT2A/KMT2B respectively.
    gene_reference won't match them (no valid Entrez ID); gene_symbol_updates doesn't
    have them either. This table provides the bridge via NCBI alias records.

    Source: $CBIO_DATAHUB/seedDB/seed-cbioportal_hg19_hg38_*.sql.gz — the gene_alias table.
    Contains ~55k alias entries. Key mappings:
      MLL  → KMT2A (Entrez 4297)
      MLL2 → KMT2D (Entrez 8085)
      MLL3 → KMT2C (Entrez 58508)
      MLL4 → KMT2B (Entrez 9757)
    Used as Pass 3 fallback after Entrez ID and symbol-update passes.
    """
    if seed_sql_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no path provided.")
            raise typer.Exit(code=1)
        seed_dir = Path(datahub) / "seedDB"
        candidates = sorted(seed_dir.glob("seed-cbioportal_hg19_hg38_*.sql.gz"))
        if not candidates:
            typer.echo(f"Warning: No seed SQL file found in {seed_dir}. Gene alias normalization will be skipped.")
            return
        seed_sql_path = candidates[-1]

    if not seed_sql_path.exists():
        typer.echo(f"Warning: seed SQL not found at {seed_sql_path}. Gene alias normalization will be skipped.")
        return

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

    conn.execute("DROP TABLE IF EXISTS gene_alias")
    conn.execute("""
        CREATE TABLE gene_alias (
            entrez_gene_id INTEGER,
            alias_symbol VARCHAR,
            PRIMARY KEY (entrez_gene_id, alias_symbol)
        )
    """)
    conn.executemany("INSERT OR REPLACE INTO gene_alias VALUES (?, ?)", alias_rows)
    typer.echo(f"Loaded {len(alias_rows)} gene alias entries.")


def load_gene_panel_definitions(conn, json_path: Path = None):
    """Load gene panel definitions from gene-panels.json into DuckDB."""
    if json_path is None:
        datahub = os.getenv("CBIO_DATAHUB")
        if not datahub:
            typer.echo("Error: CBIO_DATAHUB env var not set and no --json-path provided.")
            raise typer.Exit(code=1)
        json_path = Path(datahub) / ".circleci" / "portalinfo" / "gene-panels.json"

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


def ensure_gene_reference(conn):
    """Auto-load gene reference tables when CBIO_DATAHUB is set, if not already present.

    Called before every study load (db add, db load-lfs, db load-all) to ensure
    normalize_hugo_symbols() has the data it needs. Each table is checked independently
    so adding gene_alias later doesn't require re-loading gene_reference.

    IMPORTANT: If CBIO_DATAHUB is not set, normalization is silently skipped. Gene counts
    will then be wrong for studies using legacy aliases (e.g. KMT2 family genes).
    """
    datahub = os.getenv("CBIO_DATAHUB")
    if not datahub:
        return
    existing = {t[0] for t in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    try:
        if "gene_reference" not in existing:
            load_gene_reference(conn, Path(datahub) / ".circleci" / "portalinfo" / "genes.json")
        if "gene_symbol_updates" not in existing:
            load_gene_symbol_updates(conn, Path(datahub) / "seedDB" / "gene-update-list" / "gene-update.md")
        if "gene_alias" not in existing:
            load_gene_aliases(conn)
    except SystemExit:
        typer.echo("Warning: Could not load gene reference tables. Hugo symbol normalization will be skipped.")
    except Exception as e:
        typer.echo(f"Warning: Could not load gene reference tables: {e}. Hugo symbol normalization will be skipped.")
