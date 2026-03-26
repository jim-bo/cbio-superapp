import typer
from pathlib import Path
from typing import Optional
from cbioportal.core import loader, database

app = typer.Typer(help="Database maintenance commands")

@app.command(name="load-all")
def load_all(
    limit: int = typer.Option(None, help="Maximum number of studies to load"),
    offset: int = typer.Option(0, help="Number of studies to skip before starting"),
    mutations: Optional[bool] = typer.Option(None, help="Load mutation data (default: True when no flags given)"),
    cna: Optional[bool] = typer.Option(None, help="Load CNA data (default: True when no flags given)"),
    sv: Optional[bool] = typer.Option(None, help="Load SV data (default: True when no flags given)"),
    timeline: Optional[bool] = typer.Option(None, help="Load timeline data (default: True when no flags given)"),
):
    """Load studies from the source directory into DuckDB."""
    source_path = loader.get_source_path()

    if not source_path:
        typer.echo("Error: Neither CBIO_DOWNLOADS nor CBIO_DATAHUB environment variables are set.")
        raise typer.Exit(code=1)

    load_all_types = all(x is None for x in [mutations, cna, sv, timeline])
    load_mutations = load_all_types or mutations is True
    load_cna       = load_all_types or cna is True
    load_sv        = load_all_types or sv is True
    load_timeline  = load_all_types or timeline is True

    conn = database.get_connection()
    typer.echo(f"Searching for studies in {source_path}...")

    loaded_count, metrics = loader.load_all_studies(
        conn, source_path, limit=limit, offset=offset, load_mutations=load_mutations, load_cna=load_cna, load_sv=load_sv, load_timeline=load_timeline
    )
    
    conn.close()
    
    typer.echo(f"\nSuccessfully loaded {loaded_count} studies.")
    typer.echo(f"Peak Memory: {metrics['peak_memory_mb']:.2f} MB")
    typer.echo(f"Total Time: {metrics['elapsed_seconds']:.2f} seconds")

import subprocess

@app.command()
def load_lfs(
    study_id: str,
    mutations: Optional[bool] = typer.Option(None, help="Load mutation data (default: True when no flags given)"),
    cna: Optional[bool] = typer.Option(None, help="Load CNA data (default: True when no flags given)"),
    sv: Optional[bool] = typer.Option(None, help="Load SV data (default: True when no flags given)"),
    timeline: Optional[bool] = typer.Option(None, help="Load timeline data (default: True when no flags given)"),
    keep_data: bool = typer.Option(False, help="Whether to keep the uncompressed data on disk after loading"),
):
    """Load an LFS-backed study by pulling, loading, and then hiding the data."""
    datahub_path = loader.get_source_path()
    if not datahub_path or "datahub" not in str(datahub_path).lower():
        typer.echo("Error: CBIO_DATAHUB must be set and point to a git repository.")
        raise typer.Exit(code=1)

    load_all_types = all(x is None for x in [mutations, cna, sv, timeline])
    load_mutations = load_all_types or mutations is True
    load_cna       = load_all_types or cna is True
    load_sv        = load_all_types or sv is True
    load_timeline  = load_all_types or timeline is True

    # 1. Pull data from LFS
    typer.echo(f"Pulling LFS data for {study_id}...")
    try:
        # We assume the study is in public/ for now, or we find it
        all_studies = loader.discover_studies(datahub_path)
        study_path = next((s for s in all_studies if s.name == study_id), None)
        if not study_path:
            typer.echo(f"Error: Study '{study_id}' not found.")
            return

        rel_path = study_path.relative_to(datahub_path)
        subprocess.run(["git", "lfs", "pull", "-I", f"{rel_path}/**"], cwd=datahub_path, check=True)

        # 2. Load into DuckDB
        typer.echo(f"Ingesting into DuckDB...")
        conn = database.get_connection()
        loader.ensure_gene_reference(conn)
        loader.load_study_metadata(conn, study_path)
        success = loader.load_study(conn, study_path, load_mutations=load_mutations, load_cna=load_cna, load_sv=load_sv, load_timeline=load_timeline)
        loader.create_global_views(conn)
        conn.close()
        
        # 3. Cleanup (Hide data)
        if not keep_data:
            typer.echo(f"Cleaning up uncompressed files...")
            subprocess.run(["git", "checkout", str(rel_path)], cwd=datahub_path, check=True)
            # Note: we don't prune LFS objects here to avoid slow re-downloads next time
            
        if success:
            typer.echo(f"Successfully loaded LFS study: {study_id}")
            
    except subprocess.CalledProcessError as e:
        typer.echo(f"Git LFS operation failed: {e}")
    except Exception as e:
        typer.echo(f"An error occurred: {e}")

@app.command()
def add(
    study_id: str,
    mutations: Optional[bool] = typer.Option(None, help="Load mutation data (default: True when no flags given)"),
    cna: Optional[bool] = typer.Option(None, help="Load CNA data (default: True when no flags given)"),
    sv: Optional[bool] = typer.Option(None, help="Load SV data (default: True when no flags given)"),
    timeline: Optional[bool] = typer.Option(None, help="Load timeline data (default: True when no flags given)"),
):
    """Add or update a single study by ID."""
    load_all_types = all(x is None for x in [mutations, cna, sv, timeline])
    load_mutations = load_all_types or mutations is True
    load_cna       = load_all_types or cna is True
    load_sv        = load_all_types or sv is True
    load_timeline  = load_all_types or timeline is True

    study_path = loader.find_study_path(study_id)
    if not study_path:
        typer.echo(f"Error: Study '{study_id}' not found in CBIO_DOWNLOADS or CBIO_DATAHUB.")
        raise typer.Exit(code=1)

    conn = database.get_connection()
    typer.echo(f"Loading study from {study_path}...")

    loader.ensure_gene_reference(conn)

    # 1. Metadata
    loader.load_study_metadata(conn, study_path)

    # 2. Data
    success = loader.load_study(conn, study_path, load_mutations=load_mutations, load_cna=load_cna, load_sv=load_sv, load_timeline=load_timeline)
    
    # 3. Refresh Views
    loader.create_global_views(conn)
    
    conn.close()
    
    if success:
        typer.echo(f"Successfully loaded study: {study_id}")
    else:
        typer.echo(f"Loaded metadata for {study_id}, but no clinical/genomic data found.")

@app.command()
def remove(study_id: str):
    """Remove a study and its tables from the database."""
    conn = database.get_connection()
    
    # Find all tables starting with this study_id
    tables_res = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name LIKE ?", (f"{study_id}_%",)).fetchall()
    tables = [t[0] for t in tables_res]
    
    if not tables:
        typer.echo(f"No tables found for study: {study_id}")
    else:
        for t in tables:
            conn.execute(f'DROP TABLE "{t}"')
        typer.echo(f"Dropped {len(tables)} tables for study: {study_id}")
    
    # Remove from metadata tables
    conn.execute("DELETE FROM studies WHERE study_id = ?", (study_id,))
    conn.execute("DELETE FROM study_data_types WHERE study_id = ?", (study_id,))
    
    # Refresh views
    loader.create_global_views(conn)
    conn.close()
    typer.echo(f"Successfully removed study: {study_id}")

@app.command(name="sync-oncotree")
def sync_oncotree():
    """Fetch latest OncoTree data and sync to the database."""
    conn = database.get_connection()
    loader.sync_oncotree(conn)
    conn.close()

@app.command(name="sync-gene-panels")
def sync_gene_panels(
    json_path: Optional[Path] = typer.Option(None, help="Path to gene-panels.json (defaults to auto-discovery from CBIO_DATAHUB)")
):
    """Load gene panel definitions from gene-panels.json into the database."""
    conn = database.get_connection()
    loader.load_gene_panel_definitions(conn, json_path)
    conn.close()

@app.command(name="sync-gene-reference")
def sync_gene_reference(
    genes_json_path: Optional[Path] = typer.Option(None, help="Path to genes.json (defaults to CBIO_DATAHUB/.circleci/portalinfo/genes.json)")
):
    """Load gene reference (entrezGeneId → hugoGeneSymbol) into the database."""
    conn = database.get_connection()
    loader.load_gene_reference(conn, genes_json_path)
    loader.populate_cytoband_from_hgnc(conn)
    conn.close()

@app.command(name="sync-gene-symbol-updates")
def sync_gene_symbol_updates(
    gene_update_md: Optional[Path] = typer.Option(None, help="Path to gene-update.md (defaults to CBIO_DATAHUB/seedDB/gene-update-list/gene-update.md)")
):
    """Load gene symbol update mappings (old alias → canonical) into the database."""
    conn = database.get_connection()
    loader.load_gene_symbol_updates(conn, gene_update_md)
    conn.close()

@app.command(name="sync-gene-aliases")
def sync_gene_aliases(
    seed_sql_path: Optional[Path] = typer.Option(None, help="Path to seed SQL gz (defaults to auto-discovery from CBIO_DATAHUB/seedDB/)")
):
    """Load gene alias mappings from seed SQL into the database."""
    conn = database.get_connection()
    loader.load_gene_aliases(conn, seed_sql_path)
    conn.close()

if __name__ == "__main__":
    app()
