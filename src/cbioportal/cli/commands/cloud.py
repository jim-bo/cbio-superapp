"""cbio beta cloud — cloud pipeline commands (GCS sync, build, merge)."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from cbioportal.core.gcs import get_storage, get_staging_path

app = typer.Typer(help="Cloud pipeline: sync studies → build per-study DBs → merge to master")
console = Console()

_PER_STUDY_PREFIX = "per-study-dbs"
_MASTER_KEY = "master/cbioportal.duckdb"


@app.command()
def sync(
    force: bool = typer.Option(False, "--force", help="Re-download even if already in staging"),
    study_id: Annotated[list[str], typer.Option("--study-id", help="Sync specific study (repeatable)")] = [],
):
    """Download studies from cBioPortal.org into the staging area."""
    from cbioportal.core.pipeline.sync import sync_studies

    result = sync_studies(force=force, study_ids=list(study_id) or None)

    console.print(
        f"[green]Downloaded:[/green] {len(result.downloaded)}  "
        f"[dim]Skipped:[/dim] {len(result.skipped)}  "
        f"[red]Failed:[/red] {len(result.failed)}"
    )
    if result.failed:
        for sid in result.failed:
            console.print(f"  [red]FAILED[/red] {sid}")
        raise typer.Exit(code=1)


@app.command()
def build(
    study_id: str = typer.Option(..., "--study-id", envvar="STUDY_ID", help="Study ID to build"),
    mutations: bool = typer.Option(True, help="Include mutation data"),
    cna: bool = typer.Option(True, help="Include CNA data"),
    sv: bool = typer.Option(True, help="Include SV data"),
    timeline: bool = typer.Option(True, help="Include timeline data"),
    expression: bool = typer.Option(True, help="Include expression data"),
):
    """Build a per-study DuckDB from staging and upload to storage."""
    from cbioportal.core.pipeline.build import build_study_db

    storage = get_storage()
    staging = get_staging_path()
    build_study_db(
        study_id=study_id,
        staging_path=staging,
        storage=storage,
        load_mutations=mutations,
        load_cna=cna,
        load_sv=sv,
        load_timeline=timeline,
        load_expression=expression,
    )


@app.command("build-all")
def build_all(
    concurrency: int = typer.Option(1, help="Parallel builds (local only; cloud uses job fan-out)"),
    limit: int = typer.Option(None, help="Stop after N studies"),
    offset: int = typer.Option(0, help="Skip first N studies"),
    mutations: bool = typer.Option(True),
    cna: bool = typer.Option(True),
    sv: bool = typer.Option(True),
    timeline: bool = typer.Option(True),
    expression: bool = typer.Option(True),
):
    """Build per-study DuckDBs for all studies in staging."""
    from cbioportal.core.pipeline.build import build_study_db

    storage = get_storage()
    staging = get_staging_path()

    # Enumerate studies from staging directory.
    study_dirs = sorted(
        d for d in staging.iterdir() if d.is_dir()
    )[offset:]
    if limit:
        study_dirs = study_dirs[:limit]

    if not study_dirs:
        console.print("[yellow]No study directories found in staging.[/yellow]")
        raise typer.Exit(0)

    console.print(f"Building {len(study_dirs)} studies (concurrency={concurrency})...")

    if concurrency == 1:
        failed = []
        for study_dir in study_dirs:
            try:
                build_study_db(
                    study_id=study_dir.name,
                    staging_path=staging,
                    storage=storage,
                    load_mutations=mutations,
                    load_cna=cna,
                    load_sv=sv,
                    load_timeline=timeline,
                    load_expression=expression,
                )
            except Exception as exc:
                console.print(f"[red]FAILED {study_dir.name}: {exc}[/red]")
                failed.append(study_dir.name)
    else:
        import concurrent.futures

        failed = []

        def _build_one(study_dir: Path) -> tuple[str, Exception | None]:
            try:
                build_study_db(
                    study_id=study_dir.name,
                    staging_path=staging,
                    storage=storage,
                    load_mutations=mutations,
                    load_cna=cna,
                    load_sv=sv,
                    load_timeline=timeline,
                    load_expression=expression,
                )
                return study_dir.name, None
            except Exception as exc:
                return study_dir.name, exc

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            for sid, err in pool.map(_build_one, study_dirs):
                if err:
                    console.print(f"[red]FAILED {sid}: {err}[/red]")
                    failed.append(sid)

    if failed:
        console.print(f"[red]{len(failed)} build(s) failed.[/red]")
        raise typer.Exit(code=1)

    console.print(f"[green]All {len(study_dirs)} studies built successfully.[/green]")


@app.command()
def merge(
    no_backup: bool = typer.Option(False, "--no-backup", help="Skip writing backup"),
    study_id: Annotated[list[str], typer.Option("--study-id", help="Merge only these studies")] = [],
):
    """Merge all per-study DuckDBs into the master DB."""
    from cbioportal.core.pipeline.merge import merge_all_studies

    storage = get_storage()
    merge_all_studies(
        storage=storage,
        study_ids=list(study_id) or None,
        backup=not no_backup,
    )


@app.command()
def inject(
    study_id: str = typer.Option(..., "--study-id", envvar="STUDY_ID", help="Study ID to inject into master"),
    no_backup: bool = typer.Option(False, "--no-backup", help="Skip writing backup"),
):
    """Replace one study's tables in the existing master DB."""
    from cbioportal.core.pipeline.merge import inject_study

    storage = get_storage()
    inject_study(study_id=study_id, storage=storage, backup=not no_backup)


@app.command()
def status():
    """Show pipeline status: staging vs per-study-dbs vs master, plus backups."""
    storage = get_storage()
    staging = get_staging_path()

    # Studies in staging.
    staged = sorted(d.name for d in staging.iterdir() if d.is_dir()) if staging.exists() else []

    # Per-study DBs in storage.
    built_keys = storage.list_prefix(f"{_PER_STUDY_PREFIX}/")
    built = {Path(k).stem for k in built_keys if k.endswith(".duckdb")}

    # Backup list.
    backup_keys = storage.list_prefix("backups/")

    t = Table(title="Pipeline Status", show_header=True)
    t.add_column("Study ID")
    t.add_column("Staged", justify="center")
    t.add_column("Built", justify="center")

    all_ids = sorted(set(staged) | built)
    for sid in all_ids:
        t.add_row(
            sid,
            "[green]✓[/green]" if sid in staged else "[dim]-[/dim]",
            "[green]✓[/green]" if sid in built else "[dim]-[/dim]",
        )
    console.print(t)

    master_exists = storage.exists(_MASTER_KEY)
    console.print(
        f"\nMaster DB: {'[green]present[/green]' if master_exists else '[red]missing[/red]'}"
    )
    if backup_keys:
        console.print(f"Backups: {len(backup_keys)}")
        for bk in sorted(backup_keys)[-5:]:
            console.print(f"  {bk}")
