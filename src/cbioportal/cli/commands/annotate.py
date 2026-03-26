"""cbio annotate — run the variant annotation pipeline for one or all studies.

This module exposes an `annotate` function that is registered directly as
`cbio annotate` in main.py. It does NOT use a sub-Typer app so that positional
arguments and options can be freely interleaved (Typer limitation with add_typer
callbacks).
"""
from __future__ import annotations

from typing import Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from cbioportal.core import database
from cbioportal.core.annotation import annotate_study, refresh_reference_data

load_dotenv()

console = Console()


def annotate(
    study_id: Optional[str] = typer.Argument(None, help="Study ID to annotate"),
    all_studies: bool = typer.Option(False, "--all", help="Annotate all loaded studies"),
    force: bool = typer.Option(False, "--force", help="Rebuild even if already annotated"),
    skip_vibe_vep: bool = typer.Option(False, "--skip-vibe-vep", help="Skip vibe-vep annotation"),
    refresh_refs: bool = typer.Option(
        False, "--refresh-refs", help="Force re-download all reference data before annotating"
    ),
) -> None:
    """Annotate variants in one or all studies with functional and clinical evidence."""
    if not study_id and not all_studies:
        console.print("[bold red]Error:[/bold red] Provide a study ID or --all")
        raise typer.Exit(code=1)

    if refresh_refs:
        console.print("[bold blue]Refreshing reference data (MOAlmanac, CIViC, IntOGen)...[/bold blue]")
        try:
            refresh_reference_data()
            console.print("[green]Reference data refreshed.[/green]")
        except Exception as e:
            console.print(f"[bold red]Reference refresh failed:[/bold red] {e}")
            raise typer.Exit(code=1)

    conn = database.get_connection()
    try:
        if all_studies:
            study_ids = [r[0] for r in conn.execute("SELECT study_id FROM studies ORDER BY study_id").fetchall()]
            if not study_ids:
                console.print("[yellow]No studies found in the database.[/yellow]")
                return
            _annotate_many(conn, study_ids, force=force, skip_vibe_vep=skip_vibe_vep)
        else:
            _annotate_one(conn, study_id, force=force, skip_vibe_vep=skip_vibe_vep)
    finally:
        conn.close()


def _annotate_one(conn, study_id: str, force: bool, skip_vibe_vep: bool) -> None:
    row = conn.execute("SELECT name FROM studies WHERE study_id = ?", (study_id,)).fetchone()
    if not row:
        console.print(f"[bold red]Error:[/bold red] Study '{study_id}' not found in the database.")
        console.print(f"Tip: load it first with [cyan]cbio beta db add {study_id}[/cyan]")
        raise typer.Exit(code=1)

    study_name = row[0]
    console.print(f"\nAnnotating [cyan]{study_id}[/cyan] ({study_name})")
    if skip_vibe_vep:
        console.print("[dim]vibe-vep: skipped[/dim]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Running annotation pipeline...", total=None)
        try:
            summary = annotate_study(
                conn,
                study_id,
                force=force,
                skip_vibe_vep=skip_vibe_vep,
            )
            progress.update(task, completed=True)
        except Exception as e:
            progress.stop()
            console.print(f"[bold red]Annotation failed:[/bold red] {e}")
            raise typer.Exit(code=1)

    if summary.get("skipped"):
        console.print(
            f"[yellow]Already annotated[/yellow] ({summary['total']} rows). "
            "Use [cyan]--force[/cyan] to rebuild."
        )
    else:
        console.print(
            f"[green]Done.[/green] "
            f"{summary['mutations']} mutations, "
            f"{summary['cna']} CNA, "
            f"{summary['sv']} SV → "
            f"[bold]{summary['total']}[/bold] total rows"
        )


def _annotate_many(conn, study_ids: list[str], force: bool, skip_vibe_vep: bool) -> None:
    console.print(f"\nAnnotating [bold]{len(study_ids)}[/bold] studies...")

    success = 0
    failed = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for sid in study_ids:
            task = progress.add_task(f"{sid}", total=None)
            try:
                summary = annotate_study(conn, sid, force=force, skip_vibe_vep=skip_vibe_vep)
                skipped = summary.get("skipped", False)
                progress.update(
                    task,
                    description=f"[green]✓[/green] {sid} ({summary['total']} rows{'  [dim][skip][/dim]' if skipped else ''})",
                    completed=True,
                )
                success += 1
            except Exception as e:
                progress.update(task, description=f"[red]✗[/red] {sid}: {e}", completed=True)
                failed.append(sid)

    console.print(f"\n[green]{success}[/green] succeeded, [red]{len(failed)}[/red] failed")
    if failed:
        console.print("Failed studies: " + ", ".join(failed))
        raise typer.Exit(code=1)
