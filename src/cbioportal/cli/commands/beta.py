"""cbio beta — local DuckDB server and sync commands (wraps existing CLI)."""
from __future__ import annotations

import subprocess
import sys

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from cbioportal.cli import db, fetch, server
from cbioportal.core import database

load_dotenv()

app = typer.Typer(help="[Beta] Local DuckDB server and sync commands")

app.add_typer(db.app, name="db")
app.add_typer(fetch.app, name="fetch")

console = Console()


@app.command()
def init() -> None:
    """Initialize the database by syncing all reference data (OncoTree, genes, panels)."""
    commands = [
        ["beta", "db", "sync-oncotree"],
        ["beta", "db", "sync-gene-reference"],
        ["beta", "db", "sync-gene-symbol-updates"],
        ["beta", "db", "sync-gene-aliases"],
        ["beta", "db", "sync-gene-panels"],
    ]

    try:
        console.print("[bold blue]Starting database initialization...[/bold blue]")

        for cmd_args in commands:
            cmd_name = cmd_args[-1]
            console.print(f"[bold]Running {cmd_name}...[/bold]")

            full_cmd = [sys.executable, "-m", "cbioportal.cli.main"] + cmd_args
            result = subprocess.run(full_cmd, capture_output=True, text=True)

            if result.returncode == 0:
                console.print(result.stdout.strip())
            else:
                console.print(f"[bold red]Command {cmd_name} failed:[/bold red]")
                console.print(result.stderr)
                console.print(result.stdout)
                raise typer.Exit(code=result.returncode)

        console.print("\n[bold green]Database initialization complete![/bold green]")

    except Exception as e:
        if not isinstance(e, typer.Exit):
            console.print(f"\n[bold red]Initialization failed:[/bold red] {e}")
            raise typer.Exit(code=1)
        raise e


@app.command("local-studies")
def local_studies() -> None:
    """List all studies in the local DuckDB database with sample counts."""
    conn = database.get_connection(read_only=True)

    query = """
        SELECT
            s.study_id,
            s.name,
            s.type_of_cancer,
            COALESCE(counts.sample_count, 0) as samples
        FROM studies s
        LEFT JOIN (
            SELECT study_id, count(*) as sample_count
            FROM clinical_sample
            GROUP BY study_id
        ) counts ON s.study_id = counts.study_id
        ORDER BY samples DESC
    """

    try:
        rows = conn.execute(query).fetchall()

        table = Table(title="cBioPortal Studies (local)")
        table.add_column("Study ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Cancer Type", style="magenta")
        table.add_column("Samples", justify="right", style="yellow")

        for row in rows:
            name = row[1][:50] + "..." if len(row[1]) > 50 else row[1]
            table.add_row(row[0], name, row[2], str(row[3]))

        console.print(table)
        console.print(f"\nTotal studies: [bold]{len(rows)}[/bold]")

    except Exception as e:
        typer.echo(f"Error querying studies: {e}")
    finally:
        conn.close()


@app.command()
def study(study_id: str) -> None:
    """Show detailed information for a specific study in the local database."""
    conn = database.get_connection(read_only=True)

    try:
        meta = conn.execute(
            "SELECT * FROM studies WHERE study_id = ?", (study_id,)
        ).fetchone()
        if not meta:
            console.print(
                f"[bold red]Error:[/bold red] Study '[cyan]{study_id}[/cyan]' not found."
            )
            return

        patient_count = conn.execute(
            "SELECT count(*) FROM clinical_patient WHERE study_id = ?", (study_id,)
        ).fetchone()[0]
        sample_count = conn.execute(
            "SELECT count(*) FROM clinical_sample WHERE study_id = ?", (study_id,)
        ).fetchone()[0]

        available_types = conn.execute(
            "SELECT data_type FROM study_data_types WHERE study_id = ?", (study_id,)
        ).fetchall()
        available_types = [dt[0] for dt in available_types]

        suffix_map = {
            "mutation": "mutations",
            "gene_panel": "gene_panel",
            "cna": "cna",
            "mrna": "mrna",
            "protein": "protein",
            "methylation": "methylation",
            "sv": "sv",
            "treatment": "treatment",
            "segment": "segment",
        }

        existing_tables_res = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name LIKE ?",
            (f"{study_id}_%",),
        ).fetchall()
        existing_suffixes = [t[0].replace(f"{study_id}_", "") for t in existing_tables_res]

        type_status = []
        for dt in available_types:
            suffix = suffix_map.get(dt)
            is_loaded = suffix in existing_suffixes

            if is_loaded:
                count = 0
                try:
                    table_name = f'"{study_id}_{suffix}"'
                    if dt == "mutation":
                        count = conn.execute(
                            f"SELECT count(DISTINCT Tumor_Sample_Barcode) FROM {table_name}"
                        ).fetchone()[0]
                    else:
                        count = conn.execute(
                            f"SELECT count(*) FROM {table_name}"
                        ).fetchone()[0]
                    status = f"[green]{dt} ({count})[/green]"
                except Exception:
                    status = f"[green]{dt} (Loaded)[/green]"
            else:
                status = f"[dim]{dt} (Available)[/dim]"

            type_status.append(status)

        console.print(f"\n[bold underline]Study Details: {study_id}[/bold underline]")
        console.print(f"[bold]Name:[/bold] {meta[2]}")
        console.print(f"[bold]Cancer Type:[/bold] {meta[1]}")
        console.print(f"[bold]Description:[/bold] {meta[3] or 'N/A'}")

        stats_table = Table(show_header=False, box=None)
        stats_table.add_row(
            "[bold]Patients:[/bold]",
            f"[green]{patient_count}[/green]" if patient_count > 0 else "0",
        )
        stats_table.add_row(
            "[bold]Samples:[/bold]",
            f"[green]{sample_count}[/green]" if sample_count > 0 else "0",
        )
        stats_table.add_row(
            "[bold]Data Progress:[/bold]",
            ", ".join(type_status) if type_status else "None detected",
        )
        console.print(stats_table)
        console.print("\n[dim]Legend: [green]Imported[/green], Available but not imported[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error querying study:[/bold red] {e}")
    finally:
        conn.close()


@app.command()
def serve(
    port: int = typer.Option(8000, help="Port to run the server on"),
    host: str = typer.Option("127.0.0.1", help="Host to run the server on"),
) -> None:
    """Launch the FastAPI/HTMX webserver."""
    server.run(port=port, host=host)
