"""cbio search <query> — stub."""
from __future__ import annotations

from typing import Optional

import typer

app = typer.Typer(help="Search cBioPortal studies")


@app.callback(invoke_without_command=True)
def search(
    ctx: typer.Context,
    query: str = typer.Argument(..., help='Search term, e.g. "breast cancer"'),
    cancer_type: Optional[str] = typer.Option(
        None, "--cancer-type", help="Filter by cancer type ID"
    ),
    min_samples: Optional[int] = typer.Option(
        None, "--min-samples", help="Minimum number of samples"
    ),
) -> None:
    """Search studies on cBioPortal, then optionally export data via interactive wizard."""
    typer.echo("Not yet implemented")
    raise typer.Exit(1)
