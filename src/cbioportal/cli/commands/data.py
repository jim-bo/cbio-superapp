"""cbio data pull — stub."""
from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(help="Pull study data in analysis-ready formats")


class DataType(str, Enum):
    mutations = "mutations"
    cna = "cna"
    clinical = "clinical"
    sv = "sv"


class OutputFormat(str, Enum):
    maf = "maf"
    vcf = "vcf"
    seg = "seg"
    tsv = "tsv"


@app.command()
def pull(
    study_id: str = typer.Argument(..., help="cBioPortal study ID"),
    type: DataType = typer.Option(..., "--type", help="Data type to pull"),
    format: OutputFormat = typer.Option(..., "--format", help="Output file format"),
    output: Optional[Path] = typer.Option(
        None, "-o", "--output", help="Output file path"
    ),
    samples: Optional[str] = typer.Option(
        None, "--samples", help="Comma-separated sample IDs to filter"
    ),
    annotate_oncokb: bool = typer.Option(
        False, "--annotate-oncokb", help="Annotate mutations with OncoKB"
    ),
) -> None:
    """Pull study data and save in the requested format."""
    typer.echo("Not yet implemented")
    raise typer.Exit(1)
