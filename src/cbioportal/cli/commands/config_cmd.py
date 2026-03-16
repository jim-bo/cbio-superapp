"""cbio config — read/write ~/.cbio/config.toml."""
from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from cbioportal.core.cbio_config import get_config, set_config

app = typer.Typer(help="Configure cbio settings")
console = Console()


@app.command("set-url")
def set_url(url: str = typer.Argument(..., help="cBioPortal base URL")) -> None:
    """Set the cBioPortal portal URL."""
    set_config("portal", "url", url)
    console.print(f"[green]Portal URL set to:[/green] {url}")


@app.command("set-token")
def set_token(
    token: str = typer.Argument(..., help="Bearer token for private portals")
) -> None:
    """Set the cBioPortal API token."""
    set_config("portal", "token", token)
    console.print("[green]Portal token saved.[/green]")


@app.command("set-oncokb-token")
def set_oncokb_token(
    token: str = typer.Argument(..., help="OncoKB API token")
) -> None:
    """Set the OncoKB API token."""
    set_config("oncokb", "token", token)
    console.print("[green]OncoKB token saved.[/green]")


@app.command("show")
def show() -> None:
    """Show the current cbio configuration."""
    config = get_config()

    table = Table(title="cbio configuration", show_header=True)
    table.add_column("Section", style="cyan")
    table.add_column("Key", style="bold")
    table.add_column("Value")

    for section, values in config.items():
        if isinstance(values, dict):
            for key, value in values.items():
                display = (
                    ("*" * 8 + str(value)[-4:]) if key == "token" and value else str(value)
                )
                table.add_row(section, key, display)
        else:
            table.add_row("", section, str(values))

    console.print(table)
