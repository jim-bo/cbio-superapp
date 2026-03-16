"""Clack-style wizard aesthetic using questionary + Rich (stubbed)."""
from __future__ import annotations

from pathlib import Path

import questionary
from rich.console import Console
from rich.panel import Panel

from cbioportal.core.api.models import Study
from cbioportal.cli.commands.data import DataType, OutputFormat

console = Console()

# ---------------------------------------------------------------------------
# Custom questionary style (Clack-inspired)
# ---------------------------------------------------------------------------

CBIO_STYLE = questionary.Style(
    [
        ("qmark", "fg:cyan bold"),         # ◆ active prompt
        ("question", "bold"),
        ("answer", "fg:cyan"),
        ("pointer", "fg:cyan bold"),        # › selected item
        ("highlighted", "fg:cyan bold"),
        ("selected", "fg:cyan"),            # ◇ completed step
        ("separator", "fg:grey"),
        ("instruction", "fg:grey italic"),
        ("text", ""),
        ("disabled", "fg:grey italic"),
    ]
)

# Step prefix characters
ACTIVE = "◆"
DONE = "◇"
WARN = "▲"
ERROR = "×"


class AnnotationMode:
    moalmanac = "moalmanac"
    oncokb = "oncokb"
    none = "none"


# ---------------------------------------------------------------------------
# Public display functions
# ---------------------------------------------------------------------------


def print_header() -> None:
    """Render the cbio tool header panel."""
    console.print(
        Panel(
            "[bold cyan]cbio[/bold cyan] · cBioPortal Data Access",
            border_style="cyan",
        )
    )


def print_step(label: str, value: str) -> None:
    """Print a completed wizard step."""
    console.print(f"[dim]{DONE} {label}:[/dim] [cyan]{value}[/cyan]")


def print_success(path: Path, study_id: str, data_type: str) -> None:
    """Print a success message after saving data."""
    console.print(
        f"[bold green]✓[/bold green] Saved [cyan]{data_type}[/cyan] "
        f"for [cyan]{study_id}[/cyan] → [bold]{path}[/bold]"
    )


# ---------------------------------------------------------------------------
# Wizard step functions (stubbed)
# ---------------------------------------------------------------------------


def select_study(studies: list[Study]) -> Study:
    """Prompt user to select a study from search results."""
    raise NotImplementedError


def select_data_type(available: list[DataType]) -> DataType:
    """Prompt user to select a data type."""
    raise NotImplementedError


def select_format(data_type: DataType) -> OutputFormat:
    """Prompt user to select an output format valid for the chosen data type."""
    raise NotImplementedError


def select_annotation(has_oncokb_token: bool) -> AnnotationMode:
    """Prompt user to select an annotation mode."""
    raise NotImplementedError


def confirm_output(suggested: str) -> Path:
    """Prompt user to confirm or change the output file path."""
    raise NotImplementedError


def run_wizard(studies: list[Study], ctx_obj: dict) -> None:
    """Orchestrate all wizard steps and trigger data pull at the end."""
    raise NotImplementedError
