"""cbio — root CLI entry point.

Interactive mode is powered by ``cli-textual``'s ``ChatApp``: a Textual TUI
backed by a pydantic-ai agent that calls cbio-specific tools (gene frequency,
study validation, etc.) registered in ``cbioportal.cli.tools``.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer

from cbioportal.cli.commands import beta, config_cmd, data, search
from cbioportal.cli.commands.annotate import annotate

app = typer.Typer(help="cbio — cBioPortal data access from your terminal")

CBIO_LOG_DIR = Path(".cbio/convos")


def _launch_chat_app(log: bool = False) -> None:
    """Start the cli-textual TUI with cbio tools and slash commands.

    When ``log`` is True, the full conversation (user input, LLM events,
    tool calls) is appended to ``~/.cbio/convos/<utc-timestamp>.jsonl``.
    """
    from cli_textual.app import ChatApp

    from cbioportal.cli.tools import CBIO_TOOLS

    log_path = None
    if log:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_path = CBIO_LOG_DIR / f"{ts}.jsonl"

    ChatApp(
        tools=CBIO_TOOLS,
        command_packages=["cbioportal.cli.slash_commands"],
        log_path=log_path,
        system_prompt_append=CBIO_SYSTEM_PROMPT,
        safe_mode=True,
    ).run()


CBIO_SYSTEM_PROMPT = """\
You are the cbio assistant — a terminal interface to cBioPortal cancer genomics data.

Users typically want to:
  - Explore studies and their clinical/genomic data (use list_studies, describe_study).
  - Ask gene-frequency questions; ALWAYS use the panel-aware tools
    (gene_mutation_frequency, gene_cna_frequency, gene_sv_frequency) which
    compute denominators from the profiled-samples table — never divide by total
    samples yourself.
  - Load custom study folders. Use validate_study_folder first to surface
    formatting errors and translate them into plain English with concrete fixes
    before calling load_study_into_db.

Cite study_id and gene symbols verbatim. Prefer concise tabular answers."""


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    no_interactive: bool = typer.Option(
        False,
        "--no-interactive",
        help="Disable interactive prompts; for use in scripts/pipelines",
    ),
    log: bool = typer.Option(
        False,
        "--log",
        help=(
            "Record the full conversation (user input, LLM events, tool calls) "
            "to ~/.cbio/convos/<utc-timestamp>.jsonl. Also enabled if CBIO_LOG=1."
        ),
    ),
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["interactive"] = not no_interactive and sys.stdout.isatty()
    log_enabled = log or os.getenv("CBIO_LOG", "").lower() in ("1", "true", "yes")
    if ctx.invoked_subcommand is None and sys.stdout.isatty():
        _launch_chat_app(log=log_enabled)


app.command("annotate", help="Annotate study variants with MOAlmanac, CIViC, IntOGen, and vibe-vep")(annotate)
app.add_typer(search.app, name="search")
app.add_typer(data.app, name="data")
app.add_typer(config_cmd.app, name="config")
app.add_typer(
    beta.app,
    name="beta",
    help="[Beta] Local DuckDB server and sync commands",
)

if __name__ == "__main__":
    app()
