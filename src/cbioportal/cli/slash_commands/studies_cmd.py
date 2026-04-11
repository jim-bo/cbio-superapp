"""/studies — list studies in the local DuckDB without involving the LLM."""
from __future__ import annotations

from typing import List

from cli_textual.core.command import SlashCommand

from cbioportal.cli.tools.studies import list_studies


class StudiesCommand(SlashCommand):
    name = "/studies"
    description = "List studies in the local cbioportal DuckDB"

    async def execute(self, app, args: List[str]) -> None:
        cancer_type = args[0] if args else None
        result = await list_studies(cancer_type=cancer_type)
        app.add_to_history(result.output)
