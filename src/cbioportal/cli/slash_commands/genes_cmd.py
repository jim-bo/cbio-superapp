"""/genes — top mutated genes in a study, resolved from free text."""
from __future__ import annotations

from typing import List

from cli_textual.core.command import SlashCommand

from cbioportal.cli.slash_commands._resolve import resolve_study_id
from cbioportal.cli.tools._db import open_conn
from cbioportal.cli.tools.gene_frequency import gene_mutation_frequency


class GenesCommand(SlashCommand):
    name = "/genes"
    description = "Top mutated genes in a study (query by id, name, or cancer type)"

    async def execute(self, app, args: List[str]) -> None:
        if not args:
            app.add_to_history(
                "**Usage:** `/genes <study_id | name | cancer type> [limit]`"
            )
            return

        limit = 15
        query_parts: list[str] = []
        for a in args:
            if a.isdigit():
                limit = int(a)
            else:
                query_parts.append(a)

        if not query_parts:
            app.add_to_history("**Error:** missing study query.")
            return

        query = " ".join(query_parts)
        with open_conn() as conn:
            resolved, candidates = resolve_study_id(conn, query)

        if resolved:
            result = await gene_mutation_frequency(study_id=resolved, limit=limit)
            app.add_to_history(result.output)
            return

        if not candidates:
            app.add_to_history(
                f"_No study matched `{query}`. Try `/studies` to list them all._"
            )
            return

        lines = [
            f"**Multiple studies matched `{query}`** — be more specific:",
            "",
            "| study_id | name | cancer type |",
            "| --- | --- | --- |",
        ]
        for c in candidates:
            lines.append(
                f"| `{c['study_id']}` | {c['name'] or ''} | {c['type_of_cancer'] or ''} |"
            )
        app.add_to_history("\n".join(lines))
