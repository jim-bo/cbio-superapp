"""/study-info — show metadata for one study, resolved from free text."""
from __future__ import annotations

from typing import List

from cli_textual.core.command import SlashCommand

from cbioportal.cli.slash_commands._resolve import resolve_study_id
from cbioportal.cli.tools._db import open_conn
from cbioportal.cli.tools.studies import describe_study


class StudyInfoCommand(SlashCommand):
    name = "/study-info"
    description = "Show metadata for one study (query by id, name, or cancer type)"

    async def execute(self, app, args: List[str]) -> None:
        if not args:
            app.add_to_history(
                "**Usage:** `/study-info <study_id | name | cancer type>`"
            )
            return

        query = " ".join(args)
        with open_conn() as conn:
            resolved, candidates = resolve_study_id(conn, query)

        if resolved:
            result = await describe_study(resolved)
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
