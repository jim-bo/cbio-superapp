"""/data-types — list genomic data types profiled in a study."""
from __future__ import annotations

from typing import List

from cli_textual.core.command import SlashCommand

from cbioportal.cli.slash_commands._resolve import resolve_study_id
from cbioportal.cli.tools._db import open_conn


class DataTypesCommand(SlashCommand):
    name = "/data-types"
    description = "List genomic data types profiled in a study"

    async def execute(self, app, args: List[str]) -> None:
        if not args:
            app.add_to_history(
                "**Usage:** `/data-types <study_id | name | cancer type>`"
            )
            return

        query = " ".join(args)
        with open_conn() as conn:
            resolved, candidates = resolve_study_id(conn, query)

            if resolved:
                rows = conn.execute(
                    "SELECT DISTINCT data_type FROM study_data_types "
                    "WHERE study_id = ? ORDER BY data_type",
                    [resolved],
                ).fetchall()
                if not rows:
                    app.add_to_history(
                        f"_No data types recorded for `{resolved}`._"
                    )
                    return
                lines = [f"**Data types for `{resolved}`:**", ""]
                for (dt,) in rows:
                    lines.append(f"- {dt}")
                app.add_to_history("\n".join(lines))
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
