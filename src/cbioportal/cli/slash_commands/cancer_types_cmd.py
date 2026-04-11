"""/cancer-types — list cancer types with study counts."""
from __future__ import annotations

from typing import List

from cli_textual.core.command import SlashCommand

from cbioportal.cli.tools._db import open_conn
from cbioportal.core.study_repository import get_cancer_type_counts


class CancerTypesCommand(SlashCommand):
    name = "/cancer-types"
    description = "List cancer types with study counts (optional data_type filter)"

    async def execute(self, app, args: List[str]) -> None:
        data_types = [args[0]] if args else None
        with open_conn() as conn:
            organ, special = get_cancer_type_counts(conn, data_types=data_types)

        lines: list[str] = []
        suffix = f" (filtered: `{args[0]}`)" if args else ""

        lines.append(f"### Organ systems{suffix}")
        if organ:
            lines.append("")
            lines.append("| cancer type | studies |")
            lines.append("| --- | --- |")
            for ct in sorted(organ.keys(), key=lambda k: (-organ[k], k)):
                lines.append(f"| {ct} | {organ[ct]} |")
        else:
            lines.append("")
            lines.append("_(none)_")

        lines.append("")
        lines.append(f"### Special collections{suffix}")
        if special:
            lines.append("")
            lines.append("| collection | studies |")
            lines.append("| --- | --- |")
            for ct in sorted(special.keys(), key=lambda k: (-special[k], k)):
                lines.append(f"| {ct} | {special[ct]} |")
        else:
            lines.append("")
            lines.append("_(none)_")

        app.add_to_history("\n".join(lines))
