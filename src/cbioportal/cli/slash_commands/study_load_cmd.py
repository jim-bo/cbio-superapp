"""/study-load — validate and (with permission) load a study folder into DuckDB."""
from __future__ import annotations

from typing import List

from cli_textual.core.command import SlashCommand

from cbioportal.cli.tools.study_loader import (
    validate_study_folder,
    load_study_into_db,
)


class StudyLoadCommand(SlashCommand):
    name = "/study-load"
    description = "Validate (and optionally load) a cBioPortal study folder"
    requires_permission = True

    async def execute(self, app, args: List[str]) -> None:
        if not args:
            app.add_to_history(
                "**Usage:** `/study-load <path-to-study-folder> [--load]`\n\n"
                "Without `--load`, runs validation only and prints any errors/warnings."
            )
            return

        do_load = "--load" in args
        path_args = [a for a in args if not a.startswith("--")]
        if not path_args:
            app.add_to_history("**Error:** missing study folder path.")
            return
        path = path_args[0]

        validation = await validate_study_folder(path)
        app.add_to_history(validation.output)

        if not do_load:
            app.add_to_history(
                "_Validation only. Pass `--load` to ingest into the database._"
            )
            return

        if validation.is_error:
            app.add_to_history(
                "**Refusing to load** — fix the errors above first, or run again "
                "after correcting the study folder."
            )
            return

        result = await load_study_into_db(path)
        app.add_to_history(result.output)
