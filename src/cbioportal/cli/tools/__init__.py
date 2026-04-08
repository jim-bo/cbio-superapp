"""Agent tools that wrap cbioportal repository/loader functions.

Each tool is a pure ``async def`` returning ``cli_textual.tools.base.ToolResult``.
No TUI imports, no event queue — the cli_textual framework wraps them and emits
the AgentToolStart/Output/End lifecycle events.
"""
from cbioportal.cli.tools.studies import list_studies, describe_study
from cbioportal.cli.tools.gene_frequency import (
    gene_mutation_frequency,
    gene_cna_frequency,
    gene_sv_frequency,
)
from cbioportal.cli.tools.study_loader import (
    validate_study_folder,
    load_study_into_db,
)

# Read-only tool set: never mutates the DuckDB file.
# Used for web-served sessions (CBIO_WEB_MODE=1) where an untrusted
# browser user is driving the agent.
CBIO_READ_ONLY_TOOLS = [
    list_studies,
    describe_study,
    gene_mutation_frequency,
    gene_cna_frequency,
    gene_sv_frequency,
    validate_study_folder,  # static check, does not write
]

# Full tool set adds mutating tools.
CBIO_TOOLS = CBIO_READ_ONLY_TOOLS + [
    load_study_into_db,
]


def get_tools_for_env() -> list:
    """Return the tool set appropriate to the current execution environment.

    Under ``CBIO_WEB_MODE=1``, drop any tool that can mutate state.
    New mutating tools should be added to ``CBIO_TOOLS`` but kept out
    of ``CBIO_READ_ONLY_TOOLS``.
    """
    import os

    if os.environ.get("CBIO_WEB_MODE") == "1":
        return list(CBIO_READ_ONLY_TOOLS)
    return list(CBIO_TOOLS)


__all__ = [
    "CBIO_TOOLS",
    "CBIO_READ_ONLY_TOOLS",
    "get_tools_for_env",
    "list_studies",
    "describe_study",
    "gene_mutation_frequency",
    "gene_cna_frequency",
    "gene_sv_frequency",
    "validate_study_folder",
    "load_study_into_db",
]
