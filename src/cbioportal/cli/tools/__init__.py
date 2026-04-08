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

CBIO_TOOLS = [
    list_studies,
    describe_study,
    gene_mutation_frequency,
    gene_cna_frequency,
    gene_sv_frequency,
    validate_study_folder,
    load_study_into_db,
]

__all__ = [
    "CBIO_TOOLS",
    "list_studies",
    "describe_study",
    "gene_mutation_frequency",
    "gene_cna_frequency",
    "gene_sv_frequency",
    "validate_study_folder",
    "load_study_into_db",
]
