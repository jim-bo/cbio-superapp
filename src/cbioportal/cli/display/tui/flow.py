"""Search, wizard, and config flow handlers for the cbio TUI."""
from __future__ import annotations

import asyncio

from cbioportal.cli.display.tui.history import HistoryEntry, MessageKind
from cbioportal.cli.commands.data import DataType, OutputFormat

FORMAT_MAP: dict[DataType, list[OutputFormat]] = {
    DataType.mutations: [OutputFormat.maf, OutputFormat.vcf, OutputFormat.tsv],
    DataType.cna:       [OutputFormat.seg, OutputFormat.tsv],
    DataType.clinical:  [OutputFormat.tsv],
    DataType.sv:        [OutputFormat.tsv],
}


def _fetch_studies_sync(query: str):
    from cbioportal.core.api.client import CbioPortalClient
    with CbioPortalClient() as client:
        return client.search_studies(query)


def _add_table_header(state) -> None:
    state.selector_header = [
        ("class:table-header", f"    {'Study ID':<24}"),
        ("class:table-header", f"{'Name':<40}"),
        ("class:table-header", f"{'Samples':>8}  "),
        ("class:table-header", "Cancer Type\n"),
        ("class:table-sep", f"    {'─' * 24}"),
        ("class:table-sep", f"{'─' * 40}"),
        ("class:table-sep", f"{'─' * 8}  "),
        ("class:table-sep", "─" * 22 + "\n"),
    ]


async def handle_search(args: list[str], state, app) -> None:
    """Fetch studies, display table, then run the study → data type → format wizard."""
    query = " ".join(args)
    state.history.add(HistoryEntry(MessageKind.USER, f"search {query}"))

    state.is_thinking = True
    state.spinner_control.start(app, "Searching…")
    app.invalidate()

    try:
        studies = await asyncio.to_thread(_fetch_studies_sync, query)
    except Exception as exc:
        state.is_thinking = False
        state.spinner_control.stop()
        state.history.add(HistoryEntry(MessageKind.NOTIFICATION, str(exc)))
        app.invalidate()
        return

    state.is_thinking = False
    state.spinner_control.stop()

    if not studies:
        state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, "No studies found."))
        app.invalidate()
        return

    _add_table_header(state)

    state.flow_studies = studies

    # Build display strings that match table columns for easy visual correlation
    choices = []
    for s in studies:
        study_id = s.studyId[:24]
        name     = s.name[:40]
        samples  = f"{s.sequencedSampleCount:,}"
        cancer   = (s.cancerType.name if s.cancerType else "")[:22]
        choices.append(f"{study_id:<24}{name:<40}{samples:>8}  {cancer}")

    state.selector_options = choices
    state.selector_index = 0

    async def on_study_selected(idx: int, text: str) -> None:
        study = state.flow_studies[idx]
        state.flow_study = study
        state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, f"◇ study:  {study.studyId}"))
        _show_data_type_selector(state, app)
        app.invalidate()

    state.selector_callback = on_study_selected
    app.invalidate()


def _show_data_type_selector(state, app) -> None:
    data_types = list(DataType)
    state.selector_options = [dt.value for dt in data_types]
    state.selector_index = 0
    state.selector_header = [("class:title", "  What data type are you looking to export?\n")]

    async def on_data_type_selected(idx: int, text: str) -> None:
        dt = list(DataType)[idx]
        state.flow_data_type = dt
        state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, f"◇ type:   {dt.value}"))
        _show_format_selector(state, app)
        app.invalidate()

    state.selector_callback = on_data_type_selected


def _show_format_selector(state, app) -> None:
    formats = FORMAT_MAP[state.flow_data_type]
    state.selector_options = [f.value for f in formats]
    state.selector_index = 0
    state.selector_header = [("class:title", "  What format would you like to export?\n")]

    async def on_format_selected(idx: int, text: str) -> None:
        formats = FORMAT_MAP[state.flow_data_type]
        fmt = formats[idx]
        state.flow_format = fmt
        state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, f"◇ format: {fmt.value}"))
        _show_output_prompt(state, app)
        app.invalidate()

    state.selector_callback = on_format_selected


def _show_output_prompt(state, app) -> None:
    suggested = f"{state.flow_study.studyId}.{state.flow_format.value}"
    state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, f"Save to: [{suggested}]  (edit and press enter)"))
    # Pre-fill the input with the suggested filename so user can just press Enter or edit
    state.input_field.text = suggested

    async def on_output_confirmed(text: str) -> None:
        path = text.strip() or suggested
        state.history.add(HistoryEntry(MessageKind.USER, path))
        
        if state.flow_data_type.value == "mutations":
            state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, f"Starting pull and export to {path}..."))
            state.is_thinking = True
            state.spinner_control.start(app, "Pulling mutations & annotating...")
            app.invalidate()
            
            def _run_pull():
                from cbioportal.core.data_puller import pull_and_export_mutations
                pull_and_export_mutations(state.flow_study.studyId, path)
                
            try:
                await asyncio.to_thread(_run_pull)
                state.history.add(HistoryEntry(MessageKind.NOTIFICATION, f"✓ Successfully pulled and exported to {path}"))
            except Exception as exc:
                state.history.add(HistoryEntry(MessageKind.NOTIFICATION, f"Error during pull: {exc}"))
            finally:
                state.is_thinking = False
                state.spinner_control.stop()
        else:
            state.history.add(HistoryEntry(
                MessageKind.NOTIFICATION,
                f"Pull for {state.flow_data_type.value} not yet implemented.",
            ))
            
        state.flow_study = None
        state.flow_data_type = None
        state.flow_format = None
        state.flow_studies = []
        app.invalidate()

    state.on_submit_override = on_output_confirmed


async def handle_config(state, app) -> None:
    """Show config settings, then allow editing via selector + text input."""
    from cbioportal.core.cbio_config import get_config, set_config

    state.history.add(HistoryEntry(MessageKind.USER, "config"))

    try:
        cfg = get_config()
    except Exception as exc:
        state.history.add(HistoryEntry(MessageKind.NOTIFICATION, f"Config error: {exc}"))
        app.invalidate()
        return

    portal_url   = cfg.get("portal", {}).get("url", "")
    portal_token = cfg.get("portal", {}).get("token", "")
    oncokb_token = cfg.get("oncokb", {}).get("token", "")
    cache_ttl    = cfg.get("cache", {}).get("ttl_days", 180)

    def _mask(val: str) -> str:
        return ("*" * 8 + val[-4:]) if val else "(not set)"

    # Table header
    state.history.add(HistoryEntry(kind=MessageKind.TABLE_ROW, cells=[
        ("class:table-header", f"  {'Key':<22}"),
        ("class:table-header", "Value"),
    ]))
    for key, val in [
        ("portal.url",    portal_url or "(not set)"),
        ("portal.token",  _mask(portal_token)),
        ("oncokb.token",  _mask(oncokb_token)),
        ("cache.ttl",     f"{cache_ttl} days"),
    ]:
        state.history.add(HistoryEntry(kind=MessageKind.TABLE_ROW, cells=[
            ("class:meta",       f"  {key:<22}"),
            ("class:step-value", val),
        ]))

    SETTINGS = ["portal.url", "portal.token", "oncokb.token", "← Done"]
    state.selector_options = SETTINGS
    state.selector_index = 0

    async def on_setting_selected(idx: int, text: str) -> None:
        if text == "← Done":
            state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, "Config closed."))
            app.invalidate()
            return

        key = text
        state.history.add(HistoryEntry(MessageKind.COMMAND_RESPONSE, f"New value for {key}:"))
        state.input_field.text = ""

        async def on_value_entered(new_val: str) -> None:
            nv = new_val.strip()
            if nv:
                section, field = key.split(".")
                set_config(section, field, nv)
                state.history.add(HistoryEntry(MessageKind.USER, nv))
                state.history.add(HistoryEntry(MessageKind.NOTIFICATION, f"✓ {key} updated"))
            app.invalidate()

        state.on_submit_override = on_value_entered
        app.invalidate()

    state.selector_callback = on_setting_selected
    app.invalidate()


async def handle_sync(state, app) -> None:
    """Fetch all studies + clinical data from cBioPortal and store in cache DB."""
    from cbioportal.core.syncer import sync_all

    state.is_thinking = True
    app.invalidate()

    def on_progress(msg: str) -> None:
        state.history.add(HistoryEntry(MessageKind.NOTIFICATION, msg))
        app.invalidate()

    try:
        stats = await sync_all(on_progress)
        state.history.add(HistoryEntry(
            MessageKind.COMMAND_RESPONSE,
            f"✓ Sync complete: {stats['studies']} studies, {stats['clinical_rows']} clinical rows",
        ))
    except Exception as exc:
        state.history.add(HistoryEntry(MessageKind.NOTIFICATION, f"Sync failed: {exc}"))
    finally:
        state.is_thinking = False
        app.invalidate()
