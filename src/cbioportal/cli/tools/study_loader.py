"""Study loading & validation tools.

The validate tool does a static sanity check on a cBioPortal study folder
without touching the database. The load tool delegates to the real
``loader.load_study`` pipeline and mutates the DuckDB file.
"""
from __future__ import annotations

import asyncio
import csv
from pathlib import Path

from cli_textual.tools.base import ToolResult

from cbioportal.cli.tools._db import open_conn
from cbioportal.cli.tools._paths import PathNotAllowed, resolve_safe_path
from cbioportal.cli.tools._scrub import scrub_tool_output
from cbioportal.core import loader
from cbioportal.core.loader.discovery import discover_studies, parse_meta_file


# Per-datatype required meta keys and required data-file columns. Based on the
# cBioPortal File Formats doc; covers the common case, not every optional field.
_META_REQUIRED = {
    "meta_study.txt": {"type_of_cancer", "cancer_study_identifier", "name", "description"},
    "meta_clinical_sample.txt": {"cancer_study_identifier", "genetic_alteration_type", "datatype", "data_filename"},
    "meta_clinical_patient.txt": {"cancer_study_identifier", "genetic_alteration_type", "datatype", "data_filename"},
    "meta_mutations_extended.txt": {"cancer_study_identifier", "genetic_alteration_type", "datatype", "stable_id", "data_filename"},
    "meta_CNA.txt": {"cancer_study_identifier", "genetic_alteration_type", "datatype", "stable_id", "data_filename"},
    "meta_sv.txt": {"cancer_study_identifier", "genetic_alteration_type", "datatype", "stable_id", "data_filename"},
}

_DATA_REQUIRED_COLS = {
    "data_mutations.txt": {"Hugo_Symbol", "Tumor_Sample_Barcode", "Variant_Classification"},
    "data_mutations_extended.txt": {"Hugo_Symbol", "Tumor_Sample_Barcode", "Variant_Classification"},
    "data_sv.txt": {"Sample_Id", "Site1_Hugo_Symbol"},
    "data_CNA.txt": {"Hugo_Symbol"},
    "data_cna.txt": {"Hugo_Symbol"},
    "data_clinical_sample.txt": {"PATIENT_ID", "SAMPLE_ID"},
    "data_clinical_patient.txt": {"PATIENT_ID"},
}


def _read_tsv_header(path: Path) -> list[str] | None:
    """Return the first non-comment header row as a list of column names."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith("#") or not line.strip():
                    continue
                return [c.strip() for c in line.rstrip("\n").split("\t")]
    except Exception:
        return None
    return None


def _validate_study_sync(path_str: str) -> ToolResult:
    try:
        study_path = resolve_safe_path(path_str, must_exist=True)
    except PathNotAllowed as exc:
        return ToolResult(
            output=f"Refused: {exc}",
            is_error=True,
            exit_code=1,
        )
    if not study_path.is_dir():
        return ToolResult(
            output=f"Path is not a directory: {study_path}",
            is_error=True,
            exit_code=1,
        )

    findings: list[dict] = []

    def add(severity: str, file: str, message: str, hint: str = "") -> None:
        findings.append(
            {"severity": severity, "file": file, "message": message, "hint": hint}
        )

    # 1. meta_study.txt is mandatory
    meta_study = study_path / "meta_study.txt"
    if not meta_study.exists():
        add(
            "error",
            "meta_study.txt",
            "Missing meta_study.txt — this file anchors the study.",
            "Create meta_study.txt with type_of_cancer, cancer_study_identifier, name, description.",
        )
        study_id = study_path.name
    else:
        meta = parse_meta_file(meta_study)
        missing = _META_REQUIRED["meta_study.txt"] - set(meta.keys())
        for m in sorted(missing):
            add(
                "error",
                "meta_study.txt",
                f"Missing required key `{m}`.",
                f"Add `{m}: <value>` to meta_study.txt.",
            )
        study_id = meta.get("cancer_study_identifier") or study_path.name

    # 2. Check every meta_*.txt file we know about
    for meta_name, required in _META_REQUIRED.items():
        if meta_name == "meta_study.txt":
            continue
        meta_path = study_path / meta_name
        if not meta_path.exists():
            continue  # optional
        meta = parse_meta_file(meta_path)
        for m in sorted(required - set(meta.keys())):
            add(
                "error",
                meta_name,
                f"Missing required key `{m}`.",
                f"Add `{m}: <value>` to {meta_name}.",
            )
        # Confirm declared data_filename exists
        declared = meta.get("data_filename")
        if declared:
            declared_path = study_path / declared
            if not declared_path.exists():
                add(
                    "error",
                    meta_name,
                    f"`data_filename: {declared}` points to a file that does not exist.",
                    f"Either rename {declared} to match the actual file or fix the meta value.",
                )

    # 3. Clinical files are effectively mandatory for a loadable study
    for must in ("data_clinical_sample.txt", "data_clinical_patient.txt"):
        if not (study_path / must).exists():
            add(
                "warning",
                must,
                f"{must} not found — the loader needs at least one clinical file.",
                "Provide clinical sample/patient TSVs or the study will have zero samples.",
            )

    # 4. For every data file we recognize, spot-check column headers
    for data_name, required_cols in _DATA_REQUIRED_COLS.items():
        data_path = study_path / data_name
        if not data_path.exists():
            continue
        header = _read_tsv_header(data_path)
        if header is None:
            add(
                "error",
                data_name,
                "Unable to read header row (file empty, unreadable, or all comments).",
                "Open the file and make sure there is a tab-separated header line.",
            )
            continue
        missing_cols = required_cols - set(header)
        if missing_cols:
            add(
                "error",
                data_name,
                f"Missing required column(s): {sorted(missing_cols)}",
                "Header names are case-sensitive. Verify exact spelling against the "
                "cBioPortal file-format spec.",
            )
        # Cheap sample-row sanity check (first 5 data rows)
        try:
            with data_path.open("r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f, delimiter="\t")
                hdr_seen = False
                bad = 0
                for row in reader:
                    if not row or row[0].startswith("#"):
                        continue
                    if not hdr_seen:
                        hdr_seen = True
                        continue
                    if len(row) != len(header):
                        bad += 1
                    if bad >= 3:
                        add(
                            "warning",
                            data_name,
                            "Multiple rows have a different column count than the header.",
                            "Likely an unescaped tab or a ragged row. Open in a TSV viewer to inspect.",
                        )
                        break
        except Exception:
            pass

    # Build output
    n_err = sum(1 for f in findings if f["severity"] == "error")
    n_warn = sum(1 for f in findings if f["severity"] == "warning")
    lines = [
        f"# Study validation: `{study_id}`",
        f"_Path: {study_path}_",
        "",
        f"**{n_err} error(s), {n_warn} warning(s)**",
        "",
    ]
    if not findings:
        lines.append("✅ No issues found. The study folder looks well-formed.")
    else:
        for sev in ("error", "warning"):
            bucket = [f for f in findings if f["severity"] == sev]
            if not bucket:
                continue
            lines.append(f"## {sev.upper()}S")
            for f in bucket:
                lines.append(f"- **{f['file']}** — {f['message']}")
                if f["hint"]:
                    lines.append(f"  - _Hint:_ {f['hint']}")
            lines.append("")
    # M7: the findings include file names and hint text that may
    # contain attacker-controlled content from meta_*.txt. Scrub
    # before handing back to the LLM.
    return ToolResult(
        output=scrub_tool_output("\n".join(lines)),
        is_error=n_err > 0,
        exit_code=1 if n_err > 0 else 0,
    )


async def validate_study_folder(path: str) -> ToolResult:
    """Statically validate a cBioPortal study folder without touching the database.

    Checks meta_*.txt required keys, data file column headers, and basic row
    consistency. Returns a structured error/warning report the caller can
    explain to the user. Does NOT load anything into DuckDB.

    Args:
        path: Absolute or ``~``-expandable path to a study folder (the directory
            containing ``meta_study.txt``).
    """
    return await asyncio.to_thread(_validate_study_sync, path)


def _load_study_sync(
    path_str: str,
    load_mutations: bool,
    load_cna: bool,
    load_sv: bool,
) -> ToolResult:
    try:
        study_path = resolve_safe_path(path_str, must_exist=True)
    except PathNotAllowed as exc:
        return ToolResult(
            output=f"Refused: {exc}",
            is_error=True,
            exit_code=1,
        )
    if not study_path.is_dir():
        return ToolResult(
            output=f"Study folder is not a directory: {study_path}",
            is_error=True,
            exit_code=1,
        )
    if not (study_path / "meta_study.txt").exists():
        return ToolResult(
            output=f"Not a study folder (no meta_study.txt): {study_path}",
            is_error=True,
            exit_code=1,
        )

    with open_conn(read_only=False) as conn:
        try:
            loader.ensure_gene_reference(conn)
            loader.load_study_metadata(conn, study_path)
            ok = loader.load_study(
                conn,
                study_path,
                load_mutations=load_mutations,
                load_cna=load_cna,
                load_sv=load_sv,
            )
            loader.create_global_views(conn)
        except Exception as exc:
            return ToolResult(
                output=f"Load failed for {study_path.name}: {exc}",
                is_error=True,
                exit_code=1,
            )

    study_id = study_path.name
    if ok:
        return ToolResult(
            output=(
                f"✅ Loaded study `{study_id}` into {loader.get_source_path() or 'DuckDB'}.\n"
                f"- mutations: {load_mutations}\n"
                f"- cna: {load_cna}\n"
                f"- sv: {load_sv}"
            )
        )
    return ToolResult(
        output=(
            f"⚠️ Loaded metadata for `{study_id}` but no clinical/genomic data was found."
        ),
        is_error=True,
        exit_code=1,
    )


async def load_study_into_db(
    path: str,
    load_mutations: bool = True,
    load_cna: bool = True,
    load_sv: bool = True,
) -> ToolResult:
    """Load a study folder into the local DuckDB database.

    This mutates the database. The cli-textual permission modal will prompt
    the user before execution. Run ``validate_study_folder`` first if you
    aren't sure the folder is well-formed.

    Args:
        path: Path to the study folder (must contain meta_study.txt).
        load_mutations: Load data_mutations.txt if present. Defaults to True.
        load_cna: Load data_CNA.txt if present. Defaults to True.
        load_sv: Load data_sv.txt if present. Defaults to True.
    """
    return await asyncio.to_thread(
        _load_study_sync, path, load_mutations, load_cna, load_sv
    )
