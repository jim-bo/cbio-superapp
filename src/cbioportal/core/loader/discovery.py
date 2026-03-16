"""Study discovery: finding study directories and parsing meta files."""
import os
from pathlib import Path

import typer
import yaml


def get_source_path():
    """Get the path to the studies source (downloads or datahub)."""
    downloads = os.getenv("CBIO_DOWNLOADS")
    datahub = os.getenv("CBIO_DATAHUB")
    mode = os.getenv("CBIO_SOURCE_MODE", "downloads").lower()

    path_str = None
    if downloads and datahub:
        path_str = downloads if mode == "downloads" else datahub
    else:
        path_str = downloads or datahub

    if not path_str:
        return None

    path = Path(path_str)
    if not path.exists():
        typer.echo(f"Warning: Source path {path} does not exist.")
        return None

    return path


def discover_studies(datahub_path: Path):
    """Recursively find all directories that contain cBioPortal data files."""
    study_dirs = set()
    markers = ["meta_study.txt", "data_clinical_patient.txt", "data_clinical_sample.txt", "data_mutations.txt"]
    for marker in markers:
        for p in datahub_path.rglob(marker):
            study_dirs.add(p.parent)
    return sorted(list(study_dirs))


def find_study_path(study_id: str) -> Path | None:
    """Find a study directory by ID, preferring CBIO_DOWNLOADS over CBIO_DATAHUB."""
    candidates = []
    downloads = os.getenv("CBIO_DOWNLOADS")
    datahub = os.getenv("CBIO_DATAHUB")
    if downloads:
        candidates.append(Path(downloads))
    if datahub:
        candidates.append(Path(datahub))
    for base in candidates:
        if not base.exists():
            continue
        match = next((s for s in discover_studies(base) if s.name == study_id), None)
        if match:
            return match
    return None


def parse_meta_file(file_path: Path):
    """Parse a cBioPortal meta_*.txt file into a dictionary."""
    meta = {}
    if not file_path.exists():
        return meta
    with open(file_path, 'r') as f:
        for line in f:
            if ':' in line:
                key, value = line.split(':', 1)
                meta[key.strip()] = value.strip()
    return meta
