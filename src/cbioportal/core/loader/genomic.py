"""Genomic data loading: FGA computation from segmentation files."""
import csv
from pathlib import Path

import typer

# Variant classifications excluded by cBioPortal at import time.
# "By default, cBioPortal filters out Silent, Intron, IGR, 3'UTR, 5'UTR,
# 3'Flank and 5'Flank, except for the promoter mutations of the TERT gene
# (5'Flank only)."
# Source: https://docs.cbioportal.org/file-formats/#mutation-data
# These never enter genomic_event_derived, so we must exclude them at load time
# to keep our sample counts consistent with the public portal.
_EXCLUDED_VCS = frozenset({
    "Silent", "Intron", "IGR", "3'UTR", "5'UTR", "3'Flank", "5'Flank",
})


def _inject_fga_from_seg(conn, sample_table: str, study_path: Path) -> bool:
    """Compute FRACTION_GENOME_ALTERED from a .seg file and add it to the sample table.

    FGA = (bases in segments where |seg.mean| >= 0.2) / (total bases profiled per sample).
    Uses a per-sample denominator so panel studies work correctly.

    Returns True if FGA was successfully injected, False if skipped or failed.
    """
    try:
        existing_cols = {c[0].upper() for c in conn.execute(f"DESCRIBE {sample_table}").fetchall()}
    except Exception:
        return False

    if "FRACTION_GENOME_ALTERED" in existing_cols or "FGA" in existing_cols:
        return False  # already present in the clinical file

    seg_file = study_path / "data_cna_hg19.seg"
    if not seg_file.exists():
        candidates = list(study_path.glob("data_cna_*.seg"))
        if not candidates:
            return False
        seg_file = candidates[0]

    total: dict[str, int] = {}
    altered: dict[str, int] = {}
    try:
        with open(seg_file, newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                sid = row.get("ID") or row.get("id")
                if not sid:
                    continue
                try:
                    length = int(row["loc.end"]) - int(row["loc.start"])
                    mean = float(row["seg.mean"])
                except (KeyError, ValueError, TypeError):
                    continue
                if length <= 0:
                    continue
                total[sid] = total.get(sid, 0) + length
                if abs(mean) >= 0.2:
                    altered[sid] = altered.get(sid, 0) + length
    except Exception as e:
        typer.echo(f"Warning: Could not read seg file {seg_file.name}: {e}")
        return False

    if not total:
        return False

    fga_rows = [
        (round(altered.get(sid, 0) / t, 4), sid)
        for sid, t in total.items()
        if t > 0
    ]

    try:
        conn.execute(f"ALTER TABLE {sample_table} ADD COLUMN FRACTION_GENOME_ALTERED DOUBLE")
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _fga_tmp (sample_id VARCHAR, fga DOUBLE)")
        conn.execute("DELETE FROM _fga_tmp")
        conn.executemany("INSERT INTO _fga_tmp VALUES (?, ?)", [(sid, fga) for fga, sid in fga_rows])
        conn.execute(f"""
            UPDATE {sample_table}
            SET FRACTION_GENOME_ALTERED = t.fga
            FROM _fga_tmp t
            WHERE {sample_table}.SAMPLE_ID = t.sample_id
        """)
        conn.execute("DROP TABLE IF EXISTS _fga_tmp")
        return True
    except Exception as e:
        typer.echo(f"Warning: Could not inject FGA into {sample_table}: {e}")
        return False
