"""MAF export from DuckDB and VEP output parser."""
from __future__ import annotations

import csv
from pathlib import Path

# Minimal MAF columns required by vibe-vep
MAF_EXPORT_COLS = [
    "Hugo_Symbol",
    "Chromosome",
    "Start_Position",
    "End_Position",
    "Reference_Allele",
    "Tumor_Seq_Allele2",
    "Tumor_Sample_Barcode",
    "HGVSp_Short",
    "Variant_Classification",
    "NCBI_Build",
]

# vibe-vep appends vibe.* columns by default
# Mapping from vibe-vep output column → our schema column
VEP_COLS = {
    "vibe.consequence":              "vep_consequence",
    "vibe.transcript_id":            "vep_transcript_id",
    # AlphaMissense and hotspot columns appear when those plugins are enabled
    "vibe.alphamissense.score":      "am_score",
    "vibe.alphamissense.class":      "am_class",
    "vibe.hotspot_type":             "hotspot_type",
    # IMPACT not in vibe-vep output; derive from consequence if needed
}

# Consequence → IMPACT mapping (SO terms)
HIGH_IMPACT = {
    "transcript_ablation", "splice_acceptor_variant", "splice_donor_variant",
    "stop_gained", "frameshift_variant", "stop_lost", "start_lost",
    "transcript_amplification",
}
MODERATE_IMPACT = {
    "inframe_insertion", "inframe_deletion", "missense_variant",
    "protein_altering_variant", "regulatory_region_ablation",
}
LOW_IMPACT = {
    "splice_region_variant", "incomplete_terminal_codon_variant",
    "start_retained_variant", "stop_retained_variant", "synonymous_variant",
    "coding_sequence_variant",
}


def consequence_to_impact(consequence: str | None) -> str | None:
    """Derive VEP IMPACT tier from SO consequence term."""
    if not consequence:
        return None
    primary = consequence.split(",")[0].strip()
    if primary in HIGH_IMPACT:
        return "HIGH"
    if primary in MODERATE_IMPACT:
        return "MODERATE"
    if primary in LOW_IMPACT:
        return "LOW"
    return "MODIFIER"


def detect_assembly(conn, study_id: str) -> str:
    """Return 'GRCh37' or 'GRCh38' based on NCBI_Build column in mutations table."""
    try:
        row = conn.execute(
            f'SELECT NCBI_Build FROM "{study_id}_mutations" WHERE NCBI_Build IS NOT NULL LIMIT 1'
        ).fetchone()
        if row:
            build = str(row[0]).upper()
            if "37" in build or "19" in build:
                return "GRCh37"
    except Exception:
        pass
    return "GRCh38"


def export_mutations_to_maf(conn, study_id: str, tmp_path: Path) -> int:
    """Export study mutations to a minimal MAF file for vibe-vep input.

    Returns the number of rows exported.
    """
    table = f'"{study_id}_mutations"'
    existing = {
        row[0]
        for row in conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{study_id}_mutations'"
        ).fetchall()
    }

    select_parts = []
    for col in MAF_EXPORT_COLS:
        if col in existing:
            select_parts.append(f'"{col}"')
        else:
            select_parts.append(f"NULL AS \"{col}\"")

    select_sql = ", ".join(select_parts)
    out_file = str(tmp_path)

    conn.execute(
        f"COPY (SELECT {select_sql} FROM {table}) TO '{out_file}' (DELIMITER '\t', HEADER)"
    )

    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return count


def parse_vep_output(path: Path) -> dict[tuple, dict]:
    """Parse annotated MAF output from vibe-vep.

    Returns a dict keyed by (hugo_symbol, chromosome, start_pos, ref_allele, alt_allele)
    mapping to a dict of annotation fields (vep_impact, vep_consequence, etc.).
    """
    lookup: dict[tuple, dict] = {}

    with open(path, newline="") as fh:
        lines = [l for l in fh if not l.startswith("#")]

    if not lines:
        return lookup

    reader = csv.DictReader(lines, delimiter="\t")
    for row in reader:
        key = (
            row.get("Hugo_Symbol", ""),
            row.get("Chromosome", ""),
            row.get("Start_Position", ""),
            row.get("Reference_Allele", ""),
            row.get("Tumor_Seq_Allele2", ""),
        )

        ann: dict = {}
        for vibe_col, dest_col in VEP_COLS.items():
            val = row.get(vibe_col)
            if val and val not in ("", ".", "NA", "N/A"):
                if dest_col == "am_score":
                    try:
                        val = float(val)
                    except ValueError:
                        val = None
                ann[dest_col] = val
            else:
                ann[dest_col] = None

        # Derive vep_impact from consequence
        ann["vep_impact"] = consequence_to_impact(ann.get("vep_consequence"))

        lookup[key] = ann

    return lookup
