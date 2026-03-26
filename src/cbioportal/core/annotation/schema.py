"""Schema constants for the variant_annotations table.

ANNOTATION_COLUMNS is the single source of truth for:
  - DDL in writer.py
  - SCHEMA.md documentation
"""
from __future__ import annotations

ANNOTATION_COLUMNS: list[dict] = [
    # ── Identity ──────────────────────────────────────────────────────────────
    {"name": "study_id",                "type": "VARCHAR",   "source": "injected",          "description": "FK to studies table"},
    {"name": "alteration_type",         "type": "VARCHAR",   "source": "derived",           "description": "'MUTATION' | 'CNA' | 'SV'"},
    {"name": "sample_id",               "type": "VARCHAR",   "source": "source table",      "description": "Tumor_Sample_Barcode for mutations"},
    {"name": "hugo_symbol",             "type": "VARCHAR",   "source": "source table",      "description": "Hugo-normalized gene symbol"},
    # ── Alteration detail ────────────────────────────────────────────────────
    {"name": "hgvsp_short",             "type": "VARCHAR",   "source": "mutations table",   "description": "e.g. p.G12D; NULL for CNA/SV"},
    {"name": "variant_classification",  "type": "VARCHAR",   "source": "mutations table",   "description": "e.g. Missense_Mutation; NULL for CNA/SV"},
    {"name": "cna_value",               "type": "DOUBLE",    "source": "cna table",         "description": "±2; NULL for MUTATION/SV"},
    {"name": "sv_class",                "type": "VARCHAR",   "source": "sv table",          "description": "e.g. FUSION; NULL for MUTATION/CNA"},
    {"name": "sv_partner_gene",         "type": "VARCHAR",   "source": "sv table",          "description": "Site1/Site2 partner; NULL for MUTATION/CNA"},
    # ── vibe-vep ─────────────────────────────────────────────────────────────
    {"name": "vep_impact",              "type": "VARCHAR",   "source": "vibe-vep IMPACT",   "description": "HIGH/MODERATE/LOW/MODIFIER; NULL if vep unavailable"},
    {"name": "vep_consequence",         "type": "VARCHAR",   "source": "vibe-vep Consequence", "description": "SO term e.g. missense_variant"},
    {"name": "vep_transcript_id",       "type": "VARCHAR",   "source": "vibe-vep Transcript_ID", "description": "Ensembl transcript ID"},
    {"name": "vep_exon_number",         "type": "VARCHAR",   "source": "vibe-vep Exon_Number", "description": "e.g. 2/10"},
    {"name": "am_score",                "type": "DOUBLE",    "source": "vibe-vep am_score", "description": "AlphaMissense 0-1 (requires optional download)"},
    {"name": "am_class",                "type": "VARCHAR",   "source": "vibe-vep am_class", "description": "likely_pathogenic/ambiguous/likely_benign"},
    {"name": "hotspot_type",            "type": "VARCHAR",   "source": "vibe-vep hotspot_type", "description": "single_residue/in-frame indel; NULL if not hotspot"},
    # ── Mutation effect ───────────────────────────────────────────────────────
    {"name": "mutation_effect",         "type": "VARCHAR",   "source": "CIViC → IntOGen",   "description": "Gain-of-function/Loss-of-function/Unknown"},
    {"name": "mutation_effect_source",  "type": "VARCHAR",   "source": "derived",           "description": "'civic'/'intogen'/'unknown'"},
    # ── MOAlmanac ─────────────────────────────────────────────────────────────
    {"name": "moalmanac_score_bin",     "type": "VARCHAR",   "source": "MOAlmanac assertions", "description": "e.g. Putatively Actionable"},
    {"name": "moalmanac_oncogenic",     "type": "VARCHAR",   "source": "MOAlmanac",         "description": "Oncogenic/Likely Oncogenic/VUS; NULL if no match"},
    {"name": "moalmanac_clinical_significance", "type": "VARCHAR", "source": "MOAlmanac predictive_implication", "description": "FDA-Approved/Guideline/Clinical trial/etc."},
    {"name": "moalmanac_drug",          "type": "VARCHAR",   "source": "MOAlmanac therapy_name", "description": "Drug name; NULL if no assertion"},
    {"name": "moalmanac_disease",       "type": "VARCHAR",   "source": "MOAlmanac oncotree_term", "description": "Disease context"},
    # ── CIViC ────────────────────────────────────────────────────────────────
    {"name": "civic_evidence_id",       "type": "INTEGER",   "source": "CIViC",             "description": "Evidence item ID for citation; NULL if no match"},
    {"name": "civic_evidence_level",    "type": "VARCHAR",   "source": "CIViC",             "description": "A-E"},
    {"name": "civic_clinical_significance", "type": "VARCHAR", "source": "CIViC",           "description": "e.g. Sensitivity/Response"},
    {"name": "civic_drugs",             "type": "VARCHAR",   "source": "CIViC",             "description": "Pipe-joined drug names"},
    # ── IntOGen ───────────────────────────────────────────────────────────────
    {"name": "intogen_role",            "type": "VARCHAR",   "source": "IntOGen",           "description": "Act/LoF/Amb (gene level, cancer-type matched)"},
    # ── OncoKB stubs (always NULL until future integration) ───────────────────
    {"name": "oncokb_oncogenic",        "type": "VARCHAR",   "source": "—",                 "description": "Reserved; set NULL. TODO: OncoKB /annotate/mutations"},
    {"name": "oncokb_mutation_effect",  "type": "VARCHAR",   "source": "—",                 "description": "Reserved; set NULL. TODO: OncoKB mutationEffect"},
    {"name": "oncokb_highest_sensitive_level", "type": "VARCHAR", "source": "—",           "description": "Reserved; set NULL. TODO: OncoKB highestSensitiveLevel"},
    # ── Audit ─────────────────────────────────────────────────────────────────
    {"name": "annotated_at",            "type": "TIMESTAMP", "source": "CURRENT_TIMESTAMP", "description": "When annotations were last written"},
]


def build_create_ddl(table_name: str) -> str:
    """Return CREATE TABLE DDL for the given table name."""
    col_defs = ",\n    ".join(
        f"{col['name']} {col['type']}" for col in ANNOTATION_COLUMNS
    )
    return f"CREATE TABLE {table_name} (\n    {col_defs}\n)"
