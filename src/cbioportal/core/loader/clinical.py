"""Clinical data loading: header parsing and clinical attribute metadata."""
from pathlib import Path


def parse_clinical_headers(file_path: Path, patient_attribute: bool) -> list[dict]:
    """Parse 4 metadata header rows from a cBioPortal clinical file.

    Format (comment lines before the column-header line):
        #Display Name\t<col1>\t<col2>\t...
        #Description\t<col1>\t<col2>\t...
        #Datatype\tNUMBER|STRING|BOOLEAN\t...
        #Priority\t1\t0\t...
        PATIENT_ID\tCOL1\tCOL2\t...  (first non-# line = column names)

    Returns a list of attribute dicts, excluding PATIENT_ID and SAMPLE_ID.
    """
    if not file_path.exists():
        return []

    meta_lines: list[str] = []
    col_names: list[str] = []

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            stripped = raw.rstrip("\n\r")
            if stripped.startswith("#"):
                meta_lines.append(stripped[1:])
            else:
                col_names = stripped.split("\t")
                break

    if len(meta_lines) < 4 or not col_names:
        return []

    display_names = meta_lines[0].split("\t")
    descriptions  = meta_lines[1].split("\t")
    datatypes     = meta_lines[2].split("\t")
    priorities    = meta_lines[3].split("\t")

    results: list[dict] = []
    for i, col in enumerate(col_names):
        col = col.strip()
        if not col or col in ("PATIENT_ID", "SAMPLE_ID"):
            continue
        try:
            priority = int(priorities[i]) if i < len(priorities) else 1
        except (ValueError, IndexError):
            priority = 1
        results.append({
            "attr_id":           col,
            "display_name":      display_names[i].strip() if i < len(display_names) else col,
            "description":       descriptions[i].strip()  if i < len(descriptions)  else "",
            "datatype":          datatypes[i].strip().upper() if i < len(datatypes) else "STRING",
            "priority":          priority,
            "patient_attribute": patient_attribute,
        })
    return results


def _upsert_clinical_attribute_meta(conn, study_id: str, attrs: list[dict]) -> None:
    """Create clinical_attribute_meta table if needed and upsert attrs for a study."""
    if not attrs:
        return
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clinical_attribute_meta (
            study_id          VARCHAR NOT NULL,
            attr_id           VARCHAR NOT NULL,
            display_name      VARCHAR,
            description       VARCHAR,
            datatype          VARCHAR,
            patient_attribute BOOLEAN,
            priority          INTEGER,
            PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute("DELETE FROM clinical_attribute_meta WHERE study_id = ?", (study_id,))
    rows = [
        (study_id, a["attr_id"], a["display_name"], a["description"],
         a["datatype"], a["patient_attribute"], a["priority"])
        for a in attrs
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
