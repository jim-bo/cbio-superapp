"""Write variant annotations to the study DuckDB table."""
from __future__ import annotations

import logging
from datetime import datetime

from .schema import ANNOTATION_COLUMNS, build_create_ddl

logger = logging.getLogger(__name__)

TABLE_SUFFIX = "variant_annotations"


def write_variant_annotations(conn, study_id: str, rows: list[dict]) -> int:
    """Drop and recreate {study_id}_variant_annotations, then bulk-insert rows.

    Returns the number of rows inserted.
    """
    table_name = f'"{study_id}_{TABLE_SUFFIX}"'

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(build_create_ddl(table_name))

    if not rows:
        logger.info("No annotation rows for %s", study_id)
        return 0

    now = datetime.now().replace(microsecond=0)
    col_names = [col["name"] for col in ANNOTATION_COLUMNS]

    tuples = []
    for row in rows:
        row["annotated_at"] = now
        tuples.append(tuple(row.get(c) for c in col_names))

    placeholders = ", ".join(["?"] * len(col_names))
    col_list = ", ".join(col_names)
    conn.executemany(
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
        tuples,
    )

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info("Wrote %d annotation rows for %s", count, study_id)
    return count
