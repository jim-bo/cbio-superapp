"""
One-time migration script: convert genomic matrix value columns from DOUBLE to FLOAT.

Affected tables: *_cna, *_expression, *_methylation, *_protein
Savings: ~8-10 GB on a 23 GB database (halves storage for those value columns).

FLOAT is safe for all genomic matrix data:
- CNA values are only: -2.0, -1.5, -1.0, 1.0, 2.0 (all exactly representable in float32)
- Expression, methylation, protein don't need 15 decimal digits of precision

Usage:
    uv run python scripts/optimize_db.py <path/to/cbioportal.duckdb> [--dry-run]
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

import duckdb

# Suffixes to migrate (value column name per suffix)
GENOMIC_SUFFIXES: dict[str, str] = {
    "_cna": "cna_value",
    "_expression": "expression_value",
    "_methylation": "methylation_value",
    "_protein": "protein_value",
}

# Suffixes to skip entirely (derived/computed tables that get rebuilt by the loader)
SKIP_SUFFIXES = (
    "_genomic_event_derived",
    "_profiled_counts",
    "_mutations",
    "_patient",
    "_sample",
    "_sv",
    "_gene_panel",
)

CHECKPOINT_EVERY = 25


def get_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]


def get_value_col_type(conn: duckdb.DuckDBPyConnection, table: str, col: str) -> str | None:
    rows = conn.execute(f'DESCRIBE "{table}"').fetchall()
    for row in rows:
        if row[0] == col:
            return row[1]
    return None


def migrate_table(conn: duckdb.DuckDBPyConnection, table: str, value_col: str, dry_run: bool) -> str:
    """
    Returns: 'skipped' | 'already_float' | 'migrated' | 'error:<msg>'
    """
    col_type = get_value_col_type(conn, table, value_col)
    if col_type is None:
        return f"skipped:no_{value_col}_column"
    if col_type.upper() == "FLOAT":
        return "already_float"

    tmp = f'"{table}_opt"'
    original = f'"{table}"'

    if dry_run:
        return f"would_migrate:{col_type}→FLOAT"

    try:
        # Clean up any leftover tmp table from an interrupted run
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")
        conn.execute(f"""
            CREATE TABLE {tmp} AS
            SELECT study_id, hugo_symbol, sample_id,
                   CAST({value_col} AS FLOAT) AS {value_col}
            FROM {original}
        """)
        conn.execute(f"DROP TABLE {original}")
        conn.execute(f"ALTER TABLE {tmp} RENAME TO \"{table}\"")
        return "migrated"
    except Exception as e:
        # Try to clean up tmp if it was created
        try:
            conn.execute(f"DROP TABLE IF EXISTS {tmp}")
        except Exception:
            pass
        return f"error:{e}"


def compact_database(db_path: Path) -> None:
    """Export to Parquet and reimport to physically shrink the file."""
    export_dir = Path("/tmp/cbio_optimize_export")
    if export_dir.exists():
        shutil.rmtree(export_dir)
    export_dir.mkdir(parents=True)

    print("\nCompacting database (EXPORT/IMPORT to reclaim disk space)...")
    conn = duckdb.connect(str(db_path))
    print(f"  Exporting to {export_dir} ...")
    conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
    conn.close()

    bak_path = Path(str(db_path) + ".bak")
    print(f"  Moving original to {bak_path} ...")
    shutil.move(str(db_path), str(bak_path))

    print(f"  Importing into fresh {db_path} ...")
    new_conn = duckdb.connect(str(db_path))
    new_conn.execute(f"IMPORT DATABASE '{export_dir}'")
    new_conn.close()

    new_size = db_path.stat().st_size
    old_size = bak_path.stat().st_size
    saved = old_size - new_size
    print(f"  Before: {old_size / 1e9:.2f} GB")
    print(f"  After:  {new_size / 1e9:.2f} GB")
    print(f"  Saved:  {saved / 1e9:.2f} GB ({100 * saved / old_size:.1f}%)")

    shutil.rmtree(export_dir)
    print(f"  Backup left at {bak_path} — delete manually once satisfied.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert genomic matrix columns DOUBLE → FLOAT")
    parser.add_argument("db_path", type=Path, help="Path to cbioportal.duckdb")
    parser.add_argument("--dry-run", action="store_true", help="Report what would change without modifying the DB")
    parser.add_argument("--skip-compact", action="store_true", help="Skip the EXPORT/IMPORT compaction step")
    args = parser.parse_args()

    if not args.db_path.exists():
        print(f"Error: {args.db_path} does not exist", file=sys.stderr)
        sys.exit(1)

    print(f"Opening {args.db_path} ({args.db_path.stat().st_size / 1e9:.2f} GB)")
    if args.dry_run:
        print("DRY RUN — no changes will be made\n")

    conn = duckdb.connect(str(args.db_path))
    all_tables = get_tables(conn)
    print(f"Found {len(all_tables)} tables total\n")

    # Find candidate tables
    candidates: list[tuple[str, str]] = []
    for table in all_tables:
        if any(table.endswith(skip) for skip in SKIP_SUFFIXES):
            continue
        for suffix, value_col in GENOMIC_SUFFIXES.items():
            if table.endswith(suffix):
                candidates.append((table, value_col))
                break

    print(f"Candidates for migration: {len(candidates)}")
    for suffix, value_col in GENOMIC_SUFFIXES.items():
        n = sum(1 for t, _ in candidates if t.endswith(suffix))
        print(f"  *{suffix}: {n} tables ({value_col})")
    print()

    counts = {"migrated": 0, "already_float": 0, "skipped": 0, "error": 0}
    start = time.time()

    for i, (table, value_col) in enumerate(candidates, 1):
        result = migrate_table(conn, table, value_col, args.dry_run)

        if result == "migrated":
            counts["migrated"] += 1
            status = "✓"
        elif result == "already_float":
            counts["already_float"] += 1
            status = "–"
        elif result.startswith("would_migrate"):
            counts["migrated"] += 1
            status = "~"
        elif result.startswith("skipped"):
            counts["skipped"] += 1
            status = "?"
        else:
            counts["error"] += 1
            status = "✗"
            print(f"  [{i}/{len(candidates)}] {status} {table}: {result}")
            continue

        if i % 50 == 0 or result.startswith("error"):
            elapsed = time.time() - start
            rate = i / elapsed
            remaining = (len(candidates) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(candidates)}] {counts['migrated']} migrated, "
                  f"{counts['already_float']} already FLOAT, "
                  f"{counts['error']} errors — "
                  f"{elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining")

        if not args.dry_run and i % CHECKPOINT_EVERY == 0:
            conn.execute("CHECKPOINT")

    if not args.dry_run:
        conn.execute("CHECKPOINT")

    conn.close()

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Migrated:      {counts['migrated']}")
    print(f"  Already FLOAT: {counts['already_float']}")
    print(f"  Skipped:       {counts['skipped']}")
    print(f"  Errors:        {counts['error']}")

    if counts["error"] > 0:
        print("\nErrors occurred — review output above before compacting.", file=sys.stderr)
        sys.exit(1)

    if not args.dry_run and not args.skip_compact:
        compact_database(args.db_path)


if __name__ == "__main__":
    main()
