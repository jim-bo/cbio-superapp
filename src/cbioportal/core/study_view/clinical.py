"""Clinical attribute queries: counts, data table, and attribute listing."""
from __future__ import annotations

import math

from .filters import _build_filter_subquery, get_clinical_attributes
from .colors import RESERVED_COLORS, CBIOPORTAL_D3_COLORS


_CLEAN_INTERVALS = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000]


def _auto_bin_size(val_range: float) -> float:
    """Choose a clean bin size to yield roughly 10–20 bins."""
    if val_range <= 0:
        return 1.0
    raw = val_range / 15  # target ~15 bins
    magnitude = 10 ** math.floor(math.log10(raw))
    for m in _CLEAN_INTERVALS:
        candidate = m * magnitude
        if candidate >= raw:
            return float(candidate)
    return float(magnitude * 1000)


def get_numeric_histogram(
    conn,
    study_id: str,
    attribute_id: str,
    filter_json: str | None = None,
    bin_size: float | None = None,
    clip_min: float | None = None,
    clip_max: float | None = None,
) -> list[dict]:
    """Return equal-width histogram bins for a numeric clinical attribute.

    Auto-computes bin_size when not provided to yield roughly 10–20 bins.
    When clip_min/clip_max are set, values at or below clip_min are collapsed
    into a ``≤{clip_min}`` bin and values above clip_max into ``>{clip_max}``,
    matching the legacy cBioPortal age histogram style.
    Returns [{"x": "0-10", "y": count}, ..., {"x": "NA", "y": na_count}].
    The "NA" entry is only included when na_count > 0.
    """
    attrs = get_clinical_attributes(conn, study_id)
    source = attrs.get(attribute_id, "sample")
    table = f'"{study_id}_{source}"'
    sample_table = f'"{study_id}_sample"'
    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    if source == "patient":
        id_filter = (
            f't.PATIENT_ID IN '
            f'(SELECT PATIENT_ID FROM {sample_table} WHERE SAMPLE_ID IN ({filter_sql}))'
        )
    else:
        id_filter = f't.SAMPLE_ID IN ({filter_sql})'

    # Compute min/max for auto bin size
    try:
        row = conn.execute(
            f'SELECT MIN(TRY_CAST(t."{attribute_id}" AS DOUBLE)), '
            f'MAX(TRY_CAST(t."{attribute_id}" AS DOUBLE)) '
            f'FROM {table} t WHERE {id_filter}',
            list(params),
        ).fetchone()
    except Exception:
        return []

    if row is None or row[0] is None or row[1] is None:
        return []

    val_min, val_max = row
    val_range = val_max - val_min

    if bin_size is None:
        bin_size = _auto_bin_size(val_range)

    if bin_size <= 0:
        bin_size = 1.0

    try:
        sql = (
            f'SELECT FLOOR(TRY_CAST(t."{attribute_id}" AS DOUBLE) / ?) * ? AS bin_start, '
            f'COUNT(*) AS cnt '
            f'FROM {table} t '
            f'WHERE {id_filter} '
            f'AND TRY_CAST(t."{attribute_id}" AS DOUBLE) IS NOT NULL '
            f'GROUP BY bin_start '
            f'ORDER BY bin_start'
        )
        rows = conn.execute(sql, list(params) + [bin_size, bin_size]).fetchall()
    except Exception:
        return []

    result = []
    for r in rows:
        if r[0] is None:
            continue
        start = r[0]
        end = start + bin_size
        if bin_size == int(bin_size) and start == int(start) and end == int(end):
            label = f"{int(start)}-{int(end)}"
        else:
            label = f"{start:.1f}-{end:.1f}"
        result.append({"x": label, "y": r[1]})

    # Collapse bins outside clip_min/clip_max into edge bins
    if clip_min is not None and result:
        clip_label = f"≤{int(clip_min)}" if clip_min == int(clip_min) else f"≤{clip_min}"
        low_count = 0
        kept = []
        for item in result:
            # Parse the bin start from the label
            try:
                bin_start = float(item["x"].split("-")[0])
            except ValueError:
                kept.append(item)
                continue
            if bin_start < clip_min:
                low_count += item["y"]
            else:
                kept.append(item)
        if low_count > 0:
            kept.insert(0, {"x": clip_label, "y": low_count})
        result = kept

    if clip_max is not None and result:
        clip_label = f">{int(clip_max)}" if clip_max == int(clip_max) else f">{clip_max}"
        high_count = 0
        kept = []
        for item in result:
            try:
                bin_end = float(item["x"].split("-")[1]) if "-" in item["x"] else float("inf")
            except (ValueError, IndexError):
                kept.append(item)
                continue
            if bin_end > clip_max + bin_size:
                high_count += item["y"]
            else:
                kept.append(item)
        if high_count > 0:
            kept.append({"x": clip_label, "y": high_count})
        result = kept

    # NA count
    try:
        na_sql = (
            f'SELECT COUNT(*) FROM {table} t '
            f'WHERE {id_filter} '
            f'AND TRY_CAST(t."{attribute_id}" AS DOUBLE) IS NULL'
        )
        na_count = conn.execute(na_sql, list(params)).fetchone()[0]
        if na_count > 0:
            result.append({"x": "NA", "y": na_count})
    except Exception:
        pass

    return result


def get_clinical_counts(
    conn,
    study_id: str,
    attribute_id: str,
    source_table: str = "sample",
    filter_json: str | None = None,
) -> list[dict]:
    """Return [{value, count, pct, color}] sorted by count desc for a clinical attribute."""
    table = f'"{study_id}_{source_table}"'
    col = f'"{attribute_id}"'

    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    try:
        # Join the filtered sample list against the attribute table
        if source_table == "sample":
            sql = f"""
                SELECT
                    COALESCE(CAST(t.{col} AS VARCHAR), 'NA') AS val,
                    COUNT(*) AS cnt
                FROM {table} t
                WHERE t.SAMPLE_ID IN ({filter_sql})
                GROUP BY val
                ORDER BY cnt DESC, val ASC
                LIMIT 100
            """
        else:
            # Patient table join - count distinct patients to match cBioPortal behavior
            sql = f"""
                SELECT
                    COALESCE(CAST(p.{col} AS VARCHAR), 'NA') AS val,
                    COUNT(DISTINCT p.PATIENT_ID) AS cnt
                FROM "{study_id}_sample" s
                JOIN {table} p ON s.PATIENT_ID = p.PATIENT_ID
                WHERE s.SAMPLE_ID IN ({filter_sql})
                GROUP BY val
                ORDER BY cnt DESC, val ASC
                LIMIT 100
            """
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    total = sum(r[1] for r in rows) or 1

    results = []
    for i, r in enumerate(rows):
        value = r[0]
        count = r[1]
        v_lower = str(value).lower().strip()

        # 1. Check Reserved Colors first
        if v_lower in RESERVED_COLORS:
            color = RESERVED_COLORS[v_lower]
        else:
            # 2. Assign by Rank (Order of Frequency) to match cBioPortal aesthetic
            color = CBIOPORTAL_D3_COLORS[i % len(CBIOPORTAL_D3_COLORS)]

        results.append({
            "value": value,
            "count": count,
            "pct": round(count / total * 100, 1),
            "color": color
        })

    return results


def get_all_clinical_counts(
    conn,
    study_id: str,
    filter_json: str | None = None,
) -> dict[str, list[dict]]:
    """Return clinical counts for every available attribute."""
    attrs = get_clinical_attributes(conn, study_id)
    result: dict[str, list[dict]] = {}
    for attr_id, source in attrs.items():
        result[attr_id] = get_clinical_counts(
            conn, study_id, attr_id, source, filter_json
        )
    return result


def get_clinical_data_table(
    conn,
    study_id: str,
    filter_json: str | None = None,
    search: str | None = None,
    sort_col: str | None = None,
    sort_dir: str = "asc",
    offset: int = 0,
    limit: int = 20,
) -> dict:
    """Return clinical data rows, column metadata, and total count for the clinical data tab."""
    # 1. Fetch column metadata
    cols_meta = conn.execute(
        "SELECT attr_id, display_name, datatype, patient_attribute "
        "FROM clinical_attribute_meta WHERE study_id = ? "
        "ORDER BY priority ASC, attr_id ASC",
        (study_id,)
    ).fetchall()

    columns = []
    for cid, dn, dtype, is_patient in cols_meta:
        columns.append({
            "id": cid,
            "name": dn,
            "datatype": dtype,
            "is_patient": bool(is_patient)
        })

    # 2. Build Query
    select_parts = ['s.SAMPLE_ID', 's.PATIENT_ID']
    for col in columns:
        prefix = 'p' if col['is_patient'] else 's'
        select_parts.append(f'{prefix}."{col["id"]}"')

    filter_sql, params = _build_filter_subquery(conn, study_id, filter_json)

    base_sql = (
        f"FROM \"{study_id}_sample\" s "
        f"JOIN \"{study_id}_patient\" p ON s.PATIENT_ID = p.PATIENT_ID "
        f"WHERE s.SAMPLE_ID IN ({filter_sql})"
    )

    query_params = list(params)

    # 3. Add Search
    if search:
        search_clauses = []
        search_term = f"%{search}%"
        search_clauses.append("CAST(s.SAMPLE_ID AS VARCHAR) ILIKE ?")
        search_clauses.append("CAST(s.PATIENT_ID AS VARCHAR) ILIKE ?")
        query_params.extend([search_term, search_term])

        for col in columns:
            prefix = 'p' if col['is_patient'] else 's'
            search_clauses.append(f'CAST({prefix}."{col["id"]}" AS VARCHAR) ILIKE ?')
            query_params.append(search_term)

        base_sql += f" AND ({' OR '.join(search_clauses)})"

    # 4. Get Total Count
    total_count = conn.execute(f"SELECT COUNT(*) {base_sql}", query_params).fetchone()[0]

    # 5. Sorting
    order_by = ""
    if sort_col:
        found_col = next((c for c in columns if c['id'] == sort_col), None)
        if found_col:
            prefix = 'p' if found_col['is_patient'] else 's'
            order_by = f'ORDER BY {prefix}."{sort_col}" {sort_dir} NULLS LAST'
        elif sort_col == 'SAMPLE_ID':
            order_by = f'ORDER BY s.SAMPLE_ID {sort_dir} NULLS LAST'
        elif sort_col == 'PATIENT_ID':
            order_by = f'ORDER BY s.PATIENT_ID {sort_dir} NULLS LAST'
    else:
        order_by = "ORDER BY s.SAMPLE_ID ASC"

    # 6. Final Query with Pagination
    final_sql = f"SELECT {', '.join(select_parts)} {base_sql} {order_by} LIMIT ? OFFSET ?"
    query_params.extend([limit, offset])

    rows_raw = conn.execute(final_sql, query_params).fetchall()

    column_names = ['SAMPLE_ID', 'PATIENT_ID'] + [c['id'] for c in columns]

    data = []
    for row in rows_raw:
        data.append(dict(zip(column_names, row)))

    return {
        "data": data,
        "columns": columns,
        "total_count": total_count,
        "offset": offset,
        "limit": limit
    }
