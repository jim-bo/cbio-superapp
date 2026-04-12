"""Fuzzy study_id resolver — deterministic, no LLM."""
from __future__ import annotations


def resolve_study_id(conn, query: str) -> tuple[str | None, list[dict]]:
    """Resolve free-text query to a study_id.

    Returns (resolved_id, candidates).

    Resolution order:
      1. Exact study_id match (case-insensitive) → (study_id, [{single candidate}])
      2. Exact match on studies.name (case-insensitive) → (study_id, [{single}])
      3. Substring matches across study_id, name, type_of_cancer, category.
         Rank by quality: exact > prefix > substring; tiebreak on len(study_id) asc.
         - 1 result → (study_id, [{single}])
         - 2..5 results → (None, candidates list for disambiguation)
         - >5 results → (None, top 5 by score)
      4. No matches → (None, [])
    """
    if not query or not query.strip():
        return None, []

    q = query.strip()
    q_lower = q.lower()

    # 1. Exact study_id match (case-insensitive)
    rows = conn.execute(
        "SELECT study_id, name, type_of_cancer FROM studies WHERE LOWER(study_id) = ?",
        [q_lower],
    ).fetchall()
    if rows:
        r = rows[0]
        return r[0], [{"study_id": r[0], "name": r[1], "type_of_cancer": r[2]}]

    # 2. Exact name match (case-insensitive)
    rows = conn.execute(
        "SELECT study_id, name, type_of_cancer FROM studies WHERE LOWER(name) = ?",
        [q_lower],
    ).fetchall()
    if len(rows) == 1:
        r = rows[0]
        return r[0], [{"study_id": r[0], "name": r[1], "type_of_cancer": r[2]}]

    # 3. Substring matches across multiple columns
    rows = conn.execute(
        """
        SELECT study_id, name, type_of_cancer, category
        FROM studies
        WHERE LOWER(study_id) LIKE '%' || ? || '%'
           OR LOWER(name) LIKE '%' || ? || '%'
           OR LOWER(type_of_cancer) LIKE '%' || ? || '%'
           OR LOWER(COALESCE(category, '')) LIKE '%' || ? || '%'
        """,
        [q_lower, q_lower, q_lower, q_lower],
    ).fetchall()

    if not rows:
        return None, []

    def _score(row) -> int:
        study_id, name, toc, category = row
        score = 0
        for field, field_bonus in [
            (study_id, 5),
            (name, 3),
            (toc, 0),
            (category or "", 0),
        ]:
            fl = (field or "").lower()
            if fl == q_lower:
                score = max(score, 100 + field_bonus)
            elif fl.startswith(q_lower):
                score = max(score, 50 + field_bonus)
            elif q_lower in fl:
                score = max(score, 20 + field_bonus)
        return score

    scored = sorted(rows, key=lambda r: (-_score(r), len(r[0])))
    top5 = scored[:5]

    candidates = [
        {"study_id": r[0], "name": r[1], "type_of_cancer": r[2]} for r in top5
    ]

    if len(scored) == 1:
        return candidates[0]["study_id"], candidates

    return None, candidates
