"""Color palette logic: reserved clinical value colors and D3 fallback palette."""

CBIOPORTAL_D3_COLORS = [
    "#3366cc", "#dc3912", "#ff9900", "#109618", "#990099", "#0099c6", "#dd4477",
    "#66aa00", "#b82e2e", "#316395", "#994499", "#22aa99", "#aaaa11", "#6633cc",
    "#e67300", "#8b0707", "#651067", "#329262", "#5574a6", "#3b3eac", "#b77322",
    "#16d620", "#b91383", "#f4359e", "#9c5935", "#a9c413", "#2a778d", "#668d1c",
    "#bea413", "#0c5922", "#743411"
]

RESERVED_COLORS = {
    "male": "#2986E2",
    "female": "#E0699E",
    "yes": "#1b9e77",
    "no": "#d95f02",
    "true": "#1b9e77",
    "false": "#d95f02",
    "deceased": "#d95f02",
    "living": "#1b9e77",
    "na": "#D3D3D3",
    "unknown": "#A9A9A9"
}


def _hash_string(s: str) -> int:
    """Deterministic hash for consistent color assignment by value string."""
    h = 0
    for char in s:
        h = (31 * h + ord(char)) & 0xFFFFFFFF
    return h


def get_value_color(conn, value: str, attr_id: str = None) -> str:
    """Resolve color based on reserved maps, OncoTree, or D3 fallback."""
    v_lower = str(value).lower().strip()

    # 1. Reserved colors
    if v_lower in RESERVED_COLORS:
        return RESERVED_COLORS[v_lower]

    # 2. OncoTree colors (if it's a cancer type)
    if attr_id == "CANCER_TYPE" or attr_id == "CANCER_TYPE_DETAILED":
        try:
            row = conn.execute("SELECT dedicated_color FROM cancer_types WHERE name = ?", (value,)).fetchone()
            if row and row[0] and row[0] != 'Gainsboro':
                return row[0]
        except Exception:
            pass

    # 3. D3 Fallback (consistent hashing)
    idx = abs(_hash_string(str(value))) % len(CBIOPORTAL_D3_COLORS)
    return CBIOPORTAL_D3_COLORS[idx]
