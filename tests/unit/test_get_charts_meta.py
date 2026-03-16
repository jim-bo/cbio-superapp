"""Unit tests for get_charts_meta — dynamic chart layout from clinical_attribute_meta."""

import duckdb
import pytest

from cbioportal.core.study_view_repository import get_charts_meta

from tests.unit.conftest import STUDY


# ---------------------------------------------------------------------------
# Tests using the db_with_clinical_meta fixture
# ---------------------------------------------------------------------------

def test_results_ordered_by_priority_desc(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    # Extract priorities of clinical (non-special) charts
    clinical = [c for c in charts if not c["attr_id"].startswith("_")]
    priorities = [c["priority"] for c in clinical]
    assert priorities == sorted(priorities, reverse=True)


def test_priority_zero_excluded(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    attr_ids = [c["attr_id"] for c in charts]
    assert "HIDDEN_ATTR" not in attr_ids


def test_string_datatype_gives_pie(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    gender = next(c for c in charts if c["attr_id"] == "GENDER")
    assert gender["chart_type"] == "pie"


def test_number_datatype_gives_bar(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    age = next(c for c in charts if c["attr_id"] == "AGE")
    assert age["chart_type"] == "bar"


def test_cancer_type_overrides_string_to_table(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    ct = next(c for c in charts if c["attr_id"] == "CANCER_TYPE")
    assert ct["chart_type"] == "table"


def test_mutated_genes_appended_when_mutation_data_type(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    attr_ids = [c["attr_id"] for c in charts]
    assert "_mutated_genes" in attr_ids


def test_genomic_chart_priorities(db_with_clinical_meta):
    """Genomic special charts must have correct non-zero priorities."""
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    by_id = {c["attr_id"]: c for c in charts}
    # fixture only has "mutation" data type, so only _mutated_genes appears
    assert by_id["_mutated_genes"]["priority"] == 90


def test_genomic_chart_priorities_all_types():
    """With mutation+cna+sv all genomic charts get correct priorities."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER, PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    for dt in ("mutation", "cna", "sv"):
        conn.execute("INSERT INTO study_data_types VALUES (?, ?)", (STUDY, dt))
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    by_id = {c["attr_id"]: c for c in charts}
    assert by_id["_mutated_genes"]["priority"] == 90
    assert by_id["_cna_genes"]["priority"] == 80
    assert by_id["_sv_genes"]["priority"] == 70
    assert by_id["_scatter"]["priority"] == 50


def test_no_chart_has_priority_zero(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    zero_priority = [c["attr_id"] for c in charts if c["priority"] == 0]
    assert zero_priority == [], f"Charts with priority=0: {zero_priority}"


def test_no_special_charts_when_data_types_empty():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, CANCER_TYPE VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER, PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute("INSERT INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?, ?)",
                 (STUDY, "CANCER_TYPE", "Cancer Type", "", "STRING", False, 3000))
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    # No rows in study_data_types
    charts = get_charts_meta(conn, STUDY)
    special = [c for c in charts if c["attr_id"].startswith("_")]
    assert special == []
    conn.close()


def test_backward_compat_fallback_when_no_meta_table():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, CANCER_TYPE VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR, GENDER VARCHAR)')
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    # No clinical_attribute_meta table
    charts = get_charts_meta(conn, STUDY)
    assert len(charts) > 0, "Fallback should return non-empty results"
    attr_ids = [c["attr_id"] for c in charts]
    assert "CANCER_TYPE" in attr_ids or "GENDER" in attr_ids
    conn.close()


def test_pie_dims(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    gender = next(c for c in charts if c["attr_id"] == "GENDER")
    assert gender["w"] == 2
    assert gender["h"] == 5


def test_table_dims(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    ct = next(c for c in charts if c["attr_id"] == "CANCER_TYPE")
    assert ct["w"] == 4
    assert ct["h"] == 10


def test_km_appended_when_os_attrs_present(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    attr_ids = [c["attr_id"] for c in charts]
    assert "_km" in attr_ids


def test_bar_dims(db_with_clinical_meta):
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    age = next(c for c in charts if c["attr_id"] == "AGE")
    assert age["w"] == 4
    assert age["h"] == 5


# ---------------------------------------------------------------------------
# Cardinality-based pie → table promotion tests
# ---------------------------------------------------------------------------

def _make_conn_with_cardinality(n_distinct: int, attr_id: str = "CLINICAL_GROUP", is_patient: bool = False):
    """Create an in-memory DB with a STRING attr having n_distinct distinct values."""
    conn = duckdb.connect(":memory:")
    source = "patient" if is_patient else "sample"
    # Build a table with n_distinct distinct values
    values = [(f"S{i}", f"P{i}", f"val_{i}") for i in range(n_distinct)]
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, {attr_id} VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.executemany(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)', values)
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER, PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute(
        "INSERT INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?, ?)",
        (STUDY, attr_id, attr_id.replace("_", " ").title(), "", "STRING", is_patient, 100),
    )
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    return conn


def test_high_cardinality_string_becomes_table():
    """21 distinct values → chart_type == 'table'."""
    conn = _make_conn_with_cardinality(21)
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    chart = next(c for c in charts if c["attr_id"] == "CLINICAL_GROUP")
    assert chart["chart_type"] == "table", f"Expected 'table', got '{chart['chart_type']}'"
    assert chart["w"] == 4
    assert chart["h"] == 10


def test_low_cardinality_string_stays_pie():
    """5 distinct values → chart_type == 'pie'."""
    conn = _make_conn_with_cardinality(5)
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    chart = next(c for c in charts if c["attr_id"] == "CLINICAL_GROUP")
    assert chart["chart_type"] == "pie", f"Expected 'pie', got '{chart['chart_type']}'"


def test_exactly_threshold_stays_pie():
    """Exactly 20 distinct values → chart_type == 'pie' (threshold is strictly >20)."""
    conn = _make_conn_with_cardinality(20)
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    chart = next(c for c in charts if c["attr_id"] == "CLINICAL_GROUP")
    assert chart["chart_type"] == "pie", f"Expected 'pie', got '{chart['chart_type']}'"


def test_single_value_pie_excluded():
    """A STRING attr with exactly 1 distinct value must be excluded from charts."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, SAMPLE_CLASS VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.executemany(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)', [
        ("S1", "P1", "Tumor"),
        ("S2", "P2", "Tumor"),
    ])
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER, PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute(
        "INSERT INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?, ?)",
        (STUDY, "SAMPLE_CLASS", "Sample Class", "", "STRING", False, 100),
    )
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    attr_ids = [c["attr_id"] for c in charts]
    assert "SAMPLE_CLASS" not in attr_ids, "Single-value pie chart should be excluded"


def test_km_has_pie_dimensions(db_with_clinical_meta):
    """_km chart must use compact dimensions (w=2, h=5), same as a pie chart."""
    charts = get_charts_meta(db_with_clinical_meta, STUDY)
    km = next((c for c in charts if c["attr_id"] == "_km"), None)
    assert km is not None, "_km chart not found"
    assert km["w"] == 2 and km["h"] == 5, f"_km dimensions wrong: w={km['w']} h={km['h']}"


def test_clinical_charts_capped_at_20():
    """More than 20 clinical attrs: only the top-20 by priority appear; special charts are uncapped."""
    conn = duckdb.connect(":memory:")
    # 25 sample columns with distinct priorities
    cols = " ".join(f"ATTR_{i:02d} VARCHAR," for i in range(25))
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, {cols[:-1]})')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER, PRIMARY KEY (study_id, attr_id)
        )
    """)
    for i in range(25):
        conn.execute(
            "INSERT INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?, ?)",
            (STUDY, f"ATTR_{i:02d}", f"Attr {i:02d}", "", "STRING", False, 100 - i),
        )
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    clinical = [c for c in charts if not c["attr_id"].startswith("_")]
    assert len(clinical) == 20, f"Expected 20 clinical charts, got {len(clinical)}"
    # Highest-priority attrs must be the ones kept
    kept_ids = {c["attr_id"] for c in clinical}
    for i in range(20):
        assert f"ATTR_{i:02d}" in kept_ids, f"ATTR_{i:02d} should be in top 20"
    assert "ATTR_20" not in kept_ids, "ATTR_20 should be cut off"


def test_cancer_type_always_table_regardless_of_cardinality():
    """CANCER_TYPE with only 2 distinct values must still be 'table'."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR, CANCER_TYPE VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR)')
    conn.executemany(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?, ?)', [
        ("S1", "P1", "Lung"),
        ("S2", "P2", "Breast"),
    ])
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER, PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.execute(
        "INSERT INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?, ?)",
        (STUDY, "CANCER_TYPE", "Cancer Type", "", "STRING", False, 3000),
    )
    conn.execute("CREATE TABLE study_data_types (study_id VARCHAR, data_type VARCHAR, PRIMARY KEY (study_id, data_type))")
    charts = get_charts_meta(conn, STUDY)
    conn.close()
    ct = next(c for c in charts if c["attr_id"] == "CANCER_TYPE")
    assert ct["chart_type"] == "table", f"CANCER_TYPE should always be 'table', got '{ct['chart_type']}'"
