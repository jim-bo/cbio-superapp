"""Unit tests for get_km_data() and compute_km_curve() using in-memory DuckDB.

KM curve starts at (0.0, 1.0), steps down at each death event.
Censored events decrease n_at_risk without causing a step-down.
Empty cohort or missing OS columns returns [].
"""
import duckdb
import pytest

from cbioportal.core.study_view_repository import get_km_data, compute_km_curve

STUDY = "test_km_study"


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_patient" (
            PATIENT_ID VARCHAR,
            OS_MONTHS DOUBLE,
            OS_STATUS VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR,
            patient_attribute BOOLEAN, priority INTEGER,
            PRIMARY KEY (study_id, attr_id)
        )
    """)
    conn.executemany("INSERT INTO clinical_attribute_meta VALUES (?,?,?,?,?,?,?)", [
        (STUDY, "OS_MONTHS", "OS Months", "", "NUMBER", True, 1),
        (STUDY, "OS_STATUS", "OS Status", "", "STRING", True, 1),
    ])
    yield conn
    conn.close()


def _add_patient(db, patient_id, os_months, os_status):
    db.execute(f'INSERT INTO "{STUDY}_sample" VALUES (?, ?)', (f"S_{patient_id}", patient_id))
    db.execute(f'INSERT INTO "{STUDY}_patient" VALUES (?, ?, ?)', (patient_id, os_months, os_status))


# ---------------------------------------------------------------------------
# compute_km_curve() unit tests (pure math, no DB)
# ---------------------------------------------------------------------------

def test_empty_pairs_returns_empty():
    assert compute_km_curve([]) == []


def test_single_death():
    """1 patient dies at t=12 → S(12) = 0.0."""
    curve = compute_km_curve([(12.0, 1)])
    assert curve[0] == {"time": 0.0, "survival": 1.0}
    assert curve[1] == {"time": 12.0, "survival": 0.0}


def test_single_censored():
    """1 censored patient → no step-down, curve is just the anchor."""
    curve = compute_km_curve([(12.0, 0)])
    assert curve == [{"time": 0.0, "survival": 1.0}]


def test_two_deaths_monotone():
    """Survival is non-increasing across all time points."""
    pairs = [(6.0, 1), (12.0, 1)]
    curve = compute_km_curve(pairs)
    survivals = [p["survival"] for p in curve]
    assert survivals == sorted(survivals, reverse=True)


def test_censored_reduces_risk_pool():
    """Censored at t=3, death at t=6: n_at_risk=2 at t=0, censored at t=3 → n_at_risk=1 at t=6."""
    # 2 patients: S1 censored at 3, S2 dies at 6
    # At t=6: n_at_risk=1, deaths=1 → S(6) = 1.0 * (1-1)/1 = 0.0
    pairs = [(3.0, 0), (6.0, 1)]
    curve = compute_km_curve(pairs)
    death_point = next(p for p in curve if p["time"] == 6.0)
    assert death_point["survival"] == 0.0


def test_tied_deaths():
    """Two deaths at the same time are processed together."""
    # 4 patients, 2 die at t=5
    pairs = [(5.0, 1), (5.0, 1), (10.0, 0), (10.0, 0)]
    curve = compute_km_curve(pairs)
    # After tied deaths at t=5: S = (4-2)/4 = 0.5
    death_point = next(p for p in curve if p["time"] == 5.0)
    assert death_point["survival"] == 0.5


def test_starts_with_anchor():
    """Curve always starts at {time: 0.0, survival: 1.0}."""
    curve = compute_km_curve([(5.0, 1)])
    assert curve[0] == {"time": 0.0, "survival": 1.0}


# ---------------------------------------------------------------------------
# get_km_data() integration tests (with DuckDB)
# ---------------------------------------------------------------------------

def test_km_data_two_deaths(db):
    _add_patient(db, "P1", 6.0, "1:DECEASED")
    _add_patient(db, "P2", 12.0, "1:DECEASED")
    result = get_km_data(db, STUDY)
    assert len(result) == 3  # anchor + 2 death steps
    assert result[0] == {"time": 0.0, "survival": 1.0}
    # After first death at t=6: S = (2-1)/2 = 0.5
    assert result[1]["time"] == 6.0
    assert result[1]["survival"] == pytest.approx(0.5, abs=0.001)


def test_km_data_deceased_label_variants(db):
    """OS_STATUS values containing 'deceased' (case-insensitive) count as events."""
    _add_patient(db, "P1", 10.0, "DECEASED")
    _add_patient(db, "P2", 10.0, "Deceased")
    result = get_km_data(db, STUDY)
    assert len(result) == 2  # anchor + one death step


def test_km_data_living_not_an_event(db):
    _add_patient(db, "P1", 24.0, "LIVING")
    result = get_km_data(db, STUDY)
    # Censored patient: no step-downs, only anchor
    assert result == [{"time": 0.0, "survival": 1.0}]


def test_km_data_missing_os_columns_returns_empty():
    """If OS_MONTHS or OS_STATUS columns are absent, get_km_data returns []."""
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR, AGE DOUBLE)')
    conn.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR,
            patient_attribute BOOLEAN, priority INTEGER,
            PRIMARY KEY (study_id, attr_id)
        )
    """)
    result = get_km_data(conn, STUDY)
    assert result == []
    conn.close()


def test_km_data_empty_cohort(db):
    result = get_km_data(db, STUDY)
    assert result == []
