"""Unit tests for oncoprint_repository using in-memory DuckDB."""
import duckdb
import pytest

from cbioportal.core.oncoprint_repository import (
    get_oncoprint_data,
    get_clinical_track_options,
    get_clinical_track_data,
    _classify_mutation,
    _mut_priority,
)

STUDY = "test_study"


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    conn.execute(f'CREATE TABLE "{STUDY}_sample" (SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR)')
    conn.execute(f"""
        CREATE TABLE "{STUDY}_mutations" (
            study_id VARCHAR,
            SAMPLE_ID VARCHAR,
            Tumor_Sample_Barcode VARCHAR,
            Hugo_Symbol VARCHAR,
            Variant_Classification VARCHAR,
            HGVSp_Short VARCHAR,
            Mutation_Status VARCHAR
        )
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_cna" (
            study_id VARCHAR,
            hugo_symbol VARCHAR,
            sample_id VARCHAR,
            cna_value INTEGER
        )
    """)
    conn.execute(f"""
        CREATE TABLE "{STUDY}_sv" (
            study_id VARCHAR,
            Sample_Id VARCHAR,
            Site1_Hugo_Symbol VARCHAR,
            Site2_Hugo_Symbol VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE studies (study_id VARCHAR, name VARCHAR)
    """)
    conn.execute("INSERT INTO studies VALUES (?, ?)", (STUDY, "Test Study"))
    yield conn
    conn.close()


@pytest.fixture
def db_with_clinical(db):
    db.execute(f"""
        ALTER TABLE "{STUDY}_sample" ADD COLUMN CANCER_TYPE VARCHAR
    """)
    db.execute(f"""
        ALTER TABLE "{STUDY}_sample" ADD COLUMN CLINICAL_GROUP VARCHAR
    """)
    db.execute(f'CREATE TABLE "{STUDY}_patient" (PATIENT_ID VARCHAR, SEX VARCHAR)')
    db.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            datatype VARCHAR, patient_attribute BOOLEAN, priority INTEGER
        )
    """)
    db.executemany("INSERT INTO clinical_attribute_meta VALUES (?, ?, ?, ?, ?, ?)", [
        (STUDY, "CANCER_TYPE",    "Cancer Type",    "STRING", False, 100),
        (STUDY, "CLINICAL_GROUP", "Clinical Group", "STRING", False, 80),
        (STUDY, "SEX",            "Sex",            "STRING", True,  60),
    ])
    yield db


def _add_sample(conn, sid, pid="P1"):
    conn.execute(f'INSERT INTO "{STUDY}_sample" (SAMPLE_ID, PATIENT_ID) VALUES (?, ?)', (sid, pid))


def _add_mut(conn, sid, gene, vc, status="Somatic", hgvsp=""):
    conn.execute(
        f'INSERT INTO "{STUDY}_mutations" VALUES (?, ?, ?, ?, ?, ?, ?)',
        (STUDY, sid, sid, gene, vc, hgvsp, status),
    )


# ── _classify_mutation ───────────────────────────────────────────────────────

def test_classify_missense():
    assert _classify_mutation("Missense_Mutation") == "missense"

def test_classify_nonsense():
    assert _classify_mutation("Nonsense_Mutation") == "trunc"

def test_classify_frame_shift_del():
    assert _classify_mutation("Frame_Shift_Del") == "trunc"

def test_classify_frame_shift_ins():
    assert _classify_mutation("Frame_Shift_Ins") == "trunc"

def test_classify_inframe_del():
    assert _classify_mutation("In_Frame_Del") == "inframe"

def test_classify_inframe_ins():
    assert _classify_mutation("In_Frame_Ins") == "inframe"

def test_classify_splice_site():
    assert _classify_mutation("Splice_Site") == "splice"

def test_classify_splice_region():
    assert _classify_mutation("Splice_Region") == "splice"

def test_classify_flank_promoter():
    assert _classify_mutation("5'Flank") == "promoter"

def test_classify_unknown():
    assert _classify_mutation("RNA") == "other"

def test_classify_none():
    assert _classify_mutation(None) == "other"


# ── Mutation priority ─────────────────────────────────────────────────────────

def test_priority_trunc_beats_missense():
    assert _mut_priority("trunc") > _mut_priority("missense")

def test_priority_inframe_beats_missense():
    """Per cBioPortal render priority: inframe > missense (inframe_rec > missense_rec chain)."""
    assert _mut_priority("inframe") > _mut_priority("missense")

def test_priority_inframe_beats_other():
    assert _mut_priority("inframe") > _mut_priority("other")


# ── get_oncoprint_data ────────────────────────────────────────────────────────

def test_empty_study_returns_empty(db):
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result == []


def test_sample_no_alteration(db):
    _add_sample(db, "S1")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert len(result) == 1
    row = result[0]
    assert row["uid"] == "S1"
    assert row["disp_mut"] is None
    assert row["disp_cna"] is None
    assert row["disp_structuralVariant"] is None
    assert row["disp_germ"] is False


def test_missense_mutation_classified(db):
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "missense"


def test_truncating_mutation_classified(db):
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Nonsense_Mutation")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "trunc"


def test_uncalled_excluded(db):
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation", status="UNCALLED")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] is None


def test_germline_detection(db):
    _add_sample(db, "S1")
    _add_mut(db, "S1", "BRCA1", "Frame_Shift_Del", status="Germline")
    result = get_oncoprint_data(db, STUDY, "BRCA1")
    assert result[0]["disp_germ"] is True
    assert result[0]["disp_mut"] == "trunc"


def test_mutation_priority_trunc_over_missense(db):
    """Sample has both missense and truncating — trunc wins."""
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation")
    _add_mut(db, "S1", "KRAS", "Frame_Shift_Del")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "trunc"


def test_other_gene_not_in_result(db):
    """Alteration in TP53 doesn't affect KRAS slot."""
    _add_sample(db, "S1")
    _add_mut(db, "S1", "TP53", "Missense_Mutation")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] is None


def test_cna_amp(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?, ?)', (STUDY, "KRAS", "S1", 2))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_cna"] == "amp"


def test_cna_homdel(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?, ?)', (STUDY, "KRAS", "S1", -2))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_cna"] == "homdel"


def test_cna_gain(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?, ?)', (STUDY, "KRAS", "S1", 1))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_cna"] == "gain"


def test_cna_hetloss(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?, ?)', (STUDY, "KRAS", "S1", -1))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_cna"] == "hetloss"


def test_cna_wrong_gene_ignored(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?, ?)', (STUDY, "TP53", "S1", 2))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_cna"] is None


def test_sv_detected_site1(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?, ?, ?)', (STUDY, "S1", "KRAS", "OTHER"))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_structuralVariant"] == "sv"


def test_sv_detected_site2(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?, ?, ?)', (STUDY, "S1", "OTHER", "KRAS"))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_structuralVariant"] == "sv"


def test_sv_other_gene_not_detected(db):
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?, ?, ?)', (STUDY, "S1", "TP53", "OTHER"))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_structuralVariant"] is None


def test_multiple_samples_independent(db):
    """Each sample gets its own alteration independently."""
    _add_sample(db, "S1", "P1")
    _add_sample(db, "S2", "P2")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation")
    db.execute(f'INSERT INTO "{STUDY}_cna" VALUES (?, ?, ?, ?)', (STUDY, "KRAS", "S2", -2))
    result = get_oncoprint_data(db, STUDY, "KRAS")
    by_uid = {r["uid"]: r for r in result}
    assert by_uid["S1"]["disp_mut"] == "missense"
    assert by_uid["S1"]["disp_cna"] is None
    assert by_uid["S2"]["disp_mut"] is None
    assert by_uid["S2"]["disp_cna"] == "homdel"


def test_sample_filter(db):
    _add_sample(db, "S1", "P1")
    _add_sample(db, "S2", "P2")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation")
    result = get_oncoprint_data(db, STUDY, "KRAS", sample_ids=["S1"])
    assert len(result) == 1
    assert result[0]["uid"] == "S1"


def test_patient_id_in_result(db):
    _add_sample(db, "S1", "P-001")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["patient"] == "P-001"


# ── get_clinical_track_options ───────────────────────────────────────────────

def test_clinical_options_sorted_by_freq(db_with_clinical):
    db = db_with_clinical
    # Insert 10 samples, fill CANCER_TYPE for 8, CLINICAL_GROUP for 5
    for i in range(10):
        db.execute(
            f'INSERT INTO "{STUDY}_sample" (SAMPLE_ID, PATIENT_ID, CANCER_TYPE, CLINICAL_GROUP)'
            " VALUES (?, ?, ?, ?)",
            (f"S{i}", f"P{i}",
             "Lung" if i < 8 else None,
             "1A" if i < 5 else None),
        )
        db.execute(f'INSERT INTO "{STUDY}_patient" VALUES (?, ?)', (f"P{i}", "M"))

    options = get_clinical_track_options(db, STUDY)
    attr_ids = [o["attr_id"] for o in options]
    # CANCER_TYPE (freq 0.8) should appear before CLINICAL_GROUP (freq 0.5)
    ct_idx = attr_ids.index("CANCER_TYPE")
    cg_idx = attr_ids.index("CLINICAL_GROUP")
    assert ct_idx < cg_idx


def test_clinical_options_empty_without_meta(db):
    """No clinical_attribute_meta → returns empty list."""
    result = get_clinical_track_options(db, STUDY)
    assert result == []


# ── get_clinical_track_data ──────────────────────────────────────────────────

def test_clinical_track_data_sample_attr(db_with_clinical):
    db = db_with_clinical
    db.execute(
        f'INSERT INTO "{STUDY}_sample" (SAMPLE_ID, PATIENT_ID, CANCER_TYPE) VALUES (?, ?, ?)',
        ("S1", "P1", "Lung"),
    )
    result = get_clinical_track_data(db, STUDY, ["CANCER_TYPE"])
    assert result["S1"]["CANCER_TYPE"] == "Lung"


def test_clinical_track_data_patient_attr(db_with_clinical):
    db = db_with_clinical
    db.execute(
        f'INSERT INTO "{STUDY}_sample" (SAMPLE_ID, PATIENT_ID, CANCER_TYPE) VALUES (?, ?, ?)',
        ("S1", "P1", None),
    )
    db.execute(f'INSERT INTO "{STUDY}_patient" VALUES (?, ?)', ("P1", "Female"))
    result = get_clinical_track_data(db, STUDY, ["SEX"])
    assert result["S1"]["SEX"] == "Female"


def test_clinical_track_data_empty_attrs(db_with_clinical):
    result = get_clinical_track_data(db_with_clinical, STUDY, [])
    assert result == {}


def test_clinical_track_data_null_not_included(db_with_clinical):
    db = db_with_clinical
    db.execute(
        f'INSERT INTO "{STUDY}_sample" (SAMPLE_ID, PATIENT_ID, CANCER_TYPE) VALUES (?, ?, ?)',
        ("S1", "P1", None),
    )
    result = get_clinical_track_data(db, STUDY, ["CANCER_TYPE"])
    # NULL values are excluded from the dict
    assert "CANCER_TYPE" not in result.get("S1", {})


# ── Driver annotation (_rec suffix) ─────────────────────────────────────────

@pytest.fixture
def db_with_annotations(db):
    """Add variant_annotations table for driver testing."""
    db.execute(f"""
        CREATE TABLE "{STUDY}_variant_annotations" (
            study_id VARCHAR,
            sample_id VARCHAR,
            hugo_symbol VARCHAR,
            alteration_type VARCHAR,
            variant_classification VARCHAR,
            hgvsp_short VARCHAR,
            moalmanac_clinical_significance VARCHAR,
            hotspot_type VARCHAR
        )
    """)
    return db


def test_missense_rec_with_fda_annotation(db_with_annotations):
    db = db_with_annotations
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation", hgvsp="p.G12D")
    db.execute(
        f'INSERT INTO "{STUDY}_variant_annotations" VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (STUDY, "S1", "KRAS", "MUTATION", "Missense_Mutation", "p.G12D", "FDA-Approved", None),
    )
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "missense_rec"


def test_missense_vus_without_annotation(db_with_annotations):
    db = db_with_annotations
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation", hgvsp="p.A146T")
    # No matching annotation → bare type
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "missense"


def test_clinical_evidence_also_driver(db_with_annotations):
    db = db_with_annotations
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation", hgvsp="p.G12C")
    db.execute(
        f'INSERT INTO "{STUDY}_variant_annotations" VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (STUDY, "S1", "KRAS", "MUTATION", "Missense_Mutation", "p.G12C", "Clinical evidence", None),
    )
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "missense_rec"


def test_trunc_rec_beats_missense_vus(db_with_annotations):
    """Driver truncating should beat VUS missense in priority."""
    db = db_with_annotations
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation", hgvsp="p.A146T")
    _add_mut(db, "S1", "KRAS", "Frame_Shift_Del", hgvsp="p.V14fs")
    db.execute(
        f'INSERT INTO "{STUDY}_variant_annotations" VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (STUDY, "S1", "KRAS", "MUTATION", "Frame_Shift_Del", "p.V14fs", "FDA-Approved", None),
    )
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "trunc_rec"


def test_no_annotations_table_graceful(db):
    """Studies without variant_annotations table should still work (bare types)."""
    _add_sample(db, "S1")
    _add_mut(db, "S1", "KRAS", "Missense_Mutation")
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_mut"] == "missense"


def test_sv_driver_annotation(db_with_annotations):
    db = db_with_annotations
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?, ?, ?)', (STUDY, "S1", "KRAS", "OTHER"))
    db.execute(
        f'INSERT INTO "{STUDY}_variant_annotations" VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (STUDY, "S1", "KRAS", "SV", None, None, "FDA-Approved", None),
    )
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_structuralVariant"] == "sv_rec"


def test_sv_vus_no_annotation(db_with_annotations):
    db = db_with_annotations
    _add_sample(db, "S1")
    db.execute(f'INSERT INTO "{STUDY}_sv" VALUES (?, ?, ?, ?)', (STUDY, "S1", "KRAS", "OTHER"))
    # No annotation for SV → bare "sv"
    result = get_oncoprint_data(db, STUDY, "KRAS")
    assert result[0]["disp_structuralVariant"] == "sv"


def test_priority_rec_beats_bare(db_with_annotations):
    """_rec suffix variants should have higher priority than bare variants."""
    from cbioportal.core.oncoprint_repository import _mut_priority
    assert _mut_priority("missense_rec") > _mut_priority("missense")
    assert _mut_priority("trunc_rec") > _mut_priority("trunc")
    assert _mut_priority("missense_rec") > _mut_priority("trunc")  # rec beats any bare
