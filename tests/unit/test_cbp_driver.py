"""Unit tests for cbp_driver heuristic computation."""
import duckdb
import pytest

from cbioportal.core.annotation import _compute_cbp_driver
from cbioportal.core.plots_repository import get_plots_data


@pytest.fixture
def conn():
    """In-memory DuckDB with mutations + variant_annotations tables."""
    c = duckdb.connect(":memory:")

    # -- sample table
    c.execute("""
        CREATE TABLE "test_study_sample" (
            study_id VARCHAR, SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR,
            CANCER_TYPE VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_sample" VALUES
        ('test_study', 'S1', 'P1', 'Breast Cancer'),
        ('test_study', 'S2', 'P2', 'Breast Cancer'),
        ('test_study', 'S3', 'P3', 'Lung Cancer'),
        ('test_study', 'S4', 'P4', 'Lung Cancer'),
        ('test_study', 'S5', 'P5', 'Colorectal Cancer')
    """)

    # -- mutations table (no cbp_driver column yet — _compute_cbp_driver adds it)
    c.execute("""
        CREATE TABLE "test_study_mutations" (
            study_id VARCHAR, Hugo_Symbol VARCHAR, Tumor_Sample_Barcode VARCHAR,
            Variant_Classification VARCHAR, Mutation_Status VARCHAR,
            HGVSp_Short VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_mutations" VALUES
        ('test_study', 'KRAS', 'S1', 'Missense_Mutation', 'SOMATIC', 'p.G12D'),
        ('test_study', 'BRAF', 'S2', 'Missense_Mutation', 'SOMATIC', 'p.V600E'),
        ('test_study', 'TP53', 'S3', 'Nonsense_Mutation', 'SOMATIC', 'p.R175H'),
        ('test_study', 'KRAS', 'S4', 'Missense_Mutation', 'SOMATIC', 'p.G13D'),
        ('test_study', 'APC',  'S5', 'Missense_Mutation', 'SOMATIC', 'p.R1450Q')
    """)

    # -- variant_annotations table
    c.execute("""
        CREATE TABLE "test_study_variant_annotations" (
            study_id VARCHAR, alteration_type VARCHAR, sample_id VARCHAR,
            hugo_symbol VARCHAR, hgvsp_short VARCHAR, variant_classification VARCHAR,
            hotspot_type VARCHAR, intogen_role VARCHAR, moalmanac_oncogenic VARCHAR,
            vep_impact VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_variant_annotations" VALUES
        -- S1/KRAS: hotspot → Driver
        ('test_study', 'MUTATION', 'S1', 'KRAS', 'p.G12D', 'Missense_Mutation', 'single_residue', 'Act', NULL, 'HIGH'),
        -- S2/BRAF: hotspot + moalmanac → Driver
        ('test_study', 'MUTATION', 'S2', 'BRAF', 'p.V600E', 'Missense_Mutation', 'single_residue', NULL, 'Oncogenic', 'HIGH'),
        -- S3/TP53: intogen LoF + functional VC → Driver
        ('test_study', 'MUTATION', 'S3', 'TP53', 'p.R175H', 'Nonsense_Mutation', NULL, 'LoF', NULL, 'HIGH'),
        -- S4/KRAS: intogen Act but will test
        ('test_study', 'MUTATION', 'S4', 'KRAS', 'p.G13D', 'Missense_Mutation', NULL, 'Act', NULL, 'MODERATE'),
        -- S5/APC: no signals → Passenger
        ('test_study', 'MUTATION', 'S5', 'APC', 'p.R1450Q', 'Missense_Mutation', NULL, NULL, NULL, 'MODERATE')
    """)

    # -- clinical_attribute_meta (needed by get_plots_data)
    c.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER
        )
    """)
    c.execute("""
        INSERT INTO clinical_attribute_meta VALUES
        ('test_study', 'CANCER_TYPE', 'Cancer Type', 'Cancer type', 'STRING', false, 1)
    """)

    yield c
    c.close()


class TestComputeCbpDriver:
    def test_adds_column(self, conn):
        """_compute_cbp_driver should add cbp_driver column to mutations table."""
        _compute_cbp_driver(conn, "test_study")
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'test_study_mutations' AND column_name = 'cbp_driver'"
        ).fetchall()
        assert len(cols) == 1

    def test_hotspot_is_driver(self, conn):
        """Mutations at hotspot positions should be classified as Driver."""
        _compute_cbp_driver(conn, "test_study")
        row = conn.execute(
            'SELECT cbp_driver FROM "test_study_mutations" '
            "WHERE Tumor_Sample_Barcode = 'S1' AND Hugo_Symbol = 'KRAS'"
        ).fetchone()
        assert row[0] == "Putative_Driver"

    def test_intogen_functional_is_driver(self, conn):
        """IntOGen LoF/Act gene + functional VC → Driver."""
        _compute_cbp_driver(conn, "test_study")
        # S3: TP53 LoF + Nonsense_Mutation → Driver
        row = conn.execute(
            'SELECT cbp_driver FROM "test_study_mutations" '
            "WHERE Tumor_Sample_Barcode = 'S3'"
        ).fetchone()
        assert row[0] == "Putative_Driver"

        # S4: KRAS Act + Missense_Mutation → Driver
        row = conn.execute(
            'SELECT cbp_driver FROM "test_study_mutations" '
            "WHERE Tumor_Sample_Barcode = 'S4'"
        ).fetchone()
        assert row[0] == "Putative_Driver"

    def test_moalmanac_oncogenic_is_driver(self, conn):
        """MOAlmanac oncogenic annotation → Driver."""
        _compute_cbp_driver(conn, "test_study")
        row = conn.execute(
            'SELECT cbp_driver FROM "test_study_mutations" '
            "WHERE Tumor_Sample_Barcode = 'S2'"
        ).fetchone()
        assert row[0] == "Putative_Driver"

    def test_no_signals_is_passenger(self, conn):
        """Mutations with no driver signals → Passenger."""
        _compute_cbp_driver(conn, "test_study")
        row = conn.execute(
            'SELECT cbp_driver FROM "test_study_mutations" '
            "WHERE Tumor_Sample_Barcode = 'S5'"
        ).fetchone()
        assert row[0] == "Putative_Passenger"

    def test_idempotent(self, conn):
        """Running twice should not create duplicate columns or change values."""
        _compute_cbp_driver(conn, "test_study")
        _compute_cbp_driver(conn, "test_study")
        row = conn.execute(
            'SELECT cbp_driver FROM "test_study_mutations" '
            "WHERE Tumor_Sample_Barcode = 'S1' AND Hugo_Symbol = 'KRAS'"
        ).fetchone()
        assert row[0] == "Putative_Driver"

    def test_missing_annotations_table(self, conn):
        """Should not crash if variant_annotations table doesn't exist."""
        conn.execute('DROP TABLE "test_study_variant_annotations"')
        _compute_cbp_driver(conn, "test_study")
        # Should not have added cbp_driver column
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'test_study_mutations' AND column_name = 'cbp_driver'"
        ).fetchall()
        assert len(cols) == 0


class TestDriverPropagationToPlots:
    """Verify cbp_driver flows through to plots_repository.get_plots_data."""

    def test_driver_vs_vus_with_computed_driver(self, conn):
        """After _compute_cbp_driver, Driver vs VUS plot should show categories."""
        _compute_cbp_driver(conn, "test_study")
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "driver_vs_vus"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"
        # Should have Driver and Wild Type categories (S1 and S4 are Driver, S2/S3/S5 are Wild Type for KRAS)
        assert "Driver" in result["categories"]
        assert "Wild Type" in result["categories"]

    def test_driver_counts_correct(self, conn):
        """Driver vs VUS should count correctly per cancer type."""
        _compute_cbp_driver(conn, "test_study")
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "driver_vs_vus"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        # Build a cross-tabulation to check
        # KRAS mutations: S1 (Breast, Driver), S4 (Lung, Driver)
        # No KRAS mutation: S2 (Breast), S3 (Lung), S5 (Colorectal) → Wild Type
        cats = result["categories"]
        series = {s["name"]: s["data"] for s in result["series"]}

        # Driver category should exist
        assert "Driver" in cats
        driver_idx = cats.index("Driver")
        wild_idx = cats.index("Wild Type")

        # Breast Cancer series: 1 Driver (S1), 1 Wild Type (S2)
        if "Breast Cancer" in series:
            assert series["Breast Cancer"][driver_idx] == 1
            assert series["Breast Cancer"][wild_idx] == 1
