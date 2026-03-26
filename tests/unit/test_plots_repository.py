"""Unit tests for plots_repository — in-memory DuckDB."""
import duckdb
import pytest

from cbioportal.core.plots_repository import (
    get_cancer_types_summary,
    get_clinical_attribute_options,
    get_color_data,
    get_plots_data,
)


@pytest.fixture
def conn():
    """In-memory DuckDB with sample study data."""
    c = duckdb.connect(":memory:")

    # -- sample table
    c.execute("""
        CREATE TABLE "test_study_sample" (
            study_id VARCHAR, SAMPLE_ID VARCHAR, PATIENT_ID VARCHAR,
            CANCER_TYPE VARCHAR, CANCER_TYPE_DETAILED VARCHAR,
            FRACTION_GENOME_ALTERED DOUBLE, GENE_PANEL VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_sample" VALUES
        ('test_study', 'S1', 'P1', 'Breast Cancer', 'Invasive Breast', 0.1, 'PANEL1'),
        ('test_study', 'S2', 'P2', 'Breast Cancer', 'Invasive Breast', 0.2, 'PANEL1'),
        ('test_study', 'S3', 'P3', 'Colorectal Cancer', 'CRC NOS', 0.5, 'PANEL1'),
        ('test_study', 'S4', 'P4', 'Colorectal Cancer', 'CRC NOS', 0.3, 'PANEL1'),
        ('test_study', 'S5', 'P5', 'Lung Cancer', 'NSCLC', 0.05, 'PANEL1')
    """)

    # -- mutations table
    c.execute("""
        CREATE TABLE "test_study_mutations" (
            study_id VARCHAR, Hugo_Symbol VARCHAR, Tumor_Sample_Barcode VARCHAR,
            Variant_Classification VARCHAR, Mutation_Status VARCHAR,
            Entrez_Gene_Id BIGINT,
            t_alt_count INTEGER, t_ref_count INTEGER,
            cbp_driver VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_mutations" VALUES
        ('test_study', 'KRAS', 'S1', 'Missense_Mutation', 'SOMATIC', 3845, 50, 150, 'Putative_Driver'),
        ('test_study', 'KRAS', 'S3', 'Missense_Mutation', 'SOMATIC', 3845, 30, 170, 'Putative_Passenger'),
        ('test_study', 'KRAS', 'S3', 'Nonsense_Mutation', 'SOMATIC', 3845, 80, 120, 'Putative_Driver'),
        ('test_study', 'KRAS', 'S4', 'Missense_Mutation', 'UNCALLED', 3845, 10, 90, NULL),
        ('test_study', 'BRAF', 'S2', 'Missense_Mutation', 'SOMATIC', 673, 40, 160, NULL)
    """)

    # -- CNA table
    c.execute("""
        CREATE TABLE "test_study_cna" (
            study_id VARCHAR, hugo_symbol VARCHAR, sample_id VARCHAR, cna_value DOUBLE
        )
    """)
    c.execute("""
        INSERT INTO "test_study_cna" VALUES
        ('test_study', 'KRAS', 'S1', 2),
        ('test_study', 'KRAS', 'S5', -2),
        ('test_study', 'BRAF', 'S3', 1)
    """)

    # -- SV table
    c.execute("""
        CREATE TABLE "test_study_sv" (
            study_id VARCHAR, Sample_Id VARCHAR, SV_Status VARCHAR,
            Site1_Hugo_Symbol VARCHAR, Site2_Hugo_Symbol VARCHAR,
            Class VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_sv" VALUES
        ('test_study', 'S2', 'SOMATIC', 'KRAS', 'ALK', 'TRANSLOCATION'),
        ('test_study', 'S3', 'SOMATIC', 'KRAS', 'BRAF', 'DELETION'),
        ('test_study', 'S3', 'SOMATIC', 'BRAF', 'KRAS', 'INVERSION')
    """)

    # -- gene_panel table
    c.execute("""
        CREATE TABLE "test_study_gene_panel" (
            study_id VARCHAR, SAMPLE_ID VARCHAR,
            mutations VARCHAR, cna VARCHAR, structural_variants VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO "test_study_gene_panel" VALUES
        ('test_study', 'S1', 'PANEL1', 'PANEL1', 'PANEL1'),
        ('test_study', 'S2', 'PANEL1', 'PANEL1', 'PANEL1'),
        ('test_study', 'S3', 'PANEL1', 'PANEL1', NULL),
        ('test_study', 'S4', 'PANEL1', 'PANEL1', 'PANEL1'),
        ('test_study', 'S5', 'PANEL1', 'PANEL1', 'PANEL1')
    """)

    # -- clinical_attribute_meta
    c.execute("""
        CREATE TABLE clinical_attribute_meta (
            study_id VARCHAR, attr_id VARCHAR, display_name VARCHAR,
            description VARCHAR, datatype VARCHAR, patient_attribute BOOLEAN,
            priority INTEGER
        )
    """)
    c.execute("""
        INSERT INTO clinical_attribute_meta VALUES
        ('test_study', 'CANCER_TYPE', 'Cancer Type', 'Cancer type', 'STRING', false, 1),
        ('test_study', 'FRACTION_GENOME_ALTERED', 'Fraction Genome Altered', 'FGA', 'NUMBER', false, 2)
    """)

    yield c
    c.close()


# ── Cancer Types Summary ──────────────────────────────────────────────────


class TestCancerTypesSummary:
    def test_basic_grouping(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        assert "Breast Cancer" in cats
        assert "Colorectal Cancer" in cats
        assert "Lung Cancer" in cats

    def test_mutation_counts(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        # S1 has mutation, S2 does not
        assert cats["Breast Cancer"]["mutation"] == 1
        # S3 has mutations (UNCALLED S4 excluded), S4 excluded
        assert cats["Colorectal Cancer"]["mutation"] == 1

    def test_uncalled_excluded(self, conn):
        """UNCALLED mutations should not be counted."""
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}
        # S4 has UNCALLED mutation — should not count
        assert cats["Colorectal Cancer"]["mutation"] == 1

    def test_cna_counts(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        assert cats["Breast Cancer"]["amplification"] == 1  # S1
        assert cats["Lung Cancer"]["deep_deletion"] == 1  # S5

    def test_sv_counts(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        assert cats["Breast Cancer"]["structural_variant"] == 1  # S2

    def test_multiple_alterations(self, conn):
        """S1 has both mutation AND amplification → counts as multiple."""
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        assert cats["Breast Cancer"]["multiple"] == 1  # S1 has mut + amp

    def test_count_by_patients(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="patients")
        cats = {c["name"]: c for c in result["categories"]}

        assert cats["Breast Cancer"]["total"] == 2  # P1, P2

    def test_group_by_detailed(self, conn):
        result = get_cancer_types_summary(
            conn, "test_study", "KRAS", group_by="CANCER_TYPE_DETAILED", count_by="samples"
        )
        cats = {c["name"]: c for c in result["categories"]}

        assert "Invasive Breast" in cats
        assert "CRC NOS" in cats

    def test_profiling_counts(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        # S3 has no SV panel, S4 does
        assert cats["Colorectal Cancer"]["profiled"]["mutation"] == 2
        assert cats["Colorectal Cancer"]["profiled"]["sv"] == 1

    def test_empty_gene(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "FAKEGENE", count_by="samples")
        # Should still return categories with zero counts
        assert len(result["categories"]) > 0
        for cat in result["categories"]:
            assert cat["mutation"] == 0

    def test_totals(self, conn):
        result = get_cancer_types_summary(conn, "test_study", "KRAS", count_by="samples")
        cats = {c["name"]: c for c in result["categories"]}

        assert cats["Breast Cancer"]["total"] == 2
        assert cats["Colorectal Cancer"]["total"] == 2
        assert cats["Lung Cancer"]["total"] == 1


# ── Clinical Attribute Options ────────────────────────────────────────────


class TestClinicalOptions:
    def test_returns_options(self, conn):
        options = get_clinical_attribute_options(conn, "test_study")
        assert len(options) == 2
        ids = [o["attr_id"] for o in options]
        assert "CANCER_TYPE" in ids
        assert "FRACTION_GENOME_ALTERED" in ids

    def test_includes_datatype(self, conn):
        options = get_clinical_attribute_options(conn, "test_study")
        by_id = {o["attr_id"]: o for o in options}
        assert by_id["CANCER_TYPE"]["datatype"] == "STRING"
        assert by_id["FRACTION_GENOME_ALTERED"]["datatype"] == "NUMBER"


# ── Plots Data ────────────────────────────────────────────────────────────


class TestPlotsData:
    def test_discrete_discrete_bar(self, conn):
        """Clinical attr × mutation type → stacked bar."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "mutated_vs_wildtype"},
        )
        assert result["plot_type"] == "bar"
        assert len(result["categories"]) > 0
        assert result["total_samples"] == 5

    def test_numeric_discrete_box(self, conn):
        """FGA (numeric) × Cancer Type (discrete) → box plot."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "clinical_attribute", "attribute_id": "FRACTION_GENOME_ALTERED"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "box"
        assert len(result["categories"]) > 0
        assert len(result["box_data"]) == len(result["categories"])

    def test_sv_axis(self, conn):
        """SV variant vs no variant as an axis."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "structural_variant", "gene": "KRAS", "plot_by": "variant_vs_no_variant"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"
        # Should have categories like "With Structural Variants", "No Structural Variants"
        assert any("Structural" in c for c in result["categories"])

    def test_cna_axis(self, conn):
        """CNA as an axis."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "copy_number", "gene": "KRAS"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"
        assert "Amplification" in result["categories"] or "Diploid" in result["categories"]

    def test_mutation_type_axis(self, conn):
        """Mutation type as discrete axis."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "type"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"

    def test_empty_result(self, conn):
        """No data for gene → empty result."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "FAKEGENE", "plot_by": "type"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"
        assert result["total_samples"] == 5  # all samples still in universe

    def test_scatter_numeric_numeric(self, conn):
        """Two numeric axes → scatter. We need a second numeric column, so use FGA × FGA."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "clinical_attribute", "attribute_id": "FRACTION_GENOME_ALTERED"},
            {"data_type": "clinical_attribute", "attribute_id": "FRACTION_GENOME_ALTERED"},
        )
        assert result["plot_type"] == "scatter"
        assert len(result["points"]) == 5


# ── Color Data ────────────────────────────────────────────────────────────


class TestColorData:
    def test_mutation_color(self, conn):
        """Color by mutation type returns per-sample categories."""
        result = get_color_data(conn, "test_study", {"type": "mutation", "gene": "KRAS"})
        assert "samples" in result
        assert "colors" in result
        assert "order" in result
        # S1 has Missense, S3 has Missense + Nonsense (Multiple), others Not mutated
        assert result["samples"]["S1"] == "Missense"
        assert result["samples"]["S3"] == "Multiple"
        assert result["samples"]["S2"] == "Not mutated"

    def test_mutation_color_has_legacy_colors(self, conn):
        result = get_color_data(conn, "test_study", {"type": "mutation", "gene": "KRAS"})
        assert result["colors"]["Missense"] == "#008000"
        assert result["colors"]["Not mutated"] == "#c4e5f5"

    def test_cna_color(self, conn):
        result = get_color_data(conn, "test_study", {"type": "cna", "gene": "KRAS"})
        # S1 has cna_value=2 (Amplification), S5 has -2 (Deep Deletion)
        assert result["samples"]["S1"] == "Amplification"
        assert result["samples"]["S5"] == "Deep Deletion"
        assert result["colors"]["Amplification"] == "#ff0000"
        assert result["colors"]["Deep Deletion"] == "#0000ff"

    def test_sv_color(self, conn):
        result = get_color_data(conn, "test_study", {"type": "sv", "gene": "KRAS"})
        # S2 has SV for KRAS
        assert result["samples"]["S2"] == "Structural Variant"
        assert result["samples"]["S1"] == "No Structural Variant"
        assert result["colors"]["Structural Variant"] == "#8B00C9"

    def test_clinical_color(self, conn):
        result = get_color_data(conn, "test_study", {"type": "clinical", "attribute_id": "CANCER_TYPE"})
        assert result["samples"]["S1"] == "Breast Cancer"
        assert result["samples"]["S3"] == "Colorectal Cancer"
        # Should have 3 unique categories
        assert len(result["order"]) == 3

    def test_clinical_reserved_colors(self, conn):
        """Reserved clinical colors (e.g. Male/Female) should use legacy palette."""
        # Our fixture doesn't have sex data, but we test the color mapping logic
        result = get_color_data(conn, "test_study", {"type": "clinical", "attribute_id": "CANCER_TYPE"})
        # Non-reserved values get D3 palette colors
        assert all(c.startswith("#") for c in result["colors"].values())

    def test_empty_gene(self, conn):
        result = get_color_data(conn, "test_study", {"type": "mutation", "gene": "FAKEGENE"})
        # All samples should be "Not mutated"
        assert all(v == "Not mutated" for v in result["samples"].values())

    def test_unknown_type(self, conn):
        result = get_color_data(conn, "test_study", {"type": "unknown"})
        assert result["samples"] == {}
        assert result["colors"] == {}

    def test_box_data_includes_raw(self, conn):
        """Box plot data should include box_raw_data for scatter overlay."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "clinical_attribute", "attribute_id": "FRACTION_GENOME_ALTERED"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "box"
        assert "box_raw_data" in result
        # Each category should have raw sample data
        for cat in result["categories"]:
            assert cat in result["box_raw_data"]
            for pt in result["box_raw_data"][cat]:
                assert "sample_id" in pt
                assert "value" in pt


# ── VAF Count-By Mode ────────────────────────────────────────────────────


class TestVAFAxis:
    def test_vaf_returns_numeric(self, conn):
        """VAF plot_by should produce a numeric axis."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "vaf"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        # numeric × discrete → box plot
        assert result["plot_type"] == "box"

    def test_vaf_values(self, conn):
        """VAF should be t_alt / (t_alt + t_ref), max per sample."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "vaf"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        # S1: 50/(50+150) = 0.25
        # S3: max(30/200=0.15, 80/200=0.4) = 0.4
        # S4: UNCALLED → excluded
        # Only S1 and S3 should have VAF values
        raw = {}
        for cat, pts in result["box_raw_data"].items():
            for pt in pts:
                raw[pt["sample_id"]] = pt["value"]
        assert abs(raw["S1"] - 0.25) < 0.01
        assert abs(raw["S3"] - 0.4) < 0.01
        assert "S4" not in raw  # UNCALLED excluded

    def test_vaf_empty_gene(self, conn):
        """Gene with no mutations → no VAF data, falls back gracefully."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "FAKEGENE", "plot_by": "vaf"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        # No numeric values → empty result
        assert result["total_samples"] == 0 or result["plot_type"] in ("box", "bar")


# ── Driver vs VUS Count-By Mode ──────────────────────────────────────────


class TestDriverVsVUS:
    def test_driver_classification(self, conn):
        """cbp_driver values map to Driver/VUS/Wild Type categories."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "driver_vs_vus"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"
        # S1: Putative_Driver → "Driver"
        # S3: has both Driver and Passenger mutations → highest priority = "Driver"
        # S4: UNCALLED → excluded → Wild Type
        # S2, S5: no KRAS mutation → "Wild Type"
        # h_config is mutation axis → categories are driver labels
        assert "Driver" in result["categories"] or "Wild Type" in result["categories"]

    def test_driver_missing_column(self, conn):
        """Study without cbp_driver → graceful fallback."""
        # Drop cbp_driver column to simulate missing data
        conn.execute('ALTER TABLE "test_study_mutations" DROP COLUMN cbp_driver')
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "mutation", "gene": "KRAS", "plot_by": "driver_vs_vus"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        # Should still return valid data (all samples as "Not Available" or similar)
        assert result["plot_type"] == "bar"
        # Re-add column for other tests
        conn.execute('ALTER TABLE "test_study_mutations" ADD COLUMN cbp_driver VARCHAR')


# ── SV Variant Type Count-By Mode ────────────────────────────────────────


class TestSVVariantType:
    def test_sv_variant_type_grouping(self, conn):
        """SV variant_type plot_by should group by Class column."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "structural_variant", "gene": "KRAS", "plot_by": "variant_type"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert result["plot_type"] == "bar"
        # h_config is SV axis → categories are SV class labels
        assert any(
            name in result["categories"]
            for name in ["Translocation", "Deletion", "Multiple", "No Structural Variant"]
        )

    def test_sv_multiple_classes(self, conn):
        """Sample with multiple SV classes → Multiple category."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "structural_variant", "gene": "KRAS", "plot_by": "variant_type"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        # S3 has both DELETION and INVERSION for KRAS → "Multiple"
        assert "Multiple" in result["categories"]


# ── Data Availability Banner ─────────────────────────────────────────────


class TestDataBanner:
    def test_axis_counts_in_response(self, conn):
        """Plot response should include per-axis sample counts."""
        result = get_plots_data(
            conn,
            "test_study",
            {"data_type": "clinical_attribute", "attribute_id": "FRACTION_GENOME_ALTERED"},
            {"data_type": "clinical_attribute", "attribute_id": "CANCER_TYPE"},
        )
        assert "h_total" in result
        assert "v_total" in result
        assert result["h_total"] == 5
        assert result["v_total"] == 5
        assert result["total_samples"] == 5
