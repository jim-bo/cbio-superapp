"""Unit tests for molecular profile loading and repository functions."""
import duckdb
import pytest
from pathlib import Path


@pytest.fixture
def conn():
    return duckdb.connect(":memory:")


@pytest.fixture
def tmp_study(tmp_path):
    """Create a minimal study directory with meta files."""
    (tmp_path / "meta_study.txt").write_text(
        "cancer_study_identifier: test_study\n"
        "type_of_cancer: mixed\n"
        "name: Test Study\n"
    )
    (tmp_path / "meta_mutations.txt").write_text(
        "cancer_study_identifier: test_study\n"
        "genetic_alteration_type: MUTATION_EXTENDED\n"
        "datatype: MAF\n"
        "stable_id: mutations\n"
        "show_profile_in_analysis_tab: true\n"
        "profile_description: Mutation data.\n"
        "profile_name: Mutations\n"
        "data_filename: data_mutations.txt\n"
    )
    (tmp_path / "meta_cna.txt").write_text(
        "cancer_study_identifier: test_study\n"
        "genetic_alteration_type: COPY_NUMBER_ALTERATION\n"
        "datatype: DISCRETE\n"
        "stable_id: cna\n"
        "show_profile_in_analysis_tab: true\n"
        "profile_description: Putative copy-number from GISTIC 2.0.\n"
        "profile_name: Putative copy-number alterations from GISTIC\n"
        "data_filename: data_cna.txt\n"
    )
    (tmp_path / "meta_sv.txt").write_text(
        "cancer_study_identifier: test_study\n"
        "genetic_alteration_type: STRUCTURAL_VARIANT\n"
        "datatype: SV\n"
        "stable_id: structural_variants\n"
        "show_profile_in_analysis_tab: true\n"
        "profile_description: Structural variant data.\n"
        "profile_name: Structural variants\n"
        "data_filename: data_sv.txt\n"
    )
    # Non-molecular meta file that should be skipped
    (tmp_path / "meta_clinical_sample.txt").write_text(
        "cancer_study_identifier: test_study\n"
        "datatype: SAMPLE_ATTRIBUTES\n"
        "data_filename: data_clinical_sample.txt\n"
    )
    return tmp_path


class TestLoadMolecularProfiles:
    def test_parses_meta_files(self, conn, tmp_study):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles

        load_molecular_profiles(conn, "test_study", tmp_study)

        rows = conn.execute(
            "SELECT stable_id, profile_name, genetic_alteration_type "
            "FROM molecular_profiles ORDER BY stable_id"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0] == ("cna", "Putative copy-number alterations from GISTIC", "COPY_NUMBER_ALTERATION")
        assert rows[1] == ("mutations", "Mutations", "MUTATION_EXTENDED")
        assert rows[2] == ("structural_variants", "Structural variants", "STRUCTURAL_VARIANT")

    def test_skips_non_molecular_metas(self, conn, tmp_path):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles

        # Only meta_study.txt and meta_clinical_sample.txt — no molecular profiles
        (tmp_path / "meta_study.txt").write_text(
            "cancer_study_identifier: test_study\nname: Test\n"
        )
        (tmp_path / "meta_clinical_sample.txt").write_text(
            "cancer_study_identifier: test_study\ndatatype: SAMPLE_ATTRIBUTES\n"
        )
        load_molecular_profiles(conn, "test_study", tmp_path)

        count = conn.execute("SELECT COUNT(*) FROM molecular_profiles").fetchone()[0]
        assert count == 0

    def test_empty_directory(self, conn, tmp_path):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles

        load_molecular_profiles(conn, "test_study", tmp_path)

        count = conn.execute("SELECT COUNT(*) FROM molecular_profiles").fetchone()[0]
        assert count == 0

    def test_idempotent_reload(self, conn, tmp_study):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles

        load_molecular_profiles(conn, "test_study", tmp_study)
        load_molecular_profiles(conn, "test_study", tmp_study)

        count = conn.execute("SELECT COUNT(*) FROM molecular_profiles").fetchone()[0]
        assert count == 3  # no duplicates

    def test_show_profile_flag(self, conn, tmp_path):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles

        (tmp_path / "meta_mrna.txt").write_text(
            "cancer_study_identifier: test_study\n"
            "genetic_alteration_type: MRNA_EXPRESSION\n"
            "stable_id: mrna\n"
            "profile_name: mRNA Expression\n"
            "show_profile_in_analysis_tab: false\n"
        )
        load_molecular_profiles(conn, "test_study", tmp_path)

        row = conn.execute(
            "SELECT show_profile_in_analysis_tab FROM molecular_profiles WHERE stable_id = 'mrna'"
        ).fetchone()
        assert row[0] is False


class TestGetMolecularProfileName:
    def test_returns_name_from_db(self, conn):
        from cbioportal.core.plots_repository import get_molecular_profile_name

        conn.execute("""
            CREATE TABLE molecular_profiles (
                study_id VARCHAR, stable_id VARCHAR, genetic_alteration_type VARCHAR,
                profile_name VARCHAR, PRIMARY KEY (study_id, stable_id)
            )
        """)
        conn.execute(
            "INSERT INTO molecular_profiles VALUES (?, ?, ?, ?)",
            ["my_study", "cna", "COPY_NUMBER_ALTERATION", "Custom CNA Profile"],
        )

        name = get_molecular_profile_name(conn, "my_study", "cna")
        assert name == "Custom CNA Profile"

    def test_fallback_when_no_table(self, conn):
        from cbioportal.core.plots_repository import get_molecular_profile_name

        name = get_molecular_profile_name(conn, "missing_study", "cna")
        assert name == "Putative copy-number alterations from GISTIC"

    def test_fallback_when_no_row(self, conn):
        from cbioportal.core.plots_repository import get_molecular_profile_name

        conn.execute("""
            CREATE TABLE molecular_profiles (
                study_id VARCHAR, stable_id VARCHAR, genetic_alteration_type VARCHAR,
                profile_name VARCHAR, PRIMARY KEY (study_id, stable_id)
            )
        """)

        name = get_molecular_profile_name(conn, "my_study", "mutation")
        assert name == "Mutations"


class TestGetMolecularProfiles:
    def test_returns_all_profiles(self, conn, tmp_study):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles
        from cbioportal.core.plots_repository import get_molecular_profiles

        load_molecular_profiles(conn, "test_study", tmp_study)
        profiles = get_molecular_profiles(conn, "test_study")
        assert len(profiles) == 3
        assert all(p["study_id"] == "test_study" for p in profiles)

    def test_filter_by_alteration_type(self, conn, tmp_study):
        from cbioportal.core.loader.molecular_profiles import load_molecular_profiles
        from cbioportal.core.plots_repository import get_molecular_profiles

        load_molecular_profiles(conn, "test_study", tmp_study)
        profiles = get_molecular_profiles(conn, "test_study", "COPY_NUMBER_ALTERATION")
        assert len(profiles) == 1
        assert profiles[0]["stable_id"] == "cna"

    def test_empty_when_no_table(self, conn):
        from cbioportal.core.plots_repository import get_molecular_profiles

        profiles = get_molecular_profiles(conn, "missing_study")
        assert profiles == []
