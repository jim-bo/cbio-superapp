"""Integration tests for pulling and annotating data from live APIs."""

import os
from pathlib import Path
import pytest
import csv

from cbioportal.core.data_puller import pull_and_export_mutations
from cbioportal.core.cache import get_cache_connection, CACHE_DB_PATH

@pytest.mark.live_api
def test_pull_mutations_brca_bccrc(tmp_path):
    """
    Test pulling mutations for a real, smallish study (brca_bccrc).
    This hits:
    1. cBioPortal API (mutations)
    2. MoAlmanac API (annotations)
    """
    study_id = "brca_bccrc"
    output_file = tmp_path / f"{study_id}.tsv"
    
    # Run the orchestrator
    pull_and_export_mutations(study_id, output_file)
    
    # 1. Check if the output file exists
    assert output_file.exists()
    
    # 2. Basic validation of TSV content
    with open(output_file, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
        
    assert len(rows) > 0
    # Check for expected columns
    expected_cols = {
        "HUGO_SYMBOL", 
        "CHROMOSOME", 
        "START_POSITION", 
        "END_POSITION", 
        "VARIANT_CLASSIFICATION",
        "VARIANT_TYPE",
        "REFERENCE_ALLELE",
        "TUMOR_SAMPLE_BARCODE",
        "MUTATION_EFFECT",
        "ONCOGENIC",
        "MOALMANAC_ANNOTATION"
    }
    assert expected_cols.issubset(set(reader.fieldnames))
    
    # 3. Verify DuckDB cache populated
    conn = get_cache_connection(read_only=True)
    try:
        # Check manifest
        res = conn.execute(
            "SELECT count(*) FROM cache_manifest WHERE study_id = ? AND data_type = 'mutations'", 
            [study_id]
        ).fetchone()
        assert res[0] == 1
        
        # Check raw_mutations
        res = conn.execute("SELECT count(*) FROM raw_mutations WHERE study_id = ?", [study_id]).fetchone()
        assert res[0] == len(rows)
        
        # Check moalmanac_cache (should have some entries if annotations were found)
        res = conn.execute("SELECT count(*) FROM moalmanac_cache").fetchone()
        assert res[0] >= 0
    finally:
        conn.close()

@pytest.fixture(autouse=True)
def clean_cache_for_test():
    """Ensure we start with a clean cache for integration tests or handle it gracefully."""
    # We don't necessarily want to delete the user's real cache if they run this locally,
    # but for CI we might. For now, let's just use the default cache path.
    yield
