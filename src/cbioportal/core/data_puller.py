"""Orchestrator for pulling, caching, and exporting annotated cBioPortal data."""
import os
from pathlib import Path
import json

import duckdb

from cbioportal.core.api.client import CbioPortalClient
from cbioportal.core.cache import get_cache_connection, get_study_cache_status, update_study_cache_manifest
from cbioportal.core.annotators.moalmanac import annotate_variants


def pull_and_export_mutations(study_id: str, output_path: str | Path) -> None:
    """Pull mutations for a study, cache them, annotate, and export to TSV."""
    output_path = Path(output_path)
    status = get_study_cache_status(study_id, "mutations")
    
    conn = get_cache_connection()
    
    try:
        if not status:
            print(f"Cache miss or expired for {study_id} mutations. Fetching from API...")
            with CbioPortalClient() as client:
                profile_id = client.get_mutation_profile_id(study_id)
                if not profile_id:
                    raise ValueError(f"No MUTATION_EXTENDED profile found for study {study_id}")
                
                sample_list_id = client.get_default_sample_list_id(study_id)
                if not sample_list_id:
                    raise ValueError(f"No sample lists found for study {study_id}")
                    
                raw_mutations = client.get_mutations_raw(profile_id, sample_list_id)
                raw_clinical = client.get_clinical_data_raw(study_id, "ONCOTREE_CODE")
                
            # Create a table for these raw mutations to leverage DuckDB's querying power
            conn.execute("CREATE TABLE IF NOT EXISTS raw_mutations (study_id VARCHAR, data VARCHAR)")
            conn.execute("DELETE FROM raw_mutations WHERE study_id = ?", [study_id])
            
            # Fast batch insert by letting DuckDB read from Python structures directly
            if raw_mutations:
                # We inject the study_id and dump the raw record to JSON for flexible parsing
                data_tuples = [(study_id, json.dumps(m)) for m in raw_mutations]
                conn.executemany("INSERT INTO raw_mutations (study_id, data) VALUES (?, ?)", data_tuples)
                
            # Create a table for sample clinical data (ONCOTREE_CODE)
            conn.execute("CREATE TABLE IF NOT EXISTS raw_clinical_data (study_id VARCHAR, sample_id VARCHAR, oncotree_code VARCHAR)")
            conn.execute("DELETE FROM raw_clinical_data WHERE study_id = ?", [study_id])
            
            if raw_clinical:
                clin_tuples = [(study_id, c.get("sampleId"), c.get("value")) for c in raw_clinical]
                conn.executemany("INSERT INTO raw_clinical_data (study_id, sample_id, oncotree_code) VALUES (?, ?, ?)", clin_tuples)
                
            update_study_cache_manifest(study_id, "mutations", profile_id)
        else:
            print(f"Using cached mutations for {study_id} (fetched at {status['fetched_at']})")
            
        # Extract unique Gene + Alteration pairs from the loaded JSON
        # Example JSON: {"gene": {"hugoGeneSymbol": "BRAF"}, "proteinChange": "V600E", ...}
        print("Extracting unique variants for annotation...")
        unique_vars = conn.execute("""
            SELECT DISTINCT 
                json_extract_string(data, '$.gene.hugoGeneSymbol') as gene, 
                json_extract_string(data, '$.proteinChange') as alteration
            FROM raw_mutations 
            WHERE study_id = ? 
              AND json_extract_string(data, '$.gene.hugoGeneSymbol') IS NOT NULL 
              AND json_extract_string(data, '$.proteinChange') IS NOT NULL
        """, [study_id]).fetchall()
        
        # Run annotator
        annotate_variants(conn, unique_vars)
        
        # Build the final view joining the raw JSON mutations with the MoAlmanac JSON responses
        print(f"Exporting annotated data to {output_path}...")
        
        # Output standard MAF columns and key MoAlmanac annotations by the sample's OncoTree Code.
        export_query = f"""
            COPY (
                SELECT 
                    json_extract_string(m.data, '$.gene.hugoGeneSymbol') AS HUGO_SYMBOL,
                    json_extract_string(m.data, '$.chr') AS CHROMOSOME,
                    json_extract_string(m.data, '$.startPosition') AS START_POSITION,
                    json_extract_string(m.data, '$.endPosition') AS END_POSITION,
                    json_extract_string(m.data, '$.mutationType') AS VARIANT_CLASSIFICATION,
                    json_extract_string(m.data, '$.variantType') AS VARIANT_TYPE,
                    json_extract_string(m.data, '$.referenceAllele') AS REFERENCE_ALLELE,
                    json_extract_string(m.data, '$.variantAllele') AS TUMOR_SEQ_ALLELE1,
                    json_extract_string(m.data, '$.variantAllele') AS TUMOR_SEQ_ALLELE2,
                    json_extract_string(m.data, '$.sampleId') AS TUMOR_SAMPLE_BARCODE,
                    -- Derive a Mutation Effect proxy from the feature type
                    MAX(json_extract_string(a_feat.payload, '$.feature_type')) AS MUTATION_EFFECT,
                    -- Mark as Oncogenic if any clinical assertion exists for this cancer type
                    CASE WHEN MAX(a_sign.feature_id) IS NOT NULL THEN 'Yes' ELSE '' END AS ONCOGENIC,
                    -- Squash multiple matching annotations into a single string
                    string_agg(
                        COALESCE(a_sign.clinical_significance || ' (' || COALESCE(a_sign.drug, 'N/A') || '): ' || COALESCE(a_sign.disease, 'N/A'), NULL),
                        '; '
                    ) AS MOALMANAC_ANNOTATION
                FROM raw_mutations m
                LEFT JOIN raw_clinical_data c 
                  ON json_extract_string(m.data, '$.sampleId') = c.sample_id
                  AND c.study_id = m.study_id
                LEFT JOIN moalmanac_features_bulk a_feat 
                  ON json_extract_string(m.data, '$.gene.hugoGeneSymbol') = a_feat.gene 
                  AND json_extract_string(m.data, '$.proteinChange') = a_feat.alteration
                LEFT JOIN moalmanac_assertions_bulk a_sign
                  ON a_feat.feature_id = a_sign.feature_id
                  AND json_extract_string(a_sign.payload, '$.oncotree_code') = c.oncotree_code
                WHERE m.study_id = ?
                GROUP BY ALL
            ) TO '{output_path}' (HEADER, DELIMITER '\t');
        """
        conn.execute(export_query, [study_id])
        print(f"Successfully exported to {output_path}")

    finally:
        conn.close()
