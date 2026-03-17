"""DuckDB-based cache for cBioPortal API responses and annotations."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from cbioportal.core.cbio_config import get_config

CACHE_DIR = Path.home() / ".cbio" / "cache"
CACHE_DB_PATH = CACHE_DIR / "cache.duckdb"


def get_cache_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a connection to the local DuckDB cache."""
    if not read_only:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(CACHE_DB_PATH), read_only=read_only)

    # Initialize schema if missing
    if not read_only:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_manifest (
                study_id VARCHAR,
                data_type VARCHAR,
                molecular_profile_id VARCHAR,
                fetched_at TIMESTAMP,
                PRIMARY KEY (study_id, data_type)
            );

            CREATE TABLE IF NOT EXISTS moalmanac_cache (
                variant_hash VARCHAR PRIMARY KEY,
                payload JSON,
                fetched_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS studies (
                study_id VARCHAR PRIMARY KEY,
                type_of_cancer VARCHAR,
                name VARCHAR,
                description VARCHAR,
                short_name VARCHAR,
                all_sample_count INTEGER,
                synced_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS clinical_attributes (
                study_id VARCHAR,
                attr_id VARCHAR,
                display_name VARCHAR,
                description VARCHAR,
                datatype VARCHAR,
                patient_attribute BOOLEAN,
                priority INTEGER,
                PRIMARY KEY (study_id, attr_id)
            );

            CREATE TABLE IF NOT EXISTS clinical_data (
                study_id VARCHAR,
                sample_id VARCHAR,
                patient_id VARCHAR,
                attr_id VARCHAR,
                value VARCHAR,
                patient_attribute BOOLEAN
            );
        """)
    return conn


def get_study_cache_status(study_id: str, data_type: str) -> dict | None:
    """Check if a study's data is cached and within TTL."""
    try:
        conn = get_cache_connection(read_only=True)
    except duckdb.IOException:
        # DB doesn't exist yet
        return None
        
    try:
        res = conn.execute(
            "SELECT fetched_at, molecular_profile_id FROM cache_manifest WHERE study_id = ? AND data_type = ?",
            [study_id, data_type]
        ).fetchone()
        
        if not res:
            return None
            
        fetched_at, profile_id = res
        ttl_days = get_config().get("cache", {}).get("ttl_days", 180)
        
        # Ensure timezone info matches for comparison (DuckDB returns naive timestamps)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        age_days = (now - fetched_at).days
        
        if age_days > ttl_days:
            return None
            
        return {"fetched_at": fetched_at, "molecular_profile_id": profile_id}
    finally:
        conn.close()


def update_study_cache_manifest(study_id: str, data_type: str, molecular_profile_id: str) -> None:
    """Update the manifest timestamp for a cached study payload."""
    conn = get_cache_connection()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn.execute("""
            INSERT INTO cache_manifest (study_id, data_type, molecular_profile_id, fetched_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (study_id, data_type) DO UPDATE SET
                molecular_profile_id = excluded.molecular_profile_id,
                fetched_at = excluded.fetched_at
        """, [study_id, data_type, molecular_profile_id, now])
    finally:
        conn.close()


def upsert_studies(conn: duckdb.DuckDBPyConnection, studies: list[dict]) -> None:
    """Bulk upsert study metadata into the studies table."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for s in studies:
        cancer_type = s.get("cancerType") or {}
        rows.append((
            s.get("studyId"),
            cancer_type.get("cancerTypeId") or s.get("cancerTypeId"),
            s.get("name"),
            s.get("description"),
            s.get("shortName") or s.get("studyId"),
            s.get("allSampleCount", 0),
            now,
        ))
    conn.executemany("""
        INSERT INTO studies (study_id, type_of_cancer, name, description, short_name, all_sample_count, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (study_id) DO UPDATE SET
            type_of_cancer = excluded.type_of_cancer,
            name = excluded.name,
            description = excluded.description,
            short_name = excluded.short_name,
            all_sample_count = excluded.all_sample_count,
            synced_at = excluded.synced_at
    """, rows)


def upsert_clinical_attributes(
    conn: duckdb.DuckDBPyConnection, study_id: str, attrs: list[dict]
) -> None:
    """Upsert clinical attribute definitions for a study."""
    rows = [
        (
            study_id,
            a.get("clinicalAttributeId"),
            a.get("displayName"),
            a.get("description"),
            a.get("datatype"),
            bool(a.get("patientAttribute", False)),
            int(a.get("priority", 0)) if a.get("priority") is not None else 0,
        )
        for a in attrs
    ]
    conn.executemany("""
        INSERT INTO clinical_attributes
            (study_id, attr_id, display_name, description, datatype, patient_attribute, priority)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (study_id, attr_id) DO UPDATE SET
            display_name = excluded.display_name,
            description = excluded.description,
            datatype = excluded.datatype,
            patient_attribute = excluded.patient_attribute,
            priority = excluded.priority
    """, rows)


def upsert_clinical_data(
    conn: duckdb.DuckDBPyConnection, study_id: str, rows: list[dict]
) -> None:
    """Replace clinical data rows for a study (delete + insert)."""
    conn.execute("DELETE FROM clinical_data WHERE study_id = ?", [study_id])
    if not rows:
        return
    data = [
        (
            study_id,
            r.get("sampleId"),
            r.get("patientId"),
            r.get("clinicalAttributeId"),
            r.get("value"),
            r.get("clinicalDataType") == "PATIENT",
        )
        for r in rows
    ]
    conn.executemany("""
        INSERT INTO clinical_data (study_id, sample_id, patient_id, attr_id, value, patient_attribute)
        VALUES (?, ?, ?, ?, ?, ?)
    """, data)
