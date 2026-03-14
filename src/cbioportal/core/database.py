import duckdb
import os
from pathlib import Path

def get_db_path() -> Path:
    """Get the database path from environment or default."""
    return Path(os.getenv("CBIO_DB_PATH", "data/cbioportal.duckdb"))

def get_connection(db_path: Path = None, read_only: bool = False):
    """Get a connection to the DuckDB database."""
    if db_path is None:
        db_path = get_db_path()
    
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)

def init_db(db_path: Path = None):
    """Initialize the database schema."""
    if db_path is None:
        db_path = get_db_path()
    conn = get_connection(db_path)
    # Define your schema here
    # conn.execute("CREATE TABLE IF NOT EXISTS studies ...")
    conn.close()
