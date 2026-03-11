import duckdb
from pathlib import Path

DEFAULT_DB_PATH = Path("data/cbioportal.duckdb")

def get_connection(db_path: Path = DEFAULT_DB_PATH, read_only: bool = False):
    """Get a connection to the DuckDB database."""
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)

def init_db(db_path: Path = DEFAULT_DB_PATH):
    """Initialize the database schema."""
    conn = get_connection(db_path)
    # Define your schema here
    # conn.execute("CREATE TABLE IF NOT EXISTS studies ...")
    conn.close()
