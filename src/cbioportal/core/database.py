import duckdb
import os
import queue
from pathlib import Path
from typing import Generator

DEFAULT_DB_PATH = Path("data/cbioportal.duckdb")

# DuckDB's Python binding is not thread-safe for concurrent query execution
# from a shared connection or its cursors. The supported pattern for parallel
# access is separate connection objects created sequentially from the main
# thread, then distributed to request threads via a thread-safe queue.
_pool: "queue.Queue[duckdb.DuckDBPyConnection] | None" = None
_POOL_SIZE = 2  # 2 per worker process; DuckDB parallelises internally per query


def get_db_path() -> Path:
    """Get the database path from environment or default."""
    return Path(os.getenv("CBIO_DB_PATH", DEFAULT_DB_PATH))


def configure(db_path: Path, pool_size: int = _POOL_SIZE) -> None:
    """Pre-create the read-only connection pool. Called once from the lifespan.

    Connections are created sequentially in the calling thread to avoid the
    concurrent-initialisation race in DuckDB's Python binding.
    """
    global _pool
    _pool = queue.Queue(maxsize=pool_size)
    for _ in range(pool_size):
        conn = duckdb.connect(str(db_path), read_only=True)
        # Point spill-to-disk away from the DB directory, which may be a
        # read-only mount (Cloud Run / local Docker :ro bind).
        conn.execute("SET temp_directory='/tmp/duckdb_temp'")
        # Cap per-connection memory so the pool doesn't exhaust container RAM.
        conn.execute("SET memory_limit='6GB'")
        _pool.put(conn)


def get_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """FastAPI dependency: borrows a connection from the pool for one request.

    Declare route handlers as `def` (not `async def`) so FastAPI runs them in
    its anyio thread pool. Each thread blocks here until a connection is free,
    then returns it after the handler completes.
    """
    if _pool is None:
        raise RuntimeError("Database not configured — call configure() before serving")
    conn = _pool.get()
    try:
        yield conn
    finally:
        _pool.put(conn)


def get_connection(db_path: Path = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a one-off connection to the DuckDB database (CLI / loader use)."""
    if db_path is None:
        db_path = get_db_path()
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def init_db(db_path: Path = None) -> None:
    """Initialize the database schema."""
    if db_path is None:
        db_path = get_db_path()
    conn = get_connection(db_path)
    conn.close()
