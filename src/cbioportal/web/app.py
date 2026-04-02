import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import sessionmaker

from cbioportal.core.database import get_connection, configure as configure_db, DEFAULT_DB_PATH
from cbioportal.core.study_repository import load_study_names
from cbioportal.core.session_repository import Base, make_engine
from cbioportal.web.routes import home as home_router
from cbioportal.web.routes import study_view as study_view_router
from cbioportal.web.routes import results_view as results_view_router
from cbioportal.web.routes import session as session_router
from cbioportal.web.routes import metrics as metrics_router
from cbioportal.web.middleware.session_sync import SessionSyncMiddleware

logger = logging.getLogger(__name__)
_DEFAULT_SESSIONS_DB = "sqlite:///data/sessions.db"


def _warm_page_cache(db_path: Path, study_ids: list[str]) -> None:
    """Scan heavy tables to pull GCS FUSE data into the OS page cache.

    Runs in a background thread so it doesn't block the lifespan (which
    would prevent uvicorn from accepting connections / passing health checks).
    On local disk this completes near-instantly.
    """
    conn = get_connection(db_path, read_only=True)
    try:
        for study_id in study_ids:
            for suffix in ("mutations", "cna", "sv", "gene_panel"):
                table = f'"{study_id}_{suffix}"'
                try:
                    conn.execute(
                        f"SELECT COUNT(*), MIN(COLUMNS(*)) FROM {table}"
                    ).fetchall()
                    logger.info("Warmed page cache for %s", table)
                except Exception:
                    pass  # Table may not exist for this study
    finally:
        conn.close()
    logger.info("Page cache warmup complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # DuckDB is either at CBIO_DB_PATH (local dev / bind-mount) or mounted
    # via GCS FUSE volume (Cloud Run). No download step needed.
    db_path = Path(os.environ.get("CBIO_DB_PATH", DEFAULT_DB_PATH))

    # Pre-create the read-only connection pool (sequentially, main thread).
    # Route handlers borrow a connection via Depends(get_db).
    configure_db(db_path)

    # Load study display names at startup using a temporary connection.
    _startup_conn = get_connection(db_path, read_only=True)
    app.state.study_names = load_study_names(_startup_conn)
    _startup_conn.close()

    # Warm the OS page cache in a background thread. This scans the heaviest
    # tables so GCS FUSE data lands in Linux's page cache. The lifespan yields
    # immediately so uvicorn can accept connections and pass health checks.
    warmup_thread = threading.Thread(
        target=_warm_page_cache,
        args=(db_path, list(app.state.study_names)),
        daemon=True,
    )
    warmup_thread.start()

    # Sessions DB (SQLAlchemy — SQLite for dev, PostgreSQL/AlloyDB for prod).
    # Base.metadata.create_all is a no-op when the table already exists.
    # Alembic (`uv run alembic upgrade head`) is the authoritative tool for prod.
    sessions_url = os.environ.get("CBIO_SESSIONS_DB_URL", _DEFAULT_SESSIONS_DB)
    engine = make_engine(sessions_url)
    try:
        Base.metadata.create_all(engine)
    except Exception:
        pass  # Sibling worker already created the table (SQLite race with --workers > 1)
    app.state.session_factory = sessionmaker(
        bind=engine, autoflush=False, autocommit=False
    )

    yield

    engine.dispose()


def create_app():
    app = FastAPI(title="cBioPortal Revamp", lifespan=lifespan)

    # Templates
    templates_path = Path(__file__).parent.absolute() / "templates"
    templates = Jinja2Templates(directory=str(templates_path))

    # Custom filters
    def comma_number(value):
        try:
            return "{:,}".format(int(value))
        except (ValueError, TypeError):
            return value
    templates.env.filters["comma_number"] = comma_number

    app.state.templates = templates

    # Static files
    static_path = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

    # Middleware (add before routes so it wraps all requests)
    app.add_middleware(SessionSyncMiddleware)

    # Routes
    app.include_router(home_router.router)
    app.include_router(study_view_router.router)
    app.include_router(results_view_router.router)
    app.include_router(session_router.router)
    app.include_router(metrics_router.router)

    return app
