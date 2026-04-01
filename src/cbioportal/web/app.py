import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import sessionmaker

from cbioportal.core.database import get_connection, DEFAULT_DB_PATH
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


def _download_duckdb_from_gcs(gcs_uri: str, dest_path: Path) -> None:
    """Download the DuckDB file from GCS if not already current.

    Only called when CBIO_GCS_DB_URI is set. Authenticates via Application
    Default Credentials — on Cloud Run this is the attached service account.
    Skips download if the local file already matches the remote blob size.
    """
    try:
        from google.cloud import storage  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-storage is required when CBIO_GCS_DB_URI is set. "
            "Ensure it is listed in pyproject.toml dependencies."
        ) from exc

    parsed = urlparse(gcs_uri)
    if parsed.scheme != "gs":
        raise ValueError(f"CBIO_GCS_DB_URI must start with gs://, got: {gcs_uri!r}")

    bucket_name = parsed.netloc
    blob_name = parsed.path.lstrip("/")

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)

    if dest_path.exists():
        blob.reload()
        if dest_path.stat().st_size == blob.size:
            logger.info(
                "DuckDB already current at %s (%d bytes), skipping GCS download.",
                dest_path,
                blob.size,
            )
            return

    logger.info("Downloading DuckDB from %s to %s ...", gcs_uri, dest_path)
    blob.download_to_filename(str(dest_path))
    logger.info("DuckDB download complete: %s", dest_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Download DuckDB from GCS if CBIO_GCS_DB_URI is set (Cloud Run).
    # Falls back to local CBIO_DB_PATH for local dev / bind-mount usage.
    gcs_uri = os.environ.get("CBIO_GCS_DB_URI")
    db_path = Path(os.environ.get("CBIO_DB_PATH", DEFAULT_DB_PATH))

    if gcs_uri:
        _download_duckdb_from_gcs(gcs_uri, db_path)

    # Open a read-only connection to the DuckDB database
    app.state.db_conn = get_connection(db_path, read_only=True)

    # Load study display names from the studies table in the DB
    app.state.study_names = load_study_names(app.state.db_conn)

    # Sessions DB (SQLAlchemy — SQLite for dev, PostgreSQL/AlloyDB for prod).
    # Base.metadata.create_all is a no-op when the table already exists.
    # Alembic (`uv run alembic upgrade head`) is the authoritative tool for prod.
    sessions_url = os.environ.get("CBIO_SESSIONS_DB_URL", _DEFAULT_SESSIONS_DB)
    engine = make_engine(sessions_url)
    Base.metadata.create_all(engine)
    app.state.session_factory = sessionmaker(
        bind=engine, autoflush=False, autocommit=False
    )

    yield

    app.state.db_conn.close()
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
