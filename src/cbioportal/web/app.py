import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from sqlalchemy.orm import sessionmaker

from cbioportal.core.database import get_connection, DEFAULT_DB_PATH
from cbioportal.core.study_repository import load_study_names
from cbioportal.core.session_repository import Base, make_engine
from cbioportal.web.routes import home as home_router
from cbioportal.web.routes import study_view as study_view_router
from cbioportal.web.routes import results_view as results_view_router
from cbioportal.web.routes import session as session_router
from cbioportal.web.middleware.session_sync import SessionSyncMiddleware

_DEFAULT_SESSIONS_DB = "sqlite:///data/sessions.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Open a read-only connection to the DuckDB database
    db_path = Path(os.environ.get("CBIO_DB_PATH", DEFAULT_DB_PATH))
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
    print(f"DEBUG: Loading templates from {templates_path}")
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

    return app
