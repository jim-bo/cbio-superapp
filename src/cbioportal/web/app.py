import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from cbioportal.core.database import get_connection, DEFAULT_DB_PATH
from cbioportal.core.study_repository import load_study_names
from cbioportal.web.routes import home as home_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Open a read-only connection to the DuckDB database
    db_path = Path(os.environ.get("CBIO_DB_PATH", DEFAULT_DB_PATH))
    app.state.db_conn = get_connection(db_path, read_only=True)

    # Load study display names from the studies table in the DB
    app.state.study_names = load_study_names(app.state.db_conn)

    yield

    app.state.db_conn.close()


def create_app():
    app = FastAPI(title="cBioPortal Revamp", lifespan=lifespan)

    # Templates
    templates_path = Path(__file__).parent / "templates"
    templates = Jinja2Templates(directory=str(templates_path))
    app.state.templates = templates

    # Static files
    static_path = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

    # Routes
    app.include_router(home_router.router)

    return app
