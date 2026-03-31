"""Alembic environment — compatible with SQLite (dev) and PostgreSQL/AlloyDB (prod).

IMPORTANT: render_as_batch=True is set in both offline and online modes.
This is required for SQLite, which cannot execute ALTER TABLE without
recreating the table. It is harmless on PostgreSQL.
"""
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Import the model metadata so Alembic can detect schema changes.
from cbioportal.core.session_repository import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    """Prefer CBIO_SESSIONS_DB_URL env var; fall back to alembic.ini value."""
    return os.environ.get(
        "CBIO_SESSIONS_DB_URL",
        config.get_main_option("sqlalchemy.url"),
    )


def run_migrations_offline() -> None:
    """Run migrations without a live DB connection (generates SQL scripts)."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,  # REQUIRED for SQLite ALTER TABLE support
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live DB connection."""
    cfg = config.get_section(config.config_ini_section, {})
    cfg["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        cfg,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,  # REQUIRED for SQLite ALTER TABLE support
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
