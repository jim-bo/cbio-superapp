"""Session service — SQLAlchemy model, engine factory, and CRUD.

Compatible with SQLite (local dev) and PostgreSQL/AlloyDB (prod).

Compatibility rules (MUST follow):
- Use JSON column type — never JSONB (breaks SQLite).
- JSON path queries use SQLAlchemy subscript operator, never raw JSON_EXTRACT/->> SQL.
- No RETURNING clause — use db.refresh(record) after commit.
- Alembic env.py must keep render_as_batch=True for SQLite ALTER TABLE support.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Index, JSON, String, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Session as SASession, sessionmaker


SESSION_TYPES = frozenset(
    {
        "virtual_study",
        "settings",
        "custom_data",
        "custom_gene_list",
        "main_session",
        "comparison_session",
    }
)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class SessionRecord(Base):
    __tablename__ = "sessions"

    id = Column(String(36), primary_key=True)
    type = Column(String(32), nullable=False)
    data = Column(JSON, nullable=False)
    owner_token = Column(String(64), nullable=False)
    checksum = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        Index("ix_sessions_type", "type"),
        Index("ix_sessions_owner_token", "owner_token"),
        Index("ix_sessions_type_owner", "type", "owner_token"),
    )


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------


def make_engine(database_url: str):
    """Create a SQLAlchemy engine for the sessions DB.

    SQLite (local dev):
      - check_same_thread=False so FastAPI worker threads can share the connection.
      - WAL journal mode + foreign_keys enabled via a connect event.

    PostgreSQL / AlloyDB (prod):
      - pool_pre_ping=True to detect stale connections.
      - Do NOT use AlloyDB-specific extensions; plain postgresql+psycopg2:// URL.
    """
    connect_args: dict = {}
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}

    engine = create_engine(database_url, connect_args=connect_args, pool_pre_ping=True)

    if database_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(conn, _rec):
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")

    return engine


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _hash_token(raw_token: str) -> str:
    """SHA-256 hash of the raw browser token. Never store the raw value."""
    return hashlib.sha256(raw_token.encode()).hexdigest()


def _checksum(data: object) -> str:
    """Deterministic SHA-256 of the serialised data payload."""
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def create_session(
    db: SASession,
    session_type: str,
    data: dict,
    raw_token: str,
) -> SessionRecord:
    """Create and persist a new session. Returns the saved record."""
    record = SessionRecord(
        id=str(uuid.uuid4()),
        type=session_type,
        data=data,
        owner_token=_hash_token(raw_token),
        checksum=_checksum(data),
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


def get_session(db: SASession, session_id: str) -> SessionRecord | None:
    """Retrieve any session by ID. No ownership check — sessions are public by ID."""
    return db.get(SessionRecord, session_id)


def list_sessions(
    db: SASession,
    session_type: str,
    raw_token: str,
) -> list[SessionRecord]:
    """List all sessions of a given type owned by the caller."""
    hashed = _hash_token(raw_token)
    return (
        db.query(SessionRecord)
        .filter_by(type=session_type, owner_token=hashed)
        .order_by(SessionRecord.updated_at.desc())
        .all()
    )


def delete_session(
    db: SASession,
    session_id: str,
    raw_token: str,
) -> bool:
    """Delete a session. Returns True on success, False if not found or not owned."""
    hashed = _hash_token(raw_token)
    record = db.get(SessionRecord, session_id)
    if record and record.owner_token == hashed:
        db.delete(record)
        db.commit()
        return True
    return False


def upsert_settings(
    db: SASession,
    page: str,
    origin: list[str],
    data: dict,
    raw_token: str,
) -> SessionRecord:
    """Create or update a page-settings session for (page, origin, owner).

    `origin` is sorted before storage so callers can pass study IDs in any order
    and still match the same row.
    """
    hashed = _hash_token(raw_token)
    origin_key = json.dumps(sorted(origin))
    full_data = {**data, "page": page, "origin_key": origin_key}

    # SQLAlchemy JSON subscript compiles to JSON_EXTRACT on SQLite,
    # data->>'key' on PostgreSQL — never write raw dialect SQL here.
    existing = (
        db.query(SessionRecord)
        .filter(
            SessionRecord.type == "settings",
            SessionRecord.owner_token == hashed,
            SessionRecord.data["page"].as_string() == page,
            SessionRecord.data["origin_key"].as_string() == origin_key,
        )
        .first()
    )

    if existing:
        existing.data = full_data
        existing.checksum = _checksum(full_data)
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return existing

    return create_session(db, "settings", full_data, raw_token)


def fetch_settings(
    db: SASession,
    page: str,
    origin: list[str],
    raw_token: str,
) -> SessionRecord | None:
    """Retrieve the caller's page-settings session for (page, origin)."""
    hashed = _hash_token(raw_token)
    origin_key = json.dumps(sorted(origin))
    return (
        db.query(SessionRecord)
        .filter(
            SessionRecord.type == "settings",
            SessionRecord.owner_token == hashed,
            SessionRecord.data["page"].as_string() == page,
            SessionRecord.data["origin_key"].as_string() == origin_key,
        )
        .first()
    )
