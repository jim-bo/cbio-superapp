"""create sessions table

Revision ID: 0001
Revises:
Create Date: 2026-03-31
"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sessions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("owner_token", sa.String(64), nullable=False),
        sa.Column("checksum", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_sessions_type", "sessions", ["type"])
    op.create_index("ix_sessions_owner_token", "sessions", ["owner_token"])
    op.create_index("ix_sessions_type_owner", "sessions", ["type", "owner_token"])


def downgrade() -> None:
    op.drop_index("ix_sessions_type_owner", table_name="sessions")
    op.drop_index("ix_sessions_owner_token", table_name="sessions")
    op.drop_index("ix_sessions_type", table_name="sessions")
    op.drop_table("sessions")
