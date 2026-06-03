"""add sysop_findings table

Revision ID: 20260601_0000
Revises: 20260529_0000
Create Date: 2026-06-01 00:00:00

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260601_0000"
down_revision: Union[str, None] = "20260529_0000"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sysop_findings",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("pattern", sa.String(64), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("evidence", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("proposed_fix", sa.Text, nullable=False),
        sa.Column("fix_action", sa.String(64), nullable=True),
        sa.Column("fix_action_args", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("state", sa.String(24), nullable=False, server_default="new"),
        sa.Column("actor", sa.String(64), nullable=True),
        sa.Column("fingerprint", sa.String(64), nullable=False),
    )
    op.create_index("ix_sysop_findings_created_at", "sysop_findings", ["created_at"])
    op.create_index("ix_sysop_findings_pattern", "sysop_findings", ["pattern"])
    op.create_index("ix_sysop_findings_severity", "sysop_findings", ["severity"])
    op.create_index("ix_sysop_findings_state", "sysop_findings", ["state"])
    op.create_index("ix_sysop_findings_fingerprint", "sysop_findings", ["fingerprint"])
    op.create_index("ix_sysop_findings_state_created", "sysop_findings", ["state", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_sysop_findings_state_created", table_name="sysop_findings")
    op.drop_index("ix_sysop_findings_fingerprint", table_name="sysop_findings")
    op.drop_index("ix_sysop_findings_state", table_name="sysop_findings")
    op.drop_index("ix_sysop_findings_severity", table_name="sysop_findings")
    op.drop_index("ix_sysop_findings_pattern", table_name="sysop_findings")
    op.drop_index("ix_sysop_findings_created_at", table_name="sysop_findings")
    op.drop_table("sysop_findings")
