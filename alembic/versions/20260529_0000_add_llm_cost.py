"""add llm_cost table

Revision ID: 20260529_0000
Revises: 20260525_0000
Create Date: 2026-05-29 00:00:00

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260529_0000"
down_revision: Union[str, None] = "20260525_0000"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "llm_cost",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("ts", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("agent", sa.String(64), nullable=False),
        sa.Column("model", sa.String(128), nullable=True),
        sa.Column("prompt_tokens", sa.Integer, nullable=True),
        sa.Column("completion_tokens", sa.Integer, nullable=True),
        sa.Column("cost_usd", sa.Numeric(18, 8), nullable=False),
    )
    op.create_index("ix_llm_cost_ts", "llm_cost", ["ts"])
    op.create_index("ix_llm_cost_agent", "llm_cost", ["agent"])
    op.create_index("ix_llm_cost_agent_ts", "llm_cost", ["agent", "ts"])
    op.create_index("ix_llm_cost_ts_agent", "llm_cost", ["ts", "agent"])


def downgrade() -> None:
    op.drop_index("ix_llm_cost_ts_agent", table_name="llm_cost")
    op.drop_index("ix_llm_cost_agent_ts", table_name="llm_cost")
    op.drop_index("ix_llm_cost_agent", table_name="llm_cost")
    op.drop_index("ix_llm_cost_ts", table_name="llm_cost")
    op.drop_table("llm_cost")
