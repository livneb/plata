"""widen proposals.symbol to 64 chars

Revision ID: 20260608_0000
Revises: 20260601_0000
Create Date: 2026-06-08 00:00:00

Free models occasionally produced symbol strings > 32 chars (full name or
exchange-prefixed slug). Inserts silently failed with StringDataRightTruncation,
losing proposals. Bumped to 64.
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260608_0000"
down_revision: Union[str, None] = "20260601_0000"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "proposals", "symbol",
        existing_type=sa.String(32),
        type_=sa.String(64),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "proposals", "symbol",
        existing_type=sa.String(64),
        type_=sa.String(32),
        existing_nullable=False,
    )
