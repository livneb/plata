"""initial schema

Revision ID: 20260525_0000
Revises:
Create Date: 2026-05-25 00:00:00

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260525_0000"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("role", sa.String(32), nullable=False, server_default="viewer"),
        sa.Column("totp_secret", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_login_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "trade_ledger",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("trade_ulid", sa.String(26), nullable=False, unique=True),
        sa.Column("proposal_id", sa.String(26)),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("instrument_type", sa.String(16), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("qty", sa.Numeric(28, 12), nullable=False),
        sa.Column("entry_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("exit_price", sa.Numeric(28, 12)),
        sa.Column("fees", sa.Numeric(28, 12), server_default="0"),
        sa.Column("gross_pnl", sa.Numeric(28, 12)),
        sa.Column("net_pnl", sa.Numeric(28, 12)),
        sa.Column("sl_price", sa.Numeric(28, 12)),
        sa.Column("tp_price", sa.Numeric(28, 12)),
        sa.Column("close_reason", sa.String(16)),
        sa.Column("raw_bybit_response", postgresql.JSONB, server_default="{}"),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_trade_ledger_symbol_closed_at", "trade_ledger", ["symbol", "closed_at"])
    op.create_index("ix_trade_ledger_mode_closed_at", "trade_ledger", ["mode", "closed_at"])

    op.create_table(
        "signal_archive",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("signal_ulid", sa.String(26), nullable=False, unique=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_published_at", sa.DateTime(timezone=True)),
        sa.Column("url", sa.Text),
        sa.Column("title", sa.Text),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("metadata", postgresql.JSONB, server_default="{}"),
        sa.Column("is_duplicate", sa.Boolean, server_default=sa.false()),
        sa.Column("dedup_master_ulid", sa.String(26)),
        sa.Column("ingested_to_graph", sa.Boolean, server_default=sa.false()),
        sa.Column("graph_event_ulid", sa.String(26)),
    )
    op.create_index("ix_signal_archive_source_fetched_at", "signal_archive", ["source", "fetched_at"])
    op.create_index("ix_signal_archive_metadata_gin", "signal_archive", ["metadata"], postgresql_using="gin")

    op.create_table(
        "event_price_windows",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("event_ulid", sa.String(26), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_minutes_before", sa.Integer, nullable=False),
        sa.Column("window_minutes_after", sa.Integer, nullable=False),
        sa.Column("ohlcv", postgresql.JSONB, server_default="[]"),
        sa.Column("pct_move_1h", sa.Numeric(10, 6)),
        sa.Column("pct_move_4h", sa.Numeric(10, 6)),
        sa.Column("pct_move_24h", sa.Numeric(10, 6)),
        sa.Column("max_drawdown_24h", sa.Numeric(10, 6)),
        sa.Column("realized_vol_24h", sa.Numeric(10, 6)),
        sa.Column("recovery_minutes", sa.Integer),
        sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "event_ulid", "symbol", "window_minutes_before", "window_minutes_after",
            name="uq_event_price_window",
        ),
    )
    op.create_index(
        "ix_event_price_windows_symbol_event_ts", "event_price_windows", ["symbol", "event_ts"],
    )

    op.create_table(
        "config_settings",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("key", sa.String(128), nullable=False),
        sa.Column("value", postgresql.JSONB, nullable=False),
        sa.Column("updated_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("version", sa.Integer, server_default="1"),
    )
    op.create_index("ix_config_settings_key_version", "config_settings", ["key", "version"])

    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("ts", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("actor", sa.String(32), nullable=False),
        sa.Column("action", sa.String(64), nullable=False),
        sa.Column("target", sa.String(128)),
        sa.Column("payload", postgresql.JSONB, server_default="{}"),
    )
    op.create_index("ix_audit_log_action", "audit_log", ["action"])

    op.create_table(
        "error_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("ts", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("container", sa.String(32), nullable=False),
        sa.Column("agent", sa.String(32), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("error_type", sa.String(128), nullable=False),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("traceback", sa.Text),
        sa.Column("context", postgresql.JSONB, server_default="{}"),
        sa.Column("resolved", sa.Boolean, server_default=sa.false()),
        sa.Column("resolved_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_error_log_severity_ts", "error_log", ["severity", "ts"])
    op.create_index("ix_error_log_agent_ts", "error_log", ["agent", "ts"])
    op.create_index("ix_error_log_resolved_severity_ts", "error_log", ["resolved", "severity", "ts"])
    op.create_index("ix_error_log_context_gin", "error_log", ["context"], postgresql_using="gin")

    op.create_table(
        "backtest_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(16), server_default="pending"),
        sa.Column("config", postgresql.JSONB, server_default="{}"),
        sa.Column("results", postgresql.JSONB, server_default="{}"),
        sa.Column("error", sa.Text),
    )

    op.create_table(
        "backtest_trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("backtest_runs.id"), nullable=False),
        sa.Column("signal_ulid", sa.String(26)),
        sa.Column("trade_ulid", sa.String(26), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("qty", sa.Numeric(28, 12), nullable=False),
        sa.Column("entry_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("exit_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("net_pnl", sa.Numeric(28, 12), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("close_reason", sa.String(16), nullable=False),
    )
    op.create_index("ix_backtest_trades_run_id", "backtest_trades", ["run_id"])


def downgrade() -> None:
    op.drop_table("backtest_trades")
    op.drop_table("backtest_runs")
    op.drop_table("error_log")
    op.drop_table("audit_log")
    op.drop_table("config_settings")
    op.drop_table("event_price_windows")
    op.drop_table("signal_archive")
    op.drop_table("trade_ledger")
    op.drop_table("users")
