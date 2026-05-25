"""SQLAlchemy 2.0 ORM models — the durable cold-storage layer."""
from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

class TradeLedger(Base):
    __tablename__ = "trade_ledger"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trade_ulid: Mapped[str] = mapped_column(String(26), unique=True, index=True)
    proposal_id: Mapped[str | None] = mapped_column(String(26), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    venue: Mapped[str] = mapped_column(String(32))
    instrument_type: Mapped[str] = mapped_column(String(16))
    side: Mapped[str] = mapped_column(String(8))
    mode: Mapped[str] = mapped_column(String(8), index=True)
    qty: Mapped[Decimal] = mapped_column(Numeric(28, 12))
    entry_price: Mapped[Decimal] = mapped_column(Numeric(28, 12))
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12), nullable=True)
    fees: Mapped[Decimal] = mapped_column(Numeric(28, 12), default=Decimal("0"))
    gross_pnl: Mapped[Decimal | None] = mapped_column(Numeric(28, 12), nullable=True)
    net_pnl: Mapped[Decimal | None] = mapped_column(Numeric(28, 12), nullable=True)
    sl_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12), nullable=True)
    tp_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12), nullable=True)
    close_reason: Mapped[str | None] = mapped_column(String(16), nullable=True)
    raw_bybit_response: Mapped[dict] = mapped_column(JSONB, default=dict)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    __table_args__ = (
        Index("ix_trade_ledger_symbol_closed_at", "symbol", "closed_at"),
        Index("ix_trade_ledger_mode_closed_at", "mode", "closed_at"),
    )


# ---------------------------------------------------------------------------
# Signal archive (raw + dedup)
# ---------------------------------------------------------------------------

class SignalArchive(Base):
    __tablename__ = "signal_archive"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    signal_ulid: Mapped[str] = mapped_column(String(26), unique=True, index=True)
    source: Mapped[str] = mapped_column(String(32), index=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    body: Mapped[str] = mapped_column(Text)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, default=dict)
    is_duplicate: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    dedup_master_ulid: Mapped[str | None] = mapped_column(String(26), nullable=True, index=True)
    ingested_to_graph: Mapped[bool] = mapped_column(Boolean, default=False)
    graph_event_ulid: Mapped[str | None] = mapped_column(String(26), nullable=True, index=True)

    __table_args__ = (
        Index("ix_signal_archive_source_fetched_at", "source", "fetched_at"),
        Index("ix_signal_archive_metadata_gin", "metadata", postgresql_using="gin"),
    )


# ---------------------------------------------------------------------------
# Historical price oracle output
# ---------------------------------------------------------------------------

class EventPriceWindow(Base):
    __tablename__ = "event_price_windows"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_ulid: Mapped[str] = mapped_column(String(26), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    venue: Mapped[str] = mapped_column(String(32))
    event_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    window_minutes_before: Mapped[int] = mapped_column(Integer)
    window_minutes_after: Mapped[int] = mapped_column(Integer)
    ohlcv: Mapped[list] = mapped_column(JSONB, default=list)
    pct_move_1h: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    pct_move_4h: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    pct_move_24h: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    max_drawdown_24h: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    realized_vol_24h: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    recovery_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "event_ulid", "symbol", "window_minutes_before", "window_minutes_after",
            name="uq_event_price_window",
        ),
        Index("ix_event_price_windows_symbol_event_ts", "symbol", "event_ts"),
    )


# ---------------------------------------------------------------------------
# Configuration (append-only with versioning)
# ---------------------------------------------------------------------------

class ConfigSetting(Base):
    __tablename__ = "config_settings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(128), index=True)
    value: Mapped[dict] = mapped_column(JSONB)
    updated_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    version: Mapped[int] = mapped_column(Integer, default=1)

    __table_args__ = (
        Index("ix_config_settings_key_version", "key", "version"),
    )


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(32), default="viewer")
    totp_secret: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Audit log (intentional user actions)
# ---------------------------------------------------------------------------

class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=True
    )
    actor: Mapped[str] = mapped_column(String(32))
    action: Mapped[str] = mapped_column(String(64), index=True)
    target: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)


# ---------------------------------------------------------------------------
# Error log (system errors)
# ---------------------------------------------------------------------------

class ErrorLog(Base):
    __tablename__ = "error_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    container: Mapped[str] = mapped_column(String(32))
    agent: Mapped[str] = mapped_column(String(32), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    error_type: Mapped[str] = mapped_column(String(128))
    message: Mapped[str] = mapped_column(Text)
    traceback: Mapped[str | None] = mapped_column(Text, nullable=True)
    context: Mapped[dict] = mapped_column(JSONB, default=dict)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    resolved_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=True
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_error_log_severity_ts", "severity", "ts"),
        Index("ix_error_log_agent_ts", "agent", "ts"),
        Index("ix_error_log_resolved_severity_ts", "resolved", "severity", "ts"),
        Index("ix_error_log_context_gin", "context", postgresql_using="gin"),
    )


# ---------------------------------------------------------------------------
# Backtesting
# ---------------------------------------------------------------------------

class BacktestRun(Base):
    __tablename__ = "backtest_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending | running | completed | failed
    config: Mapped[dict] = mapped_column(JSONB, default=dict)  # date_range, prompt_version, risk_snapshot_version, models
    results: Mapped[dict] = mapped_column(JSONB, default=dict)  # total_trades, win_rate, sharpe, max_dd, etc.
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    trades: Mapped[list["BacktestTrade"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class BacktestTrade(Base):
    __tablename__ = "backtest_trades"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("backtest_runs.id"), index=True
    )
    signal_ulid: Mapped[str | None] = mapped_column(String(26), nullable=True, index=True)
    trade_ulid: Mapped[str] = mapped_column(String(26))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(8))
    qty: Mapped[Decimal] = mapped_column(Numeric(28, 12))
    entry_price: Mapped[Decimal] = mapped_column(Numeric(28, 12))
    exit_price: Mapped[Decimal] = mapped_column(Numeric(28, 12))
    net_pnl: Mapped[Decimal] = mapped_column(Numeric(28, 12))
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    close_reason: Mapped[str] = mapped_column(String(16))

    run: Mapped[BacktestRun] = relationship(back_populates="trades")
