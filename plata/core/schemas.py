"""Pydantic schemas — the shared vocabulary that flows between agents on Redis Streams."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from plata.core.ulid import new_ulid


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SignalSource(StrEnum):
    REDDIT = "reddit"
    CRYPTOPANIC = "cryptopanic"
    NEWSAPI = "newsapi"
    GDELT = "gdelt"
    CRYPTONEWS = "cryptonews"
    LUNARCRUSH = "lunarcrush"
    BYBIT_WS = "bybit_ws"
    WHALEALERT = "whalealert"
    HISTORIAN = "historian"
    RSS = "rss"
    TELEGRAM = "telegram"
    MARKET_TICKER = "market_ticker"
    MANUAL = "manual"


class EntityType(StrEnum):
    COUNTRY = "country"
    PERSON = "person"
    ORGANIZATION = "organization"
    TICKER = "ticker"
    TOPIC = "topic"


class EventCategory(StrEnum):
    WAR = "war"
    CYBER = "cyber"
    MACRO = "macro"
    REGULATION = "regulation"
    EARNINGS = "earnings"
    SOCIAL_VIRALITY = "social_virality"
    WHALE_MOVE = "whale_move"
    PRICE_ACTION = "price_action"
    OTHER = "other"


class Side(StrEnum):
    LONG = "long"
    SHORT = "short"


class TradeMode(StrEnum):
    PAPER = "paper"
    LIVE = "live"


class CloseReason(StrEnum):
    STOP_LOSS = "sl"
    TAKE_PROFIT = "tp"
    MANUAL = "manual"
    KILL_SWITCH = "kill_switch"
    TIMEOUT = "timeout"


# ---------------------------------------------------------------------------
# Base message
# ---------------------------------------------------------------------------

class StreamMessage(BaseModel):
    """Common envelope for every message on the bus."""

    model_config = ConfigDict(frozen=True, use_enum_values=True)

    ulid: str = Field(default_factory=new_ulid)
    created_at: datetime = Field(default_factory=utcnow)


# ---------------------------------------------------------------------------
# Raw signal (Scraper → Graph Ingestion)
# ---------------------------------------------------------------------------

class RawSignal(StreamMessage):
    source: SignalSource
    source_published_at: datetime | None = None
    url: str | None = None
    title: str | None = None
    body: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    # Set by dedup before publishing
    is_duplicate: bool = False
    dedup_master_ulid: str | None = None


# ---------------------------------------------------------------------------
# Enriched event (Graph Ingestion → Strategist)
# ---------------------------------------------------------------------------

class EntityRef(BaseModel):
    model_config = ConfigDict(frozen=True, use_enum_values=True)

    type: EntityType
    id: str  # canonical id, e.g. "US" or "BTC" or "elon_musk"
    name: str
    sentiment: float = Field(ge=-1.0, le=1.0, default=0.0)


class EnrichedEvent(StreamMessage):
    source_signal_ulid: str
    source: SignalSource
    summary: str
    category: EventCategory
    sentiment_magnitude: float = Field(ge=0.0, le=1.0)
    entities: list[EntityRef] = Field(default_factory=list)
    # Filled by oracle backfill (mirrored from Postgres for hot-path use)
    price_impact: dict[str, dict[str, float]] | None = None


# ---------------------------------------------------------------------------
# Trade proposal (Strategist → Risk Manager)
# ---------------------------------------------------------------------------

class AnalogousEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_ulid: str
    similarity: float = Field(ge=0.0, le=1.0)
    summary: str
    price_impact: dict[str, dict[str, float]] | None = None
    trade_outcome: dict[str, float] | None = None  # from Reviewer feedback


class Milestone(BaseModel):
    """One stop along the expected price trajectory.

    Example: eta_minutes=10080 (1 week), expected_pct_move=+0.30, confidence=0.7
    means "I expect ~+30% after one week with moderate confidence".
    """

    model_config = ConfigDict(frozen=True)

    eta_minutes: int = Field(ge=1)
    expected_pct_move: float  # signed; +0.30 = +30%; -0.05 = -5%
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str | None = None


class TradeProposal(StreamMessage):
    triggering_event_ulid: str
    symbol: str
    venue: str = "bybit_testnet"
    instrument_type: str = "perp"  # perp | spot
    side: Side
    conviction: float = Field(ge=0.0, le=1.0)
    reasoning: str
    similar_events: list[AnalogousEvent] = Field(default_factory=list)
    milestones: list[Milestone] = Field(default_factory=list)
    suggested_notional_usd: Decimal | None = None
    suggested_sl_pct: float | None = None
    suggested_tp_pct: float | None = None


# ---------------------------------------------------------------------------
# Risk decision (Risk Manager → HITL gate → Executor)
# ---------------------------------------------------------------------------

class RiskDecision(StreamMessage):
    proposal_ulid: str
    approved: bool
    requires_hitl: bool
    rejection_reason: str | None = None
    final_qty: Decimal | None = None
    final_notional_usd: Decimal | None = None
    final_sl_price: Decimal | None = None
    final_tp_price: Decimal | None = None
    risk_snapshot: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Executed trade (Executor → Reviewer + Postgres)
# ---------------------------------------------------------------------------

class ExecutedTrade(StreamMessage):
    decision_ulid: str
    proposal_ulid: str
    trade_ulid: str
    mode: TradeMode
    symbol: str
    venue: str
    instrument_type: str
    side: Side
    qty: Decimal
    entry_price: Decimal
    sl_price: Decimal | None = None
    tp_price: Decimal | None = None
    fees: Decimal = Decimal("0")
    raw_bybit_response: dict[str, Any] = Field(default_factory=dict)
    opened_at: datetime = Field(default_factory=utcnow)


class TradeClosure(StreamMessage):
    trade_ulid: str
    proposal_ulid: str
    symbol: str
    venue: str
    mode: TradeMode
    side: Side
    qty: Decimal
    entry_price: Decimal
    exit_price: Decimal
    fees: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    close_reason: CloseReason
    opened_at: datetime
    closed_at: datetime = Field(default_factory=utcnow)


# ---------------------------------------------------------------------------
# System / heartbeat
# ---------------------------------------------------------------------------

class AgentHeartbeat(StreamMessage):
    agent: str
    container: str
    last_processed_ulid: str | None = None
    pending: int = 0
    in_flight: int = 0
    error_count_60s: int = 0
