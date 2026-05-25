from datetime import datetime, timezone
from decimal import Decimal

from plata.core.schemas import (
    EnrichedEvent,
    EntityRef,
    EntityType,
    EventCategory,
    RawSignal,
    Side,
    SignalSource,
    TradeProposal,
)


def test_raw_signal_defaults():
    s = RawSignal(source=SignalSource.REDDIT, body="hello")
    assert len(s.ulid) == 26
    assert s.is_duplicate is False
    assert s.created_at.tzinfo is not None


def test_enriched_event_validates_sentiment():
    e = EnrichedEvent(
        source_signal_ulid="X" * 26,
        source=SignalSource.REDDIT,
        summary="test",
        category=EventCategory.MACRO,
        sentiment_magnitude=0.5,
    )
    assert e.sentiment_magnitude == 0.5


def test_entity_ref_clamps_sentiment_range():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        EntityRef(type=EntityType.COUNTRY, id="US", name="USA", sentiment=2.0)


def test_trade_proposal_serializable():
    p = TradeProposal(
        triggering_event_ulid="A" * 26,
        symbol="BTCUSDT",
        side=Side.LONG,
        conviction=0.7,
        reasoning="rationale",
        suggested_notional_usd=Decimal("100"),
    )
    json_str = p.model_dump_json()
    assert "BTCUSDT" in json_str
