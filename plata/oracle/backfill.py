"""Batch + per-event price-window backfill, with inline graph attach."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from plata.core.graph import attach_price_impact, get_event
from plata.core.observability import get_logger
from plata.core.schemas import EntityRef
from plata.oracle.price_oracle import compute_and_store
from plata.oracle.symbol_mapping import symbols_for_entities

_log = get_logger("oracle.backfill")


async def backfill_event_async(event_ulid: str, entity_refs: Iterable[EntityRef]) -> None:
    """Compute price windows for all symbols affected by an event, mirror to graph."""
    refs = list(entity_refs)
    if not refs:
        return
    symbols = symbols_for_entities(refs)

    full = await get_event(event_ulid)
    if not full:
        return
    try:
        event_ts = datetime.fromisoformat(full["ts"])
    except Exception:
        event_ts = datetime.now(timezone.utc)

    for sym in symbols:
        try:
            metrics = await compute_and_store(
                event_ulid=event_ulid, symbol=sym, event_ts=event_ts,
            )
            if metrics:
                await attach_price_impact(event_ulid, sym, metrics)
        except Exception:
            _log.exception("backfill_failed", event_ulid=event_ulid, symbol=sym)
