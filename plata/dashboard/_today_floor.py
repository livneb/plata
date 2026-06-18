"""Shared "today" floor for dashboard KPIs.

`today_floor()` returns `max(midnight_UTC_today, system:reset_at)` — used
by topbar + Overview daily aggregates so that after a "Start from scratch"
reset the displayed counters genuinely start from $0 / 0 even though the
calendar day hasn't rolled over.

`system:reset_at` is stamped by `/controls/reset` (see
plata.dashboard.routes.controls). When that key is missing — e.g. the
operator reset before v2.24.190 shipped, or the Redis SET failed
transiently — we retro-backfill from the most recent
`CloseReason.RESET` row in TradeLedger and cache it back into Redis.
This guarantees the topbar honors the boundary even when the original
SET didn't land.

The /money/ page does its own equivalent backfill inline; this helper is
the source of truth for everywhere else.
"""
from __future__ import annotations

from datetime import date, datetime, timezone


async def today_floor(redis) -> datetime:
    midnight = datetime.combine(date.today(), datetime.min.time(),
                                 tzinfo=timezone.utc)
    reset_at = await _read_reset_at(redis)
    if reset_at is None:
        reset_at = await _backfill_reset_at(redis)
    if reset_at is None:
        return midnight
    return max(midnight, reset_at)


async def _read_reset_at(redis) -> datetime | None:
    try:
        raw = await redis.get("system:reset_at")
    except Exception:  # noqa: BLE001
        return None
    if not raw:
        return None
    try:
        v = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v


async def _backfill_reset_at(redis) -> datetime | None:
    """If `system:reset_at` is missing, find the most recent
    CloseReason.RESET row in TradeLedger and stamp its closed_at.
    Cached back into Redis so subsequent lookups are O(1)."""
    try:
        from sqlalchemy import func, select
        from plata.core.db import TradeLedger, session_scope
        async with session_scope() as session:
            latest = (await session.execute(
                select(func.max(TradeLedger.closed_at))
                .where(TradeLedger.close_reason == "reset")
            )).scalar_one_or_none()
        if latest is None:
            return None
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        try:
            await redis.set("system:reset_at", latest.isoformat())
        except Exception:  # noqa: BLE001
            pass
        return latest
    except Exception:  # noqa: BLE001
        return None
