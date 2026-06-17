"""Shared "today" floor for dashboard KPIs.

`today_floor()` returns `max(midnight_UTC_today, system:reset_at)` — used
by topbar + Overview daily aggregates so that after a "Start from scratch"
reset the displayed counters genuinely start from $0 / 0 even though the
calendar day hasn't rolled over.

`system:reset_at` is stamped by `/controls/reset` (see
plata.dashboard.routes.controls). The /money/ page uses the same pattern
inline; this helper keeps the topbar + Overview consistent.
"""
from __future__ import annotations

from datetime import date, datetime, timezone


async def today_floor(redis) -> datetime:
    midnight = datetime.combine(date.today(), datetime.min.time(),
                                 tzinfo=timezone.utc)
    try:
        raw = await redis.get("system:reset_at")
    except Exception:  # noqa: BLE001
        return midnight
    if not raw:
        return midnight
    try:
        reset_at = datetime.fromisoformat(raw)
    except ValueError:
        return midnight
    if reset_at.tzinfo is None:
        reset_at = reset_at.replace(tzinfo=timezone.utc)
    return max(midnight, reset_at)
