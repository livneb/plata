"""US equity market-hours helper — used to decide whether to poll Alpaca
symbols on a fast cadence (open) or a slow one (closed / pre-market / weekend).

Crypto venues are 24/7 so this only affects Alpaca routing.

NOTE: This is a pragmatic implementation, NOT a full holiday calendar. It
covers the regular session (Mon-Fri 09:30-16:00 ET) and treats pre/post
market as "closed" for sampling purposes. US federal holidays are NOT
specially handled — on a holiday Mon we'd still poll, which is harmless
(Alpaca just returns stale snapshots). When market_open() returns False,
the next-open computation gives the next 09:30 ET on the next weekday.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone, tzinfo

try:
    from zoneinfo import ZoneInfo
    _ET: tzinfo = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    _ET = timezone(timedelta(hours=-5))

REG_OPEN = time(9, 30)
REG_CLOSE = time(16, 0)


def _now_et() -> datetime:
    return datetime.now(tz=_ET)


def market_open(now: datetime | None = None) -> bool:
    n = (now or _now_et()).astimezone(_ET)
    if n.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    t = n.time()
    return REG_OPEN <= t < REG_CLOSE


def seconds_until_next_open(now: datetime | None = None) -> int:
    """Returns seconds until the next regular-session open (09:30 ET, Mon-Fri).
    Returns 0 if the market is currently open."""
    n = (now or _now_et()).astimezone(_ET)
    if market_open(n):
        return 0
    candidate = n.replace(hour=REG_OPEN.hour, minute=REG_OPEN.minute,
                           second=0, microsecond=0)
    if candidate <= n:
        candidate = candidate + timedelta(days=1)
    # Skip weekends
    while candidate.weekday() >= 5:
        candidate = candidate + timedelta(days=1)
    delta = candidate - n
    return int(delta.total_seconds())


def seconds_until_next_close(now: datetime | None = None) -> int:
    n = (now or _now_et()).astimezone(_ET)
    if not market_open(n):
        return 0
    close_today = n.replace(hour=REG_CLOSE.hour, minute=REG_CLOSE.minute,
                             second=0, microsecond=0)
    return max(0, int((close_today - n).total_seconds()))


def next_open_iso(now: datetime | None = None) -> str:
    """ISO timestamp of next open, in UTC, for the UI countdown."""
    n = (now or _now_et()).astimezone(_ET)
    if market_open(n):
        close_today = n.replace(hour=REG_CLOSE.hour, minute=REG_CLOSE.minute,
                                 second=0, microsecond=0)
        return close_today.astimezone(timezone.utc).isoformat()
    candidate = n.replace(hour=REG_OPEN.hour, minute=REG_OPEN.minute,
                           second=0, microsecond=0)
    if candidate <= n:
        candidate = candidate + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate = candidate + timedelta(days=1)
    return candidate.astimezone(timezone.utc).isoformat()
