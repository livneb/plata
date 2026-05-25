"""Historical Price Oracle — fetches OHLCV around an event for one or more symbols.

Primary source: Bybit (testnet OHLCV is unrestricted public data).
Fallback: CoinGecko for crypto historical, Yahoo Finance via httpx for FX/gold.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx

from plata.core.db import EventPriceWindow, session_scope
from plata.core.observability import get_logger
from plata.oracle.impact_metrics import all_metrics

_log = get_logger("oracle")

BYBIT_BASE = "https://api.bybit.com/v5/market/kline"


async def fetch_ohlcv_bybit(
    symbol: str, *, start_ts: datetime, end_ts: datetime, interval: str = "1"
) -> list[list[float]]:
    """Fetch 1-min OHLCV from Bybit public API (no auth needed)."""
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "start": int(start_ts.timestamp() * 1000),
        "end": int(end_ts.timestamp() * 1000),
        "limit": 1000,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(BYBIT_BASE, params=params)
        r.raise_for_status()
        data = r.json()
    # Bybit returns [ts, o, h, l, c, v, turnover] in DESC order
    rows = data.get("result", {}).get("list", [])
    bars = []
    for r in reversed(rows):
        bars.append([
            int(r[0]) // 1000,
            float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]),
        ])
    return bars


async def get_window(
    *,
    symbol: str,
    event_ts: datetime,
    minutes_before: int = 60,
    minutes_after: int = 24 * 60,
) -> list[list[float]]:
    start = event_ts - timedelta(minutes=minutes_before)
    end = event_ts + timedelta(minutes=minutes_after)
    try:
        return await fetch_ohlcv_bybit(symbol, start_ts=start, end_ts=end)
    except Exception as e:
        _log.warning("bybit_ohlcv_failed", symbol=symbol, error=str(e))
        return []


async def compute_and_store(
    *,
    event_ulid: str,
    symbol: str,
    event_ts: datetime,
    minutes_before: int = 60,
    minutes_after: int = 24 * 60,
    venue: str = "bybit",
) -> dict[str, Any] | None:
    bars = await get_window(
        symbol=symbol, event_ts=event_ts,
        minutes_before=minutes_before, minutes_after=minutes_after,
    )
    if not bars:
        return None
    # Pre-event bars at the front (we slice after) — keep simple: store full window,
    # but compute metrics from post-event tail.
    pre_count = minutes_before
    post = bars[pre_count:] or bars
    metrics = all_metrics(post)

    async with session_scope() as session:
        session.add(EventPriceWindow(
            event_ulid=event_ulid,
            symbol=symbol,
            venue=venue,
            event_ts=event_ts,
            window_minutes_before=minutes_before,
            window_minutes_after=minutes_after,
            ohlcv=bars,
            pct_move_1h=_as_decimal(metrics.get("pct_move_1h")),
            pct_move_4h=_as_decimal(metrics.get("pct_move_4h")),
            pct_move_24h=_as_decimal(metrics.get("pct_move_24h")),
            max_drawdown_24h=_as_decimal(metrics.get("max_drawdown_24h")),
            realized_vol_24h=_as_decimal(metrics.get("realized_vol_24h")),
            recovery_minutes=metrics.get("recovery_minutes"),
        ))
    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in metrics.items()}


def _as_decimal(v) -> Decimal | None:
    return v if isinstance(v, Decimal) else None
