"""Adaptive price sampler for open trades.

Runs in execution_vault alongside the executor. For each open trade in the ledger,
samples Bybit's latest price at a cadence chosen from the trade's longest milestone:

    longest milestone ETA  →  sample every
    -------------------------------------------
    < 15 min               →  5 sec
    < 4 hours              →  1 min
    < 24 hours             →  5 min
    < 7 days               →  30 min
    >= 7 days              →  6 hours

Samples land in Redis as a capped list `trade:samples:<trade_ulid>`, each entry
`{"ts": iso, "price": float, "pct": signed_pct_from_entry}`. The trade-detail
page reads them and overlays the actual price on the predicted-trajectory chart.

The sampler is best-effort: any failure for a single trade is logged and skipped;
the loop keeps going for the others.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from plata.agents.base import log_action
from plata.core.bus import get_redis
from plata.core.db import TradeLedger, session_scope
from plata.core.observability import get_logger
from plata.oracle.price_oracle import fetch_ohlcv_bybit

_log = get_logger("trade_sampler")

MAX_SAMPLES_PER_TRADE = 720           # cap memory (e.g. 720×5s = 1h, 720×1min = 12h, etc.)
PROPOSAL_CACHE_TTL_SEC = 600          # remember the longest-milestone lookup per trade


def _cadence_seconds(longest_eta_minutes: int) -> int:
    """Map the longest milestone ETA to a sampling cadence."""
    if longest_eta_minutes <= 15:
        return 5
    if longest_eta_minutes <= 4 * 60:
        return 60
    if longest_eta_minutes <= 24 * 60:
        return 300
    if longest_eta_minutes <= 7 * 24 * 60:
        return 1800
    return 6 * 60 * 60


async def _longest_milestone_eta(proposal_ulid: str | None) -> int:
    """Scan trading_proposals:stream for the proposal payload and return its max eta_minutes."""
    if not proposal_ulid:
        return 24 * 60  # default: 1 day
    redis = get_redis()
    cache_key = f"trade:cadence_cache:{proposal_ulid}"
    cached = await redis.get(cache_key)
    if cached:
        try:
            return int(cached)
        except (TypeError, ValueError):
            pass
    try:
        entries = await redis.xrevrange("trading_proposals:stream", count=2000)
    except Exception:  # noqa: BLE001
        entries = []
    longest = 24 * 60
    for _id, fields in entries:
        raw = fields.get("data")
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:  # noqa: BLE001
            continue
        if payload.get("ulid") != proposal_ulid:
            continue
        milestones = payload.get("milestones") or []
        if milestones:
            longest = max(int(m.get("eta_minutes") or 0) for m in milestones) or longest
        break
    await redis.set(cache_key, longest, ex=PROPOSAL_CACHE_TTL_SEC)
    return longest


async def _latest_price(symbol: str) -> float | None:
    """Fetch the last close price from Bybit's 1-min OHLCV. Returns None on failure."""
    end = datetime.now(timezone.utc)
    start = end.replace(microsecond=0)
    try:
        bars = await fetch_ohlcv_bybit(symbol, start_ts=end, end_ts=end, interval="1")
    except Exception as exc:  # noqa: BLE001
        _log.warning("price_fetch_failed", symbol=symbol, error=str(exc)[:160])
        return None
    if not bars:
        return None
    # Bybit returns rows like [openTime, open, high, low, close, volume, turnover].
    try:
        return float(bars[-1][4])
    except (ValueError, IndexError, TypeError):
        return None


async def _sample_one(trade: TradeLedger) -> None:
    redis = get_redis()
    price = await _latest_price(trade.symbol)
    if price is None or price <= 0:
        return
    entry = float(trade.entry_price or 0)
    if entry <= 0:
        return
    sign = 1.0 if (trade.side or "").lower() == "long" else -1.0
    pct = sign * (price - entry) / entry * 100.0
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "pct": round(pct, 6),
    }
    key = f"trade:samples:{trade.trade_ulid}"
    await redis.rpush(key, json.dumps(rec))
    await redis.ltrim(key, -MAX_SAMPLES_PER_TRADE, -1)
    # also stash latest price for the dashboard tile
    await redis.hset(f"trade:latest:{trade.trade_ulid}", mapping={
        "price": price, "pct": rec["pct"], "ts": rec["ts"],
    })


async def run_sampler_loop() -> None:
    """Forever loop. Picks open trades from Postgres, samples each at its own cadence."""
    _log.info("trade_sampler_starting")
    # Per-trade last-sample timestamp so we honor per-trade cadence in one global loop.
    last_sample: dict[str, float] = {}
    cadences: dict[str, int] = {}  # per-trade cadence in seconds
    while True:
        try:
            async with session_scope() as session:
                rows = (await session.execute(
                    select(TradeLedger).where(TradeLedger.exit_price.is_(None))
                )).scalars().all()
            now = time.monotonic()
            for trade in rows:
                if trade.trade_ulid not in cadences:
                    longest = await _longest_milestone_eta(trade.proposal_id)
                    cadences[trade.trade_ulid] = _cadence_seconds(longest)
                cad = cadences[trade.trade_ulid]
                last = last_sample.get(trade.trade_ulid, 0)
                if now - last < cad:
                    continue
                try:
                    await _sample_one(trade)
                    last_sample[trade.trade_ulid] = now
                except Exception as exc:  # noqa: BLE001
                    _log.warning("sample_failed", trade=trade.trade_ulid, error=str(exc)[:160])
            await log_action("trade_sampler", f"Sampled {len(rows)} open trade(s)")
        except Exception as exc:  # noqa: BLE001
            _log.exception("sampler_loop_error", error=str(exc))
        await asyncio.sleep(5)  # base tick; per-trade cadence is enforced above
