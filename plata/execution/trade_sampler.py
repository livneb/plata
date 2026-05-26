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
from datetime import datetime, timedelta, timezone
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
SYMBOL_WATCH_CADENCE_SEC = 5 * 60     # refresh every open-position symbol every 5 min
_SYMBOL_WATCH_LAST: dict[str, float] = {}  # symbol → monotonic last-fetched ts


async def _refresh_symbol_watch(symbols: list[str], now: float) -> None:
    """Per-symbol fixed-cadence watcher. One fetch per distinct open-position
    symbol every SYMBOL_WATCH_CADENCE_SEC, independent of per-trade cadence.
    Result stored in Redis hash `symbol:latest:<symbol>` for the UI."""
    redis = get_redis()
    from plata.execution.router import venue_for
    for sym in symbols:
        last = _SYMBOL_WATCH_LAST.get(sym, 0)
        if now - last < SYMBOL_WATCH_CADENCE_SEC:
            continue
        try:
            price = await _latest_price(sym)
            if price is None or price <= 0:
                _SYMBOL_WATCH_LAST[sym] = now  # don't hammer a failing symbol
                continue
            ts_iso = datetime.now(timezone.utc).isoformat()
            await redis.hset(f"symbol:latest:{sym}", mapping={
                "price": price,
                "venue": venue_for(sym),
                "ts": ts_iso,
            })
            await redis.expire(f"symbol:latest:{sym}", 24 * 60 * 60)
            # Append to a capped history list for sparklines / charts —
            # keep the last 288 samples (24h @ 5min cadence).
            await redis.rpush(f"symbol:history:{sym}", f"{ts_iso}|{price}")
            await redis.ltrim(f"symbol:history:{sym}", -288, -1)
            await redis.expire(f"symbol:history:{sym}", 7 * 24 * 60 * 60)
            _SYMBOL_WATCH_LAST[sym] = now
        except Exception as exc:  # noqa: BLE001
            _log.warning("symbol_watch_failed", symbol=sym, error=str(exc)[:160])


def _cadence_seconds(longest_eta_minutes: int) -> int:
    """Map the longest milestone ETA to a sampling cadence.
    Floor at 60s so the topbar 'Open · unrealized' KPI is always fresh."""
    if longest_eta_minutes <= 15:
        return 5
    if longest_eta_minutes <= 4 * 60:
        return 60
    if longest_eta_minutes <= 24 * 60:
        return 60     # was 300 — keep unrealized PnL within a minute
    if longest_eta_minutes <= 7 * 24 * 60:
        return 60     # was 1800
    return 60         # was 6h — same floor, otherwise the UI looks frozen


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
    """Last close price for `symbol`. Routes to Bybit (crypto perps) or Alpaca (US equities)
    based on the symbol shape. Returns None on failure."""
    from plata.execution.router import venue_for
    venue = venue_for(symbol)
    try:
        if venue == "alpaca":
            from plata.execution.alpaca_client import AlpacaClient
            client = AlpacaClient(agent="trade_sampler")
            if not client.configured():
                return None
            t = await client.fetch_ticker(symbol)
            return float(t.get("last") or t.get("close") or 0) or None
        # Default: Bybit — query the last few minutes (a 0-width window
        # returns zero bars, which is why every trade had 0 samples).
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=5)
        bars = await fetch_ohlcv_bybit(symbol, start_ts=start, end_ts=end, interval="1")
        if not bars:
            return None
        return float(bars[-1][4])
    except Exception as exc:  # noqa: BLE001
        _log.warning("price_fetch_failed", symbol=symbol, venue=venue, error=str(exc)[:160])
        return None


async def _sample_one(trade: TradeLedger) -> None:
    """Fetch and store a single sample. Convenience wrapper."""
    price = await _latest_price(trade.symbol)
    await _sample_one_with_price(trade, price)


async def _sample_one_with_price(trade: TradeLedger, price: float | None) -> None:
    """Store a sample using a pre-fetched price. Used to dedupe Bybit calls per tick
    when multiple open trades share a symbol."""
    if price is None or price <= 0:
        return
    entry = float(trade.entry_price or 0)
    if entry <= 0:
        return
    redis = get_redis()
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
    await redis.hset(f"trade:latest:{trade.trade_ulid}", mapping={
        "price": price, "pct": rec["pct"], "ts": rec["ts"],
    })


async def run_sampler_loop() -> None:
    """Forever loop. Picks open trades from Postgres, samples each at its own cadence.

    Logging policy: only emit a Kanban activity entry when we actually sampled
    at least one trade. This prevents the Done lane from being drowned by tick-only
    "Sampled 0" rows when no trade is due for a fresh price.
    """
    _log.info("trade_sampler_starting")
    last_sample: dict[str, float] = {}
    cadences: dict[str, int] = {}
    redis = get_redis()
    while True:
        try:
            async with session_scope() as session:
                rows = (await session.execute(
                    select(TradeLedger).where(TradeLedger.exit_price.is_(None))
                )).scalars().all()
            # Heartbeat for the Active lane on /workflow/.
            await redis.hset("agent_status:trade_sampler", mapping={
                "container": "execution_vault",
                "last_heartbeat": datetime.now(timezone.utc).isoformat(),
                "in_flight": len(rows),
                "halted": "False",
            })
            now = time.monotonic()
            # Refresh the per-symbol watch-list: every distinct symbol with an
            # open position gets a fresh price every SYMBOL_WATCH_CADENCE_SEC,
            # independent of per-trade milestone cadence. Result lands in
            # `symbol:latest:<symbol>` so the dashboard can compute unrealized
            # PnL even for trades whose own sampler cadence is much slower.
            distinct_symbols = sorted({t.symbol for t in rows if t.symbol})
            await _refresh_symbol_watch(distinct_symbols, now)
            sampled_now: list[str] = []
            # Cache the latest price per symbol so N trades on the same symbol
            # = ONE Bybit fetch per tick, not N.
            price_cache: dict[str, float | None] = {}
            for trade in rows:
                if trade.trade_ulid not in cadences:
                    longest = await _longest_milestone_eta(trade.proposal_id)
                    cadences[trade.trade_ulid] = _cadence_seconds(longest)
                cad = cadences[trade.trade_ulid]
                last = last_sample.get(trade.trade_ulid, 0)
                if now - last < cad:
                    continue
                try:
                    if trade.symbol not in price_cache:
                        price_cache[trade.symbol] = await _latest_price(trade.symbol)
                    await _sample_one_with_price(trade, price_cache[trade.symbol])
                    last_sample[trade.trade_ulid] = now
                    sampled_now.append(trade.symbol)
                except Exception as exc:  # noqa: BLE001
                    _log.warning("sample_failed", trade=trade.trade_ulid, error=str(exc)[:160])
            # Only log to /workflow/ Done when there's something worth showing.
            if sampled_now:
                distinct = sorted(set(sampled_now))
                await log_action(
                    "trade_sampler",
                    f"Sampled {len(sampled_now)} of {len(rows)} open trade(s) "
                    f"({len(distinct)} symbol(s): {', '.join(distinct[:5])})",
                )
        except Exception as exc:  # noqa: BLE001
            _log.exception("sampler_loop_error", error=str(exc))
        await asyncio.sleep(5)  # base tick; per-trade cadence is enforced above
