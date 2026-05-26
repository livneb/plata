"""Positions watch — top-level page with charts.

`/positions/` — list of every symbol with an open position, with a
sparkline per row and an aggregate index chart.

`/positions/<symbol>` — full page for one symbol: price history,
list of open trades on it, click-through to each trade's detail.
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select

from plata.core.bus import get_redis
from plata.core.db import TradeLedger, session_scope
from plata.dashboard import templates
from plata.execution.router import venue_for

router = APIRouter(prefix="/positions", tags=["positions"])


async def _symbol_card(symbol: str, trades: list, redis) -> dict[str, Any]:
    from datetime import datetime as _dt, timezone as _tz
    latest = await redis.hgetall(f"symbol:latest:{symbol}")
    price = float(latest.get("price") or 0) or None
    ts_iso = latest.get("ts")
    age_sec = None
    if ts_iso:
        try:
            age_sec = (_dt.now(_tz.utc) - _dt.fromisoformat(ts_iso)).total_seconds()
        except Exception:  # noqa: BLE001
            pass
    # Sparkline points (last 60 samples = 5h of history)
    raw = await redis.lrange(f"symbol:history:{symbol}", -60, -1)
    spark: list[float] = []
    for line in raw:
        try:
            spark.append(float(line.split("|", 1)[1]))
        except Exception:  # noqa: BLE001
            continue
    unrealized = 0.0
    net_long, net_short = 0.0, 0.0
    for t in trades:
        qty = float(t.qty or 0)
        entry = float(t.entry_price or 0)
        if (t.side or "").lower() == "long":
            net_long += qty
            if price and entry:
                unrealized += (price - entry) * qty
        else:
            net_short += qty
            if price and entry:
                unrealized -= (price - entry) * qty
    return {
        "symbol": symbol,
        "venue": venue_for(symbol),
        "price": price,
        "ts": ts_iso,
        "age_sec": int(age_sec) if age_sec is not None else None,
        "spark": spark,
        "net_long": net_long,
        "net_short": net_short,
        "unrealized": round(unrealized, 2),
        "trade_count": len(trades),
    }


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    async with session_scope() as session:
        rows = (await session.execute(
            select(TradeLedger).where(TradeLedger.exit_price.is_(None))
        )).scalars().all()
    by_symbol: dict[str, list] = {}
    for t in rows:
        by_symbol.setdefault(t.symbol, []).append(t)
    cards: list[dict[str, Any]] = []
    for sym, trades in by_symbol.items():
        cards.append(await _symbol_card(sym, trades, redis))
    cards.sort(key=lambda c: -abs(c["unrealized"]))
    return templates.TemplateResponse(
        request, "pages/positions_index.html",
        {"active": "positions", "cards": cards, "cadence_sec": 5 * 60},
    )


@router.get("/{symbol}", response_class=HTMLResponse)
async def detail(request: Request, symbol: str):
    symbol = symbol.upper()
    redis = get_redis()
    async with session_scope() as session:
        trades = (await session.execute(
            select(TradeLedger)
            .where(TradeLedger.symbol == symbol)
            .where(TradeLedger.exit_price.is_(None))
        )).scalars().all()
    card = await _symbol_card(symbol, trades, redis)
    # Full history for the chart (up to 288 samples = 24h @ 5min).
    raw = await redis.lrange(f"symbol:history:{symbol}", 0, -1)
    points: list[dict[str, Any]] = []
    for line in raw:
        try:
            ts, p = line.split("|", 1)
            points.append({"x": ts, "y": float(p)})
        except Exception:  # noqa: BLE001
            continue
    return templates.TemplateResponse(
        request, "pages/positions_detail.html",
        {
            "active": "positions",
            "card": card,
            "trades": [
                {
                    "ulid": t.trade_ulid, "side": t.side,
                    "qty": float(t.qty or 0), "entry": float(t.entry_price or 0),
                    "opened_at": t.opened_at,
                } for t in trades
            ],
            "points": points,
        },
    )


@router.get("/{symbol}/history.json")
async def history_json(symbol: str):
    """Raw price history for chart polling — same data the detail page renders
    initially, useful for an auto-refresh that doesn't reload the whole page."""
    redis = get_redis()
    raw = await redis.lrange(f"symbol:history:{symbol.upper()}", 0, -1)
    points = []
    for line in raw:
        try:
            ts, p = line.split("|", 1)
            points.append({"x": ts, "y": float(p)})
        except Exception:  # noqa: BLE001
            continue
    return JSONResponse({"symbol": symbol.upper(), "points": points})
