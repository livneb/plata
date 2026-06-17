"""Money dashboard — all-time P&L overview and asset-class breakdown.

The /money/ page answers four questions at a glance:
  1. How much capital is tied up in open positions RIGHT NOW?
  2. How much money have we made (sum of winning closed trades)?
  3. How much have we lost (sum of losing closed trades)?
  4. How does that split across stocks / crypto / currencies?

Cumulative net-P&L over time is plotted as a line chart so the trajectory
is obvious without reading numbers.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from plata.core.bus import get_redis
from plata.core.db import TradeLedger, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/money", tags=["money"])


# Asset-class buckets the page splits by. Keep small — 3 buckets matches
# what the user actually thinks about when scanning the page.
ASSET_CLASS_LABELS = {
    "crypto":     {"label": "Crypto",      "icon": "₿",  "color": "#f59e0b"},
    "currencies": {"label": "Currencies",  "icon": "💱", "color": "#06b6d4"},
    "stocks":     {"label": "Stocks / ETF","icon": "📈", "color": "#10b981"},
    "other":      {"label": "Other",       "icon": "❓", "color": "#94a3b8"},
}

# Forex pairs are exchange-traded futures-style tickers; we don't take them
# today but the bucket is here so the breakdown is honest if they appear.
FOREX_SYMBOLS = {
    "EURUSDT", "GBPUSDT", "USDJPY", "AUDUSDT", "USDCAD", "USDCHF", "EURUSD",
    "GBPUSD",
}


def _classify(symbol: str, venue: str) -> str:
    """Categorize a position. Venue is the strongest signal (alpaca → US
    equities; bybit → crypto perps). Symbol heuristics handle the residual."""
    s = (symbol or "").upper()
    v = (venue or "").lower()
    if s in FOREX_SYMBOLS:
        return "currencies"
    if "bybit" in v:
        return "crypto"
    if "alpaca" in v:
        return "stocks"
    # Symbol-based fallbacks — for trades pre-venue-tagging or oddities.
    if s.endswith("USDT") or s.endswith("USD"):
        return "crypto"
    return "other"


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    async with session_scope() as session:
        rows = (await session.execute(
            select(TradeLedger).order_by(TradeLedger.opened_at.asc())
        )).scalars().all()

    # --- All-time aggregates ---
    total_earned = 0.0     # sum of winning closed trades' net PnL
    total_lost = 0.0       # sum of losing closed trades' net PnL (negative)
    open_notional = 0.0    # qty × current price for every open position
    open_unrealized = 0.0  # mark-to-market PnL on opens
    by_class: dict[str, dict[str, float]] = defaultdict(lambda: {
        "open_notional": 0.0, "open_unrealized": 0.0,
        "earned": 0.0, "lost": 0.0,
        "wins": 0, "losses": 0, "open_count": 0,
    })
    cumulative: list[dict] = []
    cum_net = 0.0

    for r in rows:
        cls = _classify(r.symbol, r.venue)
        bucket = by_class[cls]
        if r.exit_price is None:
            # Open — value at current price.
            try:
                sym = await redis.hgetall(f"symbol:latest:{r.symbol}") or {}
                cur = float(sym.get("price") or 0)
            except Exception:  # noqa: BLE001
                cur = 0.0
            qty = float(r.qty or 0)
            entry = float(r.entry_price or 0)
            notional = qty * (cur or entry)  # fall back to entry if no live mark
            open_notional += notional
            bucket["open_notional"] += notional
            bucket["open_count"] += 1
            if cur and entry > 0 and qty > 0:
                sign = 1.0 if (r.side or "").lower() == "long" else -1.0
                un = sign * (cur - entry) * qty
                open_unrealized += un
                bucket["open_unrealized"] += un
        else:
            pnl = float(r.net_pnl or 0)
            if pnl > 0:
                total_earned += pnl
                bucket["earned"] += pnl
                bucket["wins"] += 1
            elif pnl < 0:
                total_lost += pnl
                bucket["lost"] += pnl
                bucket["losses"] += 1
            # Cumulative net-PnL series — point per closed trade.
            cum_net += pnl
            cumulative.append({
                "ts": (r.closed_at or r.opened_at).isoformat(),
                "cum": round(cum_net, 4),
                "pnl": round(pnl, 4),
                "symbol": r.symbol,
                "cls": cls,
            })

    net_realized = total_earned + total_lost   # total_lost is negative
    win_count = sum(b["wins"] for b in by_class.values())
    loss_count = sum(b["losses"] for b in by_class.values())
    closed_count = win_count + loss_count
    win_rate = (win_count / closed_count * 100.0) if closed_count else 0.0

    # Reshape by_class into a stable, template-friendly list.
    classes_view = []
    for key, meta in ASSET_CLASS_LABELS.items():
        b = by_class.get(key)
        if not b:
            continue
        classes_view.append({
            "key": key, **meta,
            **{k: round(v, 4) for k, v in b.items() if isinstance(v, float)},
            "wins": b["wins"], "losses": b["losses"], "open_count": b["open_count"],
            "net_realized": round(b["earned"] + b["lost"], 4),
        })

    return templates.TemplateResponse(
        request,
        "pages/money.html",
        {
            "active": "money",
            "total_earned": round(total_earned, 2),
            "total_lost": round(total_lost, 2),
            "net_realized": round(net_realized, 2),
            "open_notional": round(open_notional, 2),
            "open_unrealized": round(open_unrealized, 2),
            "open_count": sum(b["open_count"] for b in by_class.values()),
            "closed_count": closed_count,
            "win_count": win_count,
            "loss_count": loss_count,
            "win_rate": round(win_rate, 1),
            "classes_view": classes_view,
            "cumulative": cumulative,
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    )
