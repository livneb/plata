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
from datetime import datetime, timedelta, timezone

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


RANGE_PRESETS = {
    "4h":  timedelta(hours=4),
    "24h": timedelta(hours=24),
    "7d":  timedelta(days=7),
    "30d": timedelta(days=30),
}


def _resolve_range(preset: str, from_s: str | None, to_s: str | None
                    ) -> tuple[datetime | None, datetime | None, str]:
    """Return (range_from, range_to, normalized preset key). `None` means
    open-ended. Custom takes precedence over preset when from/to are present."""
    now = datetime.now(timezone.utc)
    if from_s or to_s:
        try:
            rf = datetime.fromisoformat(from_s).replace(tzinfo=timezone.utc) \
                if from_s else None
            rt = datetime.fromisoformat(to_s).replace(tzinfo=timezone.utc) \
                if to_s else None
            return rf, rt, "custom"
        except ValueError:
            pass
    if preset in RANGE_PRESETS:
        return now - RANGE_PRESETS[preset], None, preset
    return None, None, "all"


@router.get("/", response_class=HTMLResponse)
async def index(request: Request,
                range: str = "30d",
                from_: str | None = None,
                to: str | None = None):
    # FastAPI doesn't let us bind `from` as a kwarg directly (reserved word);
    # accept via query_params for the custom range fallback.
    qp = request.query_params
    from_param = qp.get("from") or from_
    to_param = qp.get("to") or to
    range_from, range_to, preset = _resolve_range(range, from_param, to_param)

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
    cumulative: list[dict] = []  # filled after we sort closed trades by close_ts
    # Daily aggregates inside the range — for the per-day earned/lost bars.
    by_day: dict[str, dict[str, float]] = defaultdict(lambda: {
        "earned": 0.0, "lost": 0.0, "wins": 0, "losses": 0,
    })

    def _in_range(ts):
        if ts is None:
            return False
        if range_from and ts < range_from:
            return False
        if range_to and ts > range_to:
            return False
        return True

    # First pass: collect closed-in-range trades into a list so we can sort by
    # close_ts before building the cumulative series. The original loop built
    # cumulative in opened_at order but charted by close_ts, which caused the
    # backwards-traveling zigzag the user saw on the chart.
    closed_in_range: list[dict] = []
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
            close_ts = r.closed_at or r.opened_at
            # Range filter: only count closed trades whose close_ts is in range.
            if not _in_range(close_ts):
                continue
            pnl = float(r.net_pnl or 0)
            if pnl > 0:
                total_earned += pnl
                bucket["earned"] += pnl
                bucket["wins"] += 1
            elif pnl < 0:
                total_lost += pnl
                bucket["lost"] += pnl
                bucket["losses"] += 1
            closed_in_range.append({
                "close_ts": close_ts,
                "pnl": pnl,
                "symbol": r.symbol,
                "cls": cls,
            })
            # Per-day earned/lost — granularity depends on range size:
            #   <= 4h  -> bucket by 15-minute, label HH:MM
            #   <= 24h -> bucket by hour, label HH:00
            #   else   -> bucket by day, label YYYY-MM-DD
            span_hours = ((range_to or datetime.now(timezone.utc))
                           - range_from).total_seconds() / 3600 if range_from else 99999
            if span_hours <= 4:
                key = close_ts.strftime("%Y-%m-%d %H:") + f"{(close_ts.minute // 15) * 15:02d}"
            elif span_hours <= 24:
                key = close_ts.strftime("%Y-%m-%d %H:00")
            else:
                key = close_ts.strftime("%Y-%m-%d")
            day = by_day[key]
            if pnl > 0:
                day["earned"] += pnl
                day["wins"] += 1
            elif pnl < 0:
                day["lost"] += pnl
                day["losses"] += 1

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

    # Cumulative net-PnL series: sort closed trades by close timestamp, then
    # accumulate. Sorted order is what makes the line read left-to-right
    # monotonically in time — without it, an earlier-opened trade that closed
    # later would be added to cum_net AFTER a later-opened trade that closed
    # earlier, dragging the line backwards on the chart.
    closed_in_range.sort(key=lambda c: c["close_ts"])
    cum_net = 0.0
    for c in closed_in_range:
        cum_net += c["pnl"]
        cumulative.append({
            "ts": c["close_ts"].isoformat(),
            "cum": round(cum_net, 4),
            "pnl": round(c["pnl"], 4),
            "symbol": c["symbol"],
            "cls": c["cls"],
        })

    # Daily/hourly series for the bar chart, sorted by key.
    daily_series = []
    for key in sorted(by_day.keys()):
        d = by_day[key]
        daily_series.append({
            "label": key,
            "earned": round(d["earned"], 4),
            "lost": round(d["lost"], 4),
            "wins": d["wins"],
            "losses": d["losses"],
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
            "daily_series": daily_series,
            "range_preset": preset,
            "range_from": range_from.isoformat() if range_from else "",
            "range_to": range_to.isoformat() if range_to else "",
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    )
