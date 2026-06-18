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
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select

from plata.core.bus import get_redis
from plata.core.db import Proposal, TradeLedger, session_scope
from plata.dashboard import templates
from plata.dashboard.routes._close_reason import label_for

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
    # The /controls/reset endpoint stamps system:reset_at when the operator
    # clicks "Start from scratch". After that, /money/ treats it as a HARD
    # FLOOR: closed-trade aggregates never include trades from before the
    # reset, regardless of the selected window. This keeps the dashboard
    # showing "current session" cleanly even when the operator picks "All
    # time". History is still on /trades/ for forensics.
    reset_at: datetime | None = None
    try:
        ra_raw = await redis.get("system:reset_at")
        if ra_raw:
            reset_at = datetime.fromisoformat(ra_raw)
            if reset_at.tzinfo is None:
                reset_at = reset_at.replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        reset_at = None
    if reset_at is None:
        # One-shot retro backfill: prior /controls/reset calls (before
        # v2.24.190) didn't stamp the boundary. Look for the most recent
        # CloseReason.RESET row; if present, treat its closed_at as the
        # boundary so the operator's earlier reset takes effect after this
        # deploy without needing to click again. Cache to Redis so we only
        # run the query once per process boot.
        try:
            from sqlalchemy import func as _func
            async with session_scope() as _session:
                latest = (await _session.execute(
                    select(_func.max(TradeLedger.closed_at))
                    .where(TradeLedger.close_reason == "reset")
                )).scalar_one_or_none()
            if latest is not None:
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                reset_at = latest
                try:
                    await redis.set("system:reset_at", latest.isoformat())
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass
    if reset_at is not None:
        # Tighten range_from up to reset_at when the chosen window opens
        # earlier (or has no lower bound, e.g. "All time").
        if range_from is None or range_from < reset_at:
            range_from = reset_at
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

    # ===== Capital allocation breakdown =====
    # The operator's $X "Start from scratch" amount is the TRUE account
    # size. This panel answers the operator's question: "how much have I
    # used / how much is unused / where is the money / what did it earn?"
    cfg = await redis.hgetall("risk_config") or {}
    try:
        baseline_equity = float(cfg.get("account_baseline_equity_usd") or 0)
    except (TypeError, ValueError):
        baseline_equity = 0.0
    BUCKET_META = {
        "few_hours": {"label": "Few hours",  "icon": "⚡",
                        "color": "bg-yellow-100 text-yellow-800 border-yellow-200",
                        "dot": "#eab308"},
        "few_days":  {"label": "Few days",   "icon": "🌊",
                        "color": "bg-blue-100 text-blue-800 border-blue-200",
                        "dot": "#3b82f6"},
        "few_weeks": {"label": "Few weeks",  "icon": "🌒",
                        "color": "bg-cyan-100 text-cyan-800 border-cyan-200",
                        "dot": "#06b6d4"},
        "long_term": {"label": "Long-term",  "icon": "🌱",
                        "color": "bg-purple-100 text-purple-800 border-purple-200",
                        "dot": "#a855f7"},
    }
    DEFAULT_PCTS = {"few_hours": 10.0, "few_days": 25.0,
                     "few_weeks": 30.0, "long_term": 35.0}
    DEFAULT_CONC = {"few_hours": 5, "few_days": 3, "few_weeks": 2,
                     "long_term": 1}
    buckets_view: list[dict] = []
    # Per-bucket aggregates: cap, used (sum entry-notional of open),
    # mark (sum current-mark notional of open), realized (sum closed
    # net_pnl filtered by reset_at), open positions detail list.
    bucket_state: dict[str, dict] = {
        b: {"used": 0.0, "mark": 0.0, "realized": 0.0, "positions": [],
             "closed_positions": 0, "winners": 0, "losers": 0}
        for b in BUCKET_META
    }
    # Join open + closed trades to their Proposal.extras.horizon_bucket
    from plata.core.db import Proposal as _P
    ulids_needed = [r.proposal_id for r in rows if r.proposal_id]
    prop_by_ulid: dict[str, "_P"] = {}
    if ulids_needed:
        async with session_scope() as _session:
            try:
                prs = (await _session.execute(
                    select(_P).where(_P.proposal_ulid.in_(ulids_needed))
                )).scalars().all()
                prop_by_ulid = {p.proposal_ulid: p for p in prs}
            except Exception:  # noqa: BLE001
                pass

    def _bucket_of(trade) -> str | None:
        p = prop_by_ulid.get(trade.proposal_id or "")
        if p and p.extras:
            b = (p.extras or {}).get("horizon_bucket")
            if b in bucket_state:
                return b
        return None

    for r in rows:
        b = _bucket_of(r)
        if b is None:
            continue
        st = bucket_state[b]
        qty = float(r.qty or 0)
        entry = float(r.entry_price or 0)
        if r.exit_price is None:
            # Open — use entry notional for "used" (matches the sizing model)
            # and current mark notional for "mark" (shows current value).
            try:
                sym = await redis.hgetall(f"symbol:latest:{r.symbol}") or {}
                cur = float(sym.get("price") or 0)
            except Exception:  # noqa: BLE001
                cur = 0.0
            mark_price = cur or entry
            used_n = qty * entry
            mark_n = qty * mark_price
            st["used"] += used_n
            st["mark"] += mark_n
            sign = 1.0 if (r.side or "").lower() == "long" else -1.0
            unr = (sign * (mark_price - entry) * qty) if (mark_price and entry) else 0.0
            p = prop_by_ulid.get(r.proposal_id or "")
            st["positions"].append({
                "trade_ulid": r.trade_ulid,
                "symbol": r.symbol,
                "side": r.side,
                "used_usd": round(used_n, 2),
                "mark_usd": round(mark_n, 2),
                "unrealized_usd": round(unr, 2),
                "reasoning": ((p.reasoning or "")[:240] if p and p.reasoning else None),
            })
        else:
            close_ts = r.closed_at or r.opened_at
            if not _in_range(close_ts):
                continue
            pnl = float(r.net_pnl or 0)
            st["realized"] += pnl
            st["closed_positions"] += 1
            if pnl > 0:
                st["winners"] += 1
            elif pnl < 0:
                st["losers"] += 1

    # Compose the view rows.
    total_used = 0.0
    for b, meta in BUCKET_META.items():
        try:
            pct = float(cfg.get(f"horizon_{b}_budget_pct")
                         or DEFAULT_PCTS[b])
        except (TypeError, ValueError):
            pct = DEFAULT_PCTS[b]
        try:
            conc = int(cfg.get(f"horizon_{b}_target_concurrent")
                        or DEFAULT_CONC[b])
        except (TypeError, ValueError):
            conc = DEFAULT_CONC[b]
        cap = baseline_equity * pct / 100.0 if baseline_equity > 0 else 0.0
        st = bucket_state[b]
        # Sort positions by used_usd desc so the biggest stake reads first.
        st["positions"].sort(key=lambda p: p["used_usd"], reverse=True)
        total_used += st["used"]
        buckets_view.append({
            "key": b,
            **meta,
            "pct_of_equity": pct,
            "target_concurrent": conc,
            "cap_usd": round(cap, 2),
            "used_usd": round(st["used"], 2),
            "mark_usd": round(st["mark"], 2),
            "available_usd": round(max(0.0, cap - st["used"]), 2),
            "fill_pct": round((st["used"] / cap * 100.0), 1) if cap > 0 else 0.0,
            "open_count": len(st["positions"]),
            "open_slots": max(0, conc - len(st["positions"])),
            "realized_usd": round(st["realized"], 2),
            "closed_count": st["closed_positions"],
            "winners": st["winners"],
            "losers": st["losers"],
            "positions": st["positions"],
        })
    available_cash = max(0.0, baseline_equity - total_used)
    allocation = {
        "baseline_equity_usd": round(baseline_equity, 2),
        "total_used_usd": round(total_used, 2),
        "available_usd": round(available_cash, 2),
        "used_pct": round((total_used / baseline_equity * 100.0), 1) if baseline_equity > 0 else 0.0,
        "buckets": buckets_view,
    }

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
            "allocation": allocation,
        },
    )


@router.get("/closures")
async def closures_since(since: str | None = None, limit: int = 25):
    """Closures whose `closed_at > since` (ISO-8601). Drives the page-load
    banner that surfaces what closed while the user was away. LEFT JOINs
    the Proposal row so LLM-driven closes (position monitor) can surface
    their reasoning.

    `since` is optional — when missing, returns the most recent `limit`
    closures so the banner has something to show on first load.
    """
    cutoff: datetime | None = None
    if since:
        try:
            cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
        except ValueError:
            cutoff = None
    # Floor at system:reset_at — closures from before the reset boundary
    # are no longer "current session" and shouldn't surface on the banner.
    try:
        ra_raw = await get_redis().get("system:reset_at")
        if ra_raw:
            reset_at = datetime.fromisoformat(ra_raw)
            if reset_at.tzinfo is None:
                reset_at = reset_at.replace(tzinfo=timezone.utc)
            if cutoff is None or cutoff < reset_at:
                cutoff = reset_at
    except Exception:  # noqa: BLE001
        pass
    async with session_scope() as session:
        q = select(TradeLedger).where(TradeLedger.exit_price.isnot(None))
        if cutoff:
            q = q.where(TradeLedger.closed_at > cutoff)
        q = q.order_by(TradeLedger.closed_at.desc()).limit(max(1, min(100, limit)))
        rows = (await session.execute(q)).scalars().all()
        # Pull matching Proposal rows in one shot so we can read LLM
        # reasoning from extras.adjustment_executed_reasoning (set by the
        # position monitor when its LLM verdict closed the trade).
        proposal_map: dict[str, Proposal] = {}
        if rows:
            trade_ulids = [r.trade_ulid for r in rows]
            proposals = (await session.execute(
                select(Proposal).where(Proposal.trade_ulid.in_(trade_ulids))
            )).scalars().all()
            proposal_map = {p.trade_ulid: p for p in proposals if p.trade_ulid}

    out = []
    for r in rows:
        label, tooltip = label_for(r.close_reason)
        pnl = float(r.net_pnl or 0)
        inv = (float(r.qty or 0) * float(r.entry_price or 0)) or 0
        pct = (pnl / inv * 100.0) if inv > 0 else 0.0
        # LLM reasoning, if any — position monitor stores it on the proposal.
        llm_reasoning = None
        p = proposal_map.get(r.trade_ulid)
        if p and p.extras:
            llm_reasoning = (p.extras or {}).get("adjustment_executed_reasoning")
        held_sec = int((r.closed_at - r.opened_at).total_seconds()) \
            if (r.closed_at and r.opened_at) else 0
        out.append({
            "trade_ulid": r.trade_ulid,
            "symbol": r.symbol,
            "side": r.side,
            "qty": float(r.qty or 0),
            "entry_price": float(r.entry_price or 0),
            "exit_price": float(r.exit_price or 0) if r.exit_price else None,
            "net_pnl": round(pnl, 4),
            "pct": round(pct, 3),
            "close_reason": (r.close_reason or "").lower(),
            "close_label": label,
            "close_tooltip": tooltip,
            "opened_at": r.opened_at.isoformat() if r.opened_at else None,
            "closed_at": r.closed_at.isoformat() if r.closed_at else None,
            "held_sec": held_sec,
            "llm_reasoning": llm_reasoning,
        })
    return JSONResponse({"closures": out,
                          "as_of": datetime.now(timezone.utc).isoformat()})


@router.get("/openings")
async def openings_since(since: str | None = None, limit: int = 25):
    """Recently opened positions, with the strategist's reasoning. Powers the
    sibling section on the closures banner: "X positions opened since your
    last visit, here's WHY." JOINs the Proposal row so we surface the LLM
    reasoning that justified each open.
    """
    cutoff: datetime | None = None
    if since:
        try:
            cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
        except ValueError:
            cutoff = None
    # Floor at system:reset_at so opens from before "Start from scratch"
    # don't pollute the banner.
    try:
        ra_raw = await get_redis().get("system:reset_at")
        if ra_raw:
            reset_at = datetime.fromisoformat(ra_raw)
            if reset_at.tzinfo is None:
                reset_at = reset_at.replace(tzinfo=timezone.utc)
            if cutoff is None or cutoff < reset_at:
                cutoff = reset_at
    except Exception:  # noqa: BLE001
        pass
    async with session_scope() as session:
        q = select(TradeLedger)
        if cutoff:
            q = q.where(TradeLedger.opened_at > cutoff)
        q = q.order_by(TradeLedger.opened_at.desc()).limit(max(1, min(100, limit)))
        rows = (await session.execute(q)).scalars().all()
        proposal_map: dict[str, Proposal] = {}
        if rows:
            ids = [r.proposal_id for r in rows if r.proposal_id]
            if ids:
                proposals = (await session.execute(
                    select(Proposal).where(Proposal.proposal_ulid.in_(ids))
                )).scalars().all()
                proposal_map = {p.proposal_ulid: p for p in proposals}

    out = []
    for r in rows:
        p = proposal_map.get(r.proposal_id or "")
        reasoning = (p.reasoning if p else None) or None
        conviction = float(p.conviction) if (p and p.conviction is not None) else None
        notional = float(r.qty or 0) * float(r.entry_price or 0)
        out.append({
            "trade_ulid": r.trade_ulid,
            "symbol": r.symbol,
            "side": r.side,
            "qty": float(r.qty or 0),
            "entry_price": float(r.entry_price or 0),
            "notional_usd": round(notional, 4),
            "conviction": conviction,
            "reasoning": reasoning,
            "opened_at": r.opened_at.isoformat() if r.opened_at else None,
        })
    return JSONResponse({"openings": out,
                          "as_of": datetime.now(timezone.utc).isoformat()})
