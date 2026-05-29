"""Open/closed positions from Postgres ledger + decision-chain detail view."""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, select

from plata.core.bus import Streams, get_redis
from plata.core.db import AuditLog, TradeLedger, session_scope
from plata.core.graph import event_key
from plata.dashboard import templates

router = APIRouter(prefix="/trades", tags=["trades"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    from plata.core.db import Proposal
    from plata.execution.router import venue_for
    redis = get_redis()
    async with session_scope() as session:
        result = await session.execute(
            select(TradeLedger).order_by(TradeLedger.opened_at.desc()).limit(100)
        )
        rows = result.scalars().all()
        # Bulk-fetch the originating proposal for each trade in ONE query so
        # we can show conviction + a reasoning preview without N round-trips.
        prop_by_ulid: dict[str, Proposal] = {}
        prop_ulids = [r.proposal_id for r in rows if r.proposal_id]
        if prop_ulids:
            try:
                pres = await session.execute(
                    select(Proposal).where(Proposal.proposal_ulid.in_(prop_ulids))
                )
                for p in pres.scalars().all():
                    prop_by_ulid[p.proposal_ulid] = p
            except Exception:  # noqa: BLE001
                pass
    # Enrich each row with current price + unrealized PnL + venue.
    # Open trades use the symbol watch cache (refreshed every 5 min by the
    # sampler) so the page paints with up-to-date numbers without doing N
    # network calls. Closed trades just echo their stored exit fields.
    enriched: list[dict] = []
    for r in rows:
        entry = float(r.entry_price or 0)
        qty = float(r.qty or 0)
        sign = 1.0 if (r.side or "").lower() == "long" else -1.0
        venue = venue_for(r.symbol)
        cur_price = None
        unrealized = None
        pct = None
        cur_ts = None
        if r.exit_price is None and entry > 0 and qty > 0:
            sym = await redis.hgetall(f"symbol:latest:{r.symbol}")
            if sym.get("price"):
                try:
                    cur_price = float(sym["price"])
                    cur_ts = sym.get("ts")
                    unrealized = sign * (cur_price - entry) * qty
                    pct = sign * (cur_price - entry) / entry * 100.0
                except Exception:  # noqa: BLE001
                    pass
            else:
                # Fall back to per-trade latest sample.
                lt = await redis.hgetall(f"trade:latest:{r.trade_ulid}")
                if lt.get("price"):
                    try:
                        cur_price = float(lt["price"])
                        cur_ts = lt.get("ts")
                        unrealized = sign * (cur_price - entry) * qty
                        pct = sign * (cur_price - entry) / entry * 100.0
                    except Exception:  # noqa: BLE001
                        pass
        # Realized pct for closed trades — same formula but exit_price.
        realized_pct = None
        if r.exit_price is not None and entry > 0:
            try:
                realized_pct = sign * (float(r.exit_price) - entry) / entry * 100.0
            except Exception:  # noqa: BLE001
                pass
        # Holding duration (for the Held column).
        from datetime import datetime as _dt, timezone as _tz
        held_sec: int | None = None
        if r.opened_at:
            end = r.closed_at or _dt.now(_tz.utc)
            try:
                held_sec = max(0, int((end - r.opened_at).total_seconds()))
            except Exception:  # noqa: BLE001
                pass
        # Status: open / closed-with-reason. Title + colour combo.
        STATUS_META = {
            "sl":          {"label": "SL hit",      "icon": "🛑", "color": "bg-red-100 text-red-800"},
            "tp":          {"label": "TP hit",      "icon": "🎯", "color": "bg-emerald-100 text-emerald-800"},
            "manual":      {"label": "Closed by you","icon": "✋", "color": "bg-blue-100 text-blue-800"},
            "timeout":     {"label": "Timed out",   "icon": "⏱",  "color": "bg-amber-100 text-amber-800"},
            "kill_switch": {"label": "Kill switch", "icon": "⚠",  "color": "bg-red-200 text-red-900"},
        }
        if r.exit_price is None:
            status = {"label": "Open", "icon": "●", "color": "bg-emerald-100 text-emerald-800 animate-pulse",
                      "tooltip": "Position still open — live mark-to-market shown."}
        else:
            cr = (str(r.close_reason) if r.close_reason else "").lower() or "—"
            meta = STATUS_META.get(cr, {"label": cr, "icon": "✓", "color": "bg-gray-200 text-gray-700"})
            status = {**meta, "tooltip": f"Closed automatically by rule: {cr}" if cr in ("sl", "tp", "timeout", "kill_switch") else ("Closed manually" if cr == "manual" else "Closed")}

        prop = prop_by_ulid.get(r.proposal_id or "")
        # Cheap health lookup from position monitor (None when closed/untracked).
        health: dict = {}
        try:
            if r.exit_price is None:
                health = await redis.hgetall(f"position:health:{r.trade_ulid}") or {}
        except Exception:  # noqa: BLE001
            pass
        enriched.append({
            "row": r, "venue": venue,
            "cur_price": cur_price, "cur_ts": cur_ts,
            "unrealized": unrealized, "pct": pct,
            "realized_pct": realized_pct,
            "held_sec": held_sec,
            "status": status,
            "conviction": float(prop.conviction) if (prop and prop.conviction is not None) else None,
            "reasoning_preview": (prop.reasoning[:280] if (prop and prop.reasoning) else None),
            "event_ulid": (prop.triggering_event_ulid if prop else None),
            "health": health,
        })
    return templates.TemplateResponse(
        request, "pages/trades.html", {"trades": enriched, "active": "trades"}
    )


async def _find_proposal_in_stream(proposal_ulid: str) -> dict[str, Any] | None:
    """Best-effort scan of trading_proposals:stream for a payload with matching ULID."""
    redis = get_redis()
    try:
        entries = await redis.xrevrange(Streams.TRADING_PROPOSALS, count=2000)
    except Exception:  # noqa: BLE001
        return None
    for _id, fields in entries:
        raw = fields.get("data")
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:  # noqa: BLE001
            continue
        if payload.get("ulid") == proposal_ulid:
            return payload
    return None


async def _find_audit(target_ulid: str) -> list[AuditLog]:
    async with session_scope() as session:
        rows = (await session.execute(
            select(AuditLog)
            .where(AuditLog.target == target_ulid)
            .order_by(desc(AuditLog.ts))
        )).scalars().all()
    return rows


@router.get("/watch", response_class=HTMLResponse)
async def watch_list(request: Request):
    """Per-symbol price watch list — one row per distinct symbol that has an
    open position, refreshed every 5 min by the trade_sampler loop, regardless
    of each trade's milestone cadence."""
    from datetime import datetime as _dt, timezone as _tz
    from plata.execution.router import venue_for
    redis = get_redis()
    async with session_scope() as session:
        rows = (await session.execute(
            select(TradeLedger).where(TradeLedger.exit_price.is_(None))
        )).scalars().all()
    by_symbol: dict[str, dict[str, Any]] = {}
    for t in rows:
        s = by_symbol.setdefault(t.symbol, {
            "symbol": t.symbol, "venue": venue_for(t.symbol),
            "trades": [], "net_qty_long": 0.0, "net_qty_short": 0.0,
        })
        s["trades"].append({
            "ulid": t.trade_ulid, "side": t.side, "qty": float(t.qty or 0),
            "entry": float(t.entry_price or 0), "opened_at": t.opened_at,
        })
        if (t.side or "").lower() == "long":
            s["net_qty_long"] += float(t.qty or 0)
        else:
            s["net_qty_short"] += float(t.qty or 0)
    for s in by_symbol.values():
        latest = await redis.hgetall(f"symbol:latest:{s['symbol']}")
        price = float(latest.get("price") or 0) or None
        ts_iso = latest.get("ts")
        age_sec = None
        if ts_iso:
            try:
                age_sec = (_dt.now(_tz.utc) - _dt.fromisoformat(ts_iso)).total_seconds()
            except Exception:  # noqa: BLE001
                pass
        unrealized = 0.0
        for tr in s["trades"]:
            if price and tr["entry"]:
                sign = 1.0 if (tr["side"] or "").lower() == "long" else -1.0
                unrealized += sign * (price - tr["entry"]) * tr["qty"]
        s["price"] = price
        s["ts"] = ts_iso
        s["age_sec"] = int(age_sec) if age_sec is not None else None
        s["unrealized"] = round(unrealized, 2)
    items = sorted(by_symbol.values(), key=lambda x: -abs(x["unrealized"]))
    return templates.TemplateResponse(
        request, "pages/positions_watch.html",
        {"active": "trades", "items": items, "cadence_sec": 5 * 60},
    )


@router.post("/{trade_ulid}/close")
async def manual_close(trade_ulid: str, request: Request):
    """Close an open position at the current market price (paper or live).
    Synthesizes a TradeClosure event so the reviewer pipeline picks it up
    (same path SL/TP/timeout closures take)."""
    from datetime import datetime, timezone
    from decimal import Decimal
    from fastapi.responses import JSONResponse
    from plata.core.bus import Streams, publish, publish_channel
    from plata.core.schemas import CloseReason, Side, TradeClosure, TradeMode
    from plata.execution.router import client_for, venue_for

    async with session_scope() as session:
        trade = (await session.execute(
            select(TradeLedger).where(TradeLedger.trade_ulid == trade_ulid)
        )).scalar_one_or_none()
    if not trade or trade.exit_price is not None:
        return JSONResponse({"ok": False, "reason": "not_open"}, status_code=400)

    venue = venue_for(trade.symbol)
    exit_price = Decimal("0")
    try:
        c = client_for(agent="manual_close", venue=venue)
        t = await c.fetch_ticker(trade.symbol)
        lp = float(t.get("last") or t.get("close") or 0)
        if lp > 0:
            exit_price = Decimal(str(lp))
    except Exception:  # noqa: BLE001
        pass
    if exit_price <= 0:
        # Fall back to last sampled price.
        redis = get_redis()
        sym_latest = await redis.hgetall(f"symbol:latest:{trade.symbol}")
        if sym_latest.get("price"):
            exit_price = Decimal(str(sym_latest["price"]))
    if exit_price <= 0:
        return JSONResponse({"ok": False, "reason": "no_price_available"}, status_code=502)

    sign = Decimal("1") if (trade.side or "").lower() == "long" else Decimal("-1")
    qty = Decimal(str(trade.qty or 0))
    entry = Decimal(str(trade.entry_price or 0))
    gross = sign * (exit_price - entry) * qty
    fees = Decimal(str(trade.fees or 0))
    net = gross - fees

    closure = TradeClosure(
        trade_ulid=trade_ulid, proposal_ulid=trade.proposal_id or "",
        symbol=trade.symbol, venue=venue,
        mode=TradeMode.PAPER, side=Side(trade.side),
        qty=qty, entry_price=entry, exit_price=exit_price,
        fees=fees, gross_pnl=gross, net_pnl=net,
        close_reason=CloseReason.MANUAL,
        opened_at=trade.opened_at, closed_at=datetime.now(timezone.utc),
    )
    await publish(Streams.TRADE_CLOSURES, closure)
    await publish_channel("dashboard:events", {
        "kind": "trade_closed", "trade_ulid": trade_ulid,
        "symbol": trade.symbol, "net_pnl": float(net),
        "close_reason": "manual",
    })
    return JSONResponse({"ok": True, "exit_price": str(exit_price), "net_pnl": str(net)})


@router.post("/{trade_ulid}/sl_tp")
async def manual_sl_tp(trade_ulid: str, request: Request):
    """Update the stored SL / TP prices on an open trade. The reviewer reads
    these on every sample; updating here adjusts the auto-exit triggers
    without closing the position."""
    from decimal import Decimal, InvalidOperation
    from fastapi.responses import JSONResponse
    form = await request.form()
    def _dec(v: str | None) -> Decimal | None:
        if not v: return None
        try: return Decimal(v.strip())
        except (InvalidOperation, ValueError): return None
    sl = _dec(form.get("sl_price"))
    tp = _dec(form.get("tp_price"))
    async with session_scope() as session:
        trade = (await session.execute(
            select(TradeLedger).where(TradeLedger.trade_ulid == trade_ulid)
        )).scalar_one_or_none()
        if not trade or trade.exit_price is not None:
            return JSONResponse({"ok": False, "reason": "not_open"}, status_code=400)
        if sl is not None: trade.sl_price = sl
        if tp is not None: trade.tp_price = tp
    return RedirectResponse(url=f"/trades/{trade_ulid}", status_code=303)


@router.post("/{trade_ulid}/auto_rules")
async def manual_auto_rules(trade_ulid: str, request: Request, clear: int = 0):
    """Save deterministic auto-close rules for an open position. Position
    monitor reads them every minute. Empty fields disable the rule.
    Pass ?clear=1 to wipe all rules at once."""
    from datetime import datetime, timezone
    from fastapi.responses import JSONResponse
    form = await request.form()
    rules: dict = {}
    if not clear:
        # Numeric fields — store as floats; skip blank/invalid.
        for k in ("max_loss_usd", "max_loss_pct", "trailing_peak_pct",
                  "close_after_days", "rolling_loss_pct", "rolling_loss_days"):
            v = (form.get(k) or "").strip()
            if not v:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f <= 0:
                continue
            rules[k] = f
    async with session_scope() as session:
        trade = (await session.execute(
            select(TradeLedger).where(TradeLedger.trade_ulid == trade_ulid)
        )).scalar_one_or_none()
        if not trade or trade.exit_price is not None:
            return JSONResponse({"ok": False, "reason": "not_open"}, status_code=400)
        raw = dict(trade.raw_bybit_response or {})
        if rules:
            raw["auto_close_rules"] = rules
            # Capture the rule-setting moment so we can compute "rolling N-day"
            # deltas without rescanning every sample.
            raw["auto_close_rules_set_at"] = datetime.now(timezone.utc).isoformat()
        else:
            raw.pop("auto_close_rules", None)
            raw.pop("auto_close_rules_set_at", None)
        trade.raw_bybit_response = raw
    return RedirectResponse(url=f"/trades/{trade_ulid}", status_code=303)


@router.post("/{trade_ulid}/note")
async def manual_note(trade_ulid: str, request: Request):
    """Append a free-text note to the trade's raw_bybit_response JSONB under
    a `notes` list. Visible on the trade detail page audit section."""
    from datetime import datetime, timezone
    from fastapi.responses import JSONResponse
    from plata.dashboard.auth import current_user_email
    form = await request.form()
    note = (form.get("note") or "").strip()[:1000]
    if not note:
        return RedirectResponse(url=f"/trades/{trade_ulid}", status_code=303)
    async with session_scope() as session:
        trade = (await session.execute(
            select(TradeLedger).where(TradeLedger.trade_ulid == trade_ulid)
        )).scalar_one_or_none()
        if not trade:
            return JSONResponse({"ok": False, "reason": "not_found"}, status_code=404)
        raw = dict(trade.raw_bybit_response or {})
        notes = list(raw.get("notes") or [])
        notes.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "by": current_user_email(request) or "manual",
            "text": note,
        })
        raw["notes"] = notes
        trade.raw_bybit_response = raw
    return RedirectResponse(url=f"/trades/{trade_ulid}", status_code=303)


@router.get("/{trade_ulid}/samples")
async def samples(trade_ulid: str):
    """Return recorded price samples + a diagnostic block explaining
    *why* the sampler may not have written any (helps when the
    actual-price line on the trade chart stays empty)."""
    from datetime import datetime, timezone
    from fastapi.responses import JSONResponse

    redis = get_redis()
    raw = await redis.lrange(f"trade:samples:{trade_ulid}", 0, -1)
    out = []
    for s in raw:
        try:
            out.append(json.loads(s))
        except Exception:  # noqa: BLE001
            pass

    diag: dict = {}
    try:
        async with session_scope() as session:
            trade = (await session.execute(
                select(TradeLedger).where(TradeLedger.trade_ulid == trade_ulid)
            )).scalar_one_or_none()
        if trade is None:
            diag["trade"] = "not_found"
        else:
            from plata.execution.router import venue_for
            venue = venue_for(trade.symbol)
            diag["symbol"] = trade.symbol
            diag["venue"] = venue
            diag["side"] = trade.side
            diag["entry_price"] = float(trade.entry_price or 0)
            diag["exit_price"] = float(trade.exit_price) if trade.exit_price is not None else None
            diag["closed"] = trade.exit_price is not None
            diag["opened_at"] = trade.opened_at.isoformat() if trade.opened_at else None

            # Sampler heartbeat (set inside run_sampler_loop every tick)
            hb = await redis.hgetall("agent_status:trade_sampler")
            if hb:
                last = hb.get("last_heartbeat")
                age_sec = None
                if last:
                    try:
                        age_sec = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
                    except Exception:  # noqa: BLE001
                        pass
                diag["sampler_heartbeat"] = {
                    "last": last,
                    "age_sec": int(age_sec) if age_sec is not None else None,
                    "in_flight": hb.get("in_flight"),
                    "halted": hb.get("halted"),
                    "alive": (age_sec is not None and age_sec < 30),
                }
            else:
                diag["sampler_heartbeat"] = None  # sampler has never run

            # Cadence the loop *would* use for this trade
            try:
                from plata.execution.trade_sampler import _cadence_seconds, _longest_milestone_eta
                longest = await _longest_milestone_eta(trade.proposal_id)
                diag["longest_milestone_eta_minutes"] = longest
                diag["cadence_sec"] = _cadence_seconds(longest)
            except Exception as exc:  # noqa: BLE001
                diag["cadence_lookup_error"] = str(exc)[:160]

            # Live one-shot price probe — same path the sampler takes
            try:
                from plata.execution.trade_sampler import _latest_price
                price = await _latest_price(trade.symbol)
                diag["probe_price"] = price
                if price is None:
                    if venue == "alpaca":
                        diag["probe_hint"] = "Alpaca returned None — Alpaca key/secret may be missing or symbol is unsupported."
                    else:
                        diag["probe_hint"] = "Bybit OHLCV returned None — Bybit credentials may be missing or symbol is delisted."
            except Exception as exc:  # noqa: BLE001
                diag["probe_error"] = str(exc)[:200]
    except Exception as exc:  # noqa: BLE001
        diag["diag_error"] = str(exc)[:200]

    return JSONResponse({"count": len(out), "samples": out, "diag": diag})


@router.get("/{trade_ulid}", response_class=HTMLResponse)
async def detail(request: Request, trade_ulid: str):
    async with session_scope() as session:
        trade = (await session.execute(
            select(TradeLedger).where(TradeLedger.trade_ulid == trade_ulid)
        )).scalar_one_or_none()
    proposal = None
    event_doc = None
    audits: list[AuditLog] = []
    if trade and trade.proposal_id:
        proposal = await _find_proposal_in_stream(trade.proposal_id)
        audits = await _find_audit(trade.proposal_id)
        if proposal:
            triggering = proposal.get("triggering_event_ulid")
            if triggering:
                redis = get_redis()
                event_doc = await redis.json().get(event_key(triggering))
                if isinstance(event_doc, dict):
                    event_doc.pop("embedding", None)
    # Live price + unrealized PnL for open positions, so the summary shows
    # "Current value" / "Unrealized PnL" instead of "—" placeholders.
    live: dict = {}
    if trade and trade.exit_price is None:
        try:
            redis = get_redis()
            sym = await redis.hgetall(f"symbol:latest:{trade.symbol}") or {}
            price = float(sym.get("price") or 0) or None
            if price is None:
                tl = await redis.hgetall(f"trade:latest:{trade.trade_ulid}") or {}
                if tl.get("price"):
                    price = float(tl["price"])
            entry = float(trade.entry_price or 0)
            qty = float(trade.qty or 0)
            if price and entry > 0 and qty > 0:
                sign = 1.0 if (trade.side or "").lower() == "long" else -1.0
                live = {
                    "price": price,
                    "ts": sym.get("ts"),
                    "notional_now": round(qty * price, 4),
                    "unrealized_pnl": round(sign * (price - entry) * qty, 4),
                    "pnl_pct": round(sign * (price - entry) / entry * 100.0, 3),
                    "price_pct": round((price - entry) / entry * 100.0, 3),
                }
        except Exception:  # noqa: BLE001
            pass
    # Position monitor health snapshot, if any
    health: dict = {}
    suggested_adjustment_ulid: str | None = None
    if trade:
        try:
            health = await get_redis().hgetall(f"position:health:{trade.trade_ulid}") or {}
        except Exception:  # noqa: BLE001
            pass
        try:
            from plata.core.db import Proposal
            async with session_scope() as session:
                row = (await session.execute(
                    select(Proposal)
                    .where(Proposal.state == "adjustment_suggested")
                    .where(Proposal.extras["adjustment_target_trade_ulid"].astext == trade.trade_ulid)
                    .order_by(Proposal.created_at.desc())
                    .limit(1)
                )).scalar_one_or_none()
                if row:
                    suggested_adjustment_ulid = row.proposal_ulid
        except Exception:  # noqa: BLE001
            pass
    return templates.TemplateResponse(
        request,
        "pages/trade_detail.html",
        {
            "active": "trades",
            "trade": trade,
            "proposal": proposal,
            "event_doc": event_doc,
            "audits": audits,
            "health": health,
            "suggested_adjustment_ulid": suggested_adjustment_ulid,
            "live": live,
        },
    )
