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
    async with session_scope() as session:
        result = await session.execute(
            select(TradeLedger).order_by(TradeLedger.opened_at.desc()).limit(100)
        )
        rows = result.scalars().all()
    return templates.TemplateResponse(
        request, "pages/trades.html", {"trades": rows, "active": "trades"}
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
    return templates.TemplateResponse(
        request,
        "pages/trade_detail.html",
        {
            "active": "trades",
            "trade": trade,
            "proposal": proposal,
            "event_doc": event_doc,
            "audits": audits,
        },
    )
