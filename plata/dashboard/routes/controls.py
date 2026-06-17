"""System-control endpoints — currently just the "start from scratch" reset.

Reset semantics:
  - Force-close every open trade at its last sampled price (paper book-close).
    Each gets a TradeClosure row with close_reason=reset so /money/ keeps
    the historical truth.
  - Wipe transient per-day state (horizon counters, tuning timer) so the
    new session starts clean.
  - Set the operator's new starting equity in risk_config.
  - Preserve everything that represents the system's accumulated knowledge:
    TradeLedger history, the Redis-graph events + embeddings, cost tracking,
    lifetime agent stats.

The route lives at /controls/reset; the modal that confirms it lives in
the Settings → Controls tab.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from plata.core.bus import Channels, Streams, get_redis, publish, publish_channel
from plata.core.db import TradeLedger, session_scope
from plata.core.observability import get_logger
from plata.core.schemas import CloseReason, Side, TradeClosure, TradeMode

router = APIRouter(prefix="/controls", tags=["controls"])
_log = get_logger("controls")


@router.post("/reset")
async def reset_system(request: Request):
    """Wipe open positions + per-day counters, set new equity. Requires the
    client to send confirm="RESET" so a stray POST can't trip it.

    Form fields:
      - confirm: must equal "RESET" (typed in the modal).
      - new_equity_usd: float, default 10000.
    """
    form = await request.form()
    confirm = (form.get("confirm") or "").strip()
    if confirm != "RESET":
        return JSONResponse(
            {"ok": False, "error": "confirm_required",
              "detail": "Type 'RESET' in the confirmation box to proceed."},
            status_code=400,
        )
    try:
        new_equity = float(form.get("new_equity_usd") or 10000)
    except (TypeError, ValueError):
        new_equity = 10000.0
    if new_equity <= 0:
        return JSONResponse(
            {"ok": False, "error": "bad_equity",
              "detail": "new_equity_usd must be > 0."},
            status_code=400,
        )

    redis = get_redis()
    closed_count = 0
    closures_to_publish: list[TradeClosure] = []

    # 1. Force-close every open trade at its last mark.
    async with session_scope() as session:
        open_rows = (await session.execute(
            select(TradeLedger).where(TradeLedger.exit_price.is_(None))
        )).scalars().all()
        for r in open_rows:
            sym = await redis.hgetall(f"symbol:latest:{r.symbol}") or {}
            try:
                mark = float(sym.get("price") or 0)
            except (TypeError, ValueError):
                mark = 0.0
            if mark <= 0:
                # No live mark — fall back to entry price so PnL is 0.
                mark = float(r.entry_price or 0)
            if mark <= 0:
                # Truly broken row; skip rather than crash the whole reset.
                continue
            sign = 1.0 if (r.side or "").lower() == "long" else -1.0
            qty = float(r.qty or 0)
            entry = float(r.entry_price or 0)
            fees = float(r.fees or 0)
            gross = sign * (mark - entry) * qty
            net = gross - fees
            now = datetime.now(timezone.utc)
            r.exit_price = Decimal(str(mark))
            r.closed_at = now
            r.close_reason = CloseReason.RESET.value
            r.net_pnl = Decimal(str(net))
            r.gross_pnl = Decimal(str(gross))
            closures_to_publish.append(TradeClosure(
                trade_ulid=r.trade_ulid,
                proposal_ulid=r.proposal_id or "",
                symbol=r.symbol,
                venue=r.venue,
                mode=TradeMode(r.mode) if r.mode else TradeMode.PAPER,
                side=Side(r.side),
                qty=Decimal(str(qty)),
                entry_price=Decimal(str(entry)),
                exit_price=Decimal(str(mark)),
                fees=Decimal(str(fees)),
                gross_pnl=Decimal(str(gross)),
                net_pnl=Decimal(str(net)),
                close_reason=CloseReason.RESET,
                opened_at=r.opened_at,
                closed_at=now,
            ))
            closed_count += 1
        # commit happens on session_scope exit

    # 2. Publish closures on the stream so reviewer + dashboard SSE update.
    for c in closures_to_publish:
        try:
            await publish(Streams.TRADE_CLOSURES, c)
        except Exception as exc:  # noqa: BLE001
            _log.warning("reset_publish_closure_failed",
                          trade=c.trade_ulid, error=str(exc)[:160])

    # 3. Wipe transient per-day state — leaves history intact.
    counter_keys_wiped = 0
    try:
        async for k in redis.scan_iter(match="horizon:count:*", count=200):
            await redis.delete(k); counter_keys_wiped += 1
    except Exception as exc:  # noqa: BLE001
        _log.warning("reset_horizon_wipe_failed", error=str(exc)[:160])
    try:
        await redis.delete("reviewer:closures_since_tune")
    except Exception:  # noqa: BLE001
        pass

    # 4. Update baseline equity in risk_config — the Money page + agents
    #    will pick this up on their next config reload.
    try:
        await redis.hset("risk_config", "account_baseline_equity_usd",
                          str(new_equity))
    except Exception as exc:  # noqa: BLE001
        _log.warning("reset_set_equity_failed", error=str(exc)[:160])

    # 5. Audit + sysop notify.
    try:
        await publish_channel(Channels.SYSTEM_RESET, {
            "closed_count": closed_count,
            "new_equity_usd": new_equity,
            "counter_keys_wiped": counter_keys_wiped,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:  # noqa: BLE001
        pass

    _log.warning("system_reset_complete", closed=closed_count,
                  new_equity=new_equity, counters_wiped=counter_keys_wiped)
    return JSONResponse({
        "ok": True,
        "closed_count": closed_count,
        "new_equity_usd": new_equity,
        "counter_keys_wiped": counter_keys_wiped,
    })
