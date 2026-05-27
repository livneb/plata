"""Proposals — full lifecycle list (published / pending HITL / rejected /
approved / executed / dropped) with expandable detail and a
clone-and-edit form that re-submits a manual-override proposal to the
executor."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from plata.core.proposals import get as proposal_get, list_recent
from plata.dashboard import templates
from plata.hitl.approval_store import list_pending, resolve

router = APIRouter(prefix="/proposals", tags=["proposals"])


STATE_META: dict[str, dict[str, str]] = {
    "dropped":          {"label": "Dropped",       "color": "bg-gray-200 text-gray-700",     "icon": "🚫"},
    "published":        {"label": "Published",     "color": "bg-blue-100 text-blue-800",     "icon": "📨"},
    "rejected":         {"label": "Rejected",      "color": "bg-red-100 text-red-800",       "icon": "🛡️"},
    "pending_hitl":     {"label": "Pending HITL",  "color": "bg-amber-100 text-amber-800",   "icon": "⏳"},
    "hitl_approved":    {"label": "HITL approved", "color": "bg-emerald-100 text-emerald-800","icon": "👤"},
    "hitl_rejected":    {"label": "HITL rejected", "color": "bg-red-100 text-red-800",       "icon": "👤"},
    "hitl_timeout":     {"label": "HITL timeout",  "color": "bg-gray-200 text-gray-700",     "icon": "⌛"},
    "approved":         {"label": "Approved",      "color": "bg-emerald-100 text-emerald-800","icon": "✅"},
    "executed":         {"label": "Executed",      "color": "bg-green-100 text-green-800",   "icon": "📈"},
    "failed_execution": {"label": "Exec failed",   "color": "bg-red-100 text-red-800",       "icon": "💥"},
    "manual_override":  {"label": "Manual",        "color": "bg-purple-100 text-purple-800", "icon": "✋"},
}


def _row(p) -> dict[str, Any]:
    meta = STATE_META.get(p.state, {"label": p.state, "color": "bg-gray-200 text-gray-700", "icon": "?"})
    return {
        "ulid": p.proposal_ulid,
        "symbol": p.symbol,
        "side": p.side,
        "conviction": float(p.conviction) if p.conviction is not None else None,
        "state": p.state,
        "state_label": meta["label"],
        "state_color": meta["color"],
        "state_icon": meta["icon"],
        "state_reason": p.state_reason,
        "trade_ulid": p.trade_ulid,
        "event_ulid": p.triggering_event_ulid,
        "reasoning": p.reasoning or "",
        "milestones": p.milestones or [],
        "analogs": p.analogs or [],
        "extras": p.extras or {},
        "last_actor": p.last_actor,
        "created_at": p.created_at,
        "updated_at": p.updated_at,
        "sl_pct": float(p.suggested_sl_pct) if p.suggested_sl_pct is not None else None,
        "tp_pct": float(p.suggested_tp_pct) if p.suggested_tp_pct is not None else None,
    }


DROP_REASON_META: dict[str, dict[str, str]] = {
    "below_threshold":      {"label": "Below magnitude threshold", "icon": "📉",
                              "hint": "sentiment_magnitude < 0.5 — event wasn't dramatic enough to consider"},
    "event_missing_in_graph": {"label": "Event missing in graph",   "icon": "❓",
                              "hint": "EnrichedEvent passed magnitude gate but its document is no longer in Redis JSON"},
    "no_embedding":         {"label": "No embedding",                "icon": "🧬",
                              "hint": "Voyage didn't return a vector — probably rate-limited or budget capped"},
    "llm_no_trade":         {"label": "LLM said don't trade",        "icon": "🤔",
                              "hint": "Strategist LLM looked at the analogs and decided conviction < 0.6 OR no analog moved the same direction"},
}


async def _strategist_pipeline_stats() -> dict[str, Any]:
    """Snapshot of what the strategist has seen vs done — surfaces upstream
    pipeline issues (no events arriving, enricher dead, etc.) and any
    persistence failure so the user sees the real cause inline."""
    from plata.core.bus import get_redis
    redis = get_redis()
    out: dict[str, Any] = {}
    try:
        stats = await redis.hgetall("agent_stats:strategist")
        out["processed_total"] = int(stats.get("processed_total") or 0)
        out["dropped_below_threshold"] = int(stats.get("dropped_below_threshold") or 0)
        out["dropped_missing_event"] = int(stats.get("dropped_missing_event") or 0)
        out["dropped_no_embedding"] = int(stats.get("dropped_no_embedding") or 0)
    except Exception:  # noqa: BLE001
        pass
    try:
        hb = await redis.hgetall("agent_status:strategist")
        out["last_heartbeat"] = hb.get("last_heartbeat")
        out["halted"] = (hb.get("halted") or "").lower() == "true"
    except Exception:  # noqa: BLE001
        pass
    # Persistence error — if record_drop / record_published kept failing.
    try:
        err = await redis.hgetall("proposals:last_persist_error")
        if err:
            out["persist_error"] = err
            out["persist_failures_total"] = int(
                (await redis.get("proposals:persist_failures_total")) or 0
            )
    except Exception:  # noqa: BLE001
        pass
    return out


@router.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    state: str | None = None,
    symbol: str | None = None,
    reason: str | None = None,
    page: int = 1,
    per_page: int = 25,
):
    # Clamp params so a bad URL doesn't crash the route.
    page = max(1, int(page or 1))
    per_page = max(10, min(int(per_page or 25), 200))

    # Reason filter is applied AFTER fetch (it lives in extras JSON, not a column),
    # so when one is active we have to fetch more aggressively to keep the page
    # full. With state=dropped + reason filter most rows match, so this is fine.
    fetch_limit = per_page if not reason else max(per_page * 10, 500)
    fetch_offset = (page - 1) * per_page if not reason else 0

    from plata.core.proposals import count_recent
    total = await count_recent(state=state, symbol=symbol)
    rows_db = await list_recent(
        state=state, symbol=symbol, limit=fetch_limit, offset=fetch_offset,
    )
    rows = [_row(r) for r in rows_db]
    if reason:
        rows = [r for r in rows if (r["extras"] or {}).get("drop_reason_code") == reason]
        # Now apply page window client-side on the filtered set.
        total = len(rows) + fetch_offset  # best-effort; reason filter has no native count
        rows = rows[(page - 1) * per_page : page * per_page]
    # Enrich each row with the triggering event's headline / summary / sentiment
    # so the user sees WHY the proposal existed in the first place (not just what
    # the LLM said about it). Falls back gracefully when the event TTL has expired.
    try:
        from plata.core.graph import get_event
        for r in rows:
            eu = r.get("event_ulid")
            if not eu:
                continue
            doc = await get_event(eu)
            if not doc:
                continue
            r["event"] = {
                "title": doc.get("title") or "",
                "summary": doc.get("summary") or "",
                "category": doc.get("category") or "",
                "source": doc.get("source") or "",
                "ts": doc.get("ts") or "",
                "sentiment": doc.get("sentiment"),
                "sentiment_magnitude": doc.get("sentiment_magnitude"),
                "url": doc.get("url") or "",
            }
    except Exception:  # noqa: BLE001
        pass
    # Counts per state + per drop-reason for the filter chips.
    counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    for r in (await list_recent(limit=500)):
        counts[r.state] = counts.get(r.state, 0) + 1
        if r.state == "dropped":
            code = (r.extras or {}).get("drop_reason_code") or "unknown"
            reason_counts[code] = reason_counts.get(code, 0) + 1
    legacy_pending = await list_pending()
    pipeline = await _strategist_pipeline_stats()
    import math
    pages = max(1, math.ceil(total / per_page)) if per_page else 1
    return templates.TemplateResponse(
        request,
        "pages/proposals.html",
        {
            "active": "proposals",
            "rows": rows,
            "counts": counts,
            "reason_counts": reason_counts,
            "state_meta": STATE_META,
            "reason_meta": DROP_REASON_META,
            "active_state": state,
            "active_symbol": symbol,
            "active_reason": reason,
            "legacy_pending": legacy_pending,
            "pipeline": pipeline,
            "page": page,
            "per_page": per_page,
            "pages": pages,
            "total": total,
        },
    )


@router.get("/{proposal_ulid}/json")
async def proposal_json(proposal_ulid: str):
    p = await proposal_get(proposal_ulid)
    if not p:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse(_row(p))


@router.post("/{proposal_ulid}/decide")
async def decide(proposal_ulid: str, action: str = Form(...)):
    """HITL approve / reject (back-compat with the original endpoint)."""
    await resolve(
        proposal_ulid, approved=(action == "approve"), actor="dashboard"
    )
    return RedirectResponse(url="/proposals/", status_code=303)


@router.post("/{proposal_ulid}/resubmit")
async def resubmit(
    request: Request,
    proposal_ulid: str,
    symbol: str = Form(...),
    side: str = Form(...),
    conviction: float = Form(...),
    sl_pct: float | None = Form(None),
    tp_pct: float | None = Form(None),
    reasoning: str = Form(""),
    bypass_risk: bool = Form(False),
):
    """Clone-and-edit: take the values the user typed and emit a NEW proposal.
    If `bypass_risk` is set, the new proposal is published straight to the
    approved-trades stream with a manual-override flag (no risk gates applied).
    Otherwise it goes through the normal pipeline (risk → executor)."""
    from plata.core.bus import Streams, publish
    from plata.core.proposals import record_published, update_state
    from plata.core.schemas import Side, TradeProposal
    from plata.dashboard.auth import current_user_email

    src = await proposal_get(proposal_ulid)
    if not src:
        return JSONResponse({"error": "source_not_found"}, status_code=404)

    actor = current_user_email(request) or "manual"

    cloned = TradeProposal(
        triggering_event_ulid=src.triggering_event_ulid,
        symbol=symbol.upper().strip(),
        side=Side(side.lower().strip()),
        conviction=max(0.0, min(1.0, float(conviction))),
        reasoning=(reasoning or src.reasoning or "(manual override)")[:1500],
        similar_events=[],
        milestones=[],
        suggested_sl_pct=sl_pct if sl_pct is not None else None,
        suggested_tp_pct=tp_pct if tp_pct is not None else None,
    )

    if bypass_risk:
        # Skip risk_manager — push straight to approved_trades for the executor.
        # Two things the previous version got wrong:
        #   1. executor._load_proposal does an XRANGE on trading_proposals:stream
        #      to find symbol/side; we have to publish there too, not just
        #      approved_trades.
        #   2. final_qty=0 silently produced zero-quantity orders (or paper
        #      ledger rows with 0 qty). Compute a real qty from a $100 default
        #      notional ÷ current ticker price.
        from decimal import Decimal
        from plata.core.schemas import RiskDecision
        from plata.execution.router import client_for, venue_for

        default_notional = Decimal("100")
        venue = venue_for(cloned.symbol)
        # Best-effort live price for sizing. Falls back to a per-venue stub if
        # neither venue is configured.
        last_price: Decimal = Decimal("0")
        try:
            client = client_for(agent="manual_override", venue=venue)
            t = await client.fetch_ticker(cloned.symbol)
            lp = float(t.get("last") or t.get("close") or 0)
            if lp > 0:
                last_price = Decimal(str(lp))
        except Exception:  # noqa: BLE001
            pass
        if last_price <= 0:
            last_price = Decimal("100") if venue == "alpaca" else Decimal("50000")
        qty = (default_notional / last_price).quantize(Decimal("0.0001"))
        if qty <= 0:
            qty = Decimal("0.0001")

        decision = RiskDecision(
            proposal_ulid=cloned.ulid, approved=True, requires_hitl=False,
            final_qty=qty, final_notional_usd=default_notional,
            final_sl_price=None, final_tp_price=None,
            risk_snapshot={"manual_override": True, "actor": actor,
                            "default_notional_usd": str(default_notional),
                            "price_at_manual": str(last_price)},
        )
        # IMPORTANT: publish to trading_proposals first so executor's
        # _load_proposal can find symbol/side via XRANGE.
        await publish(Streams.TRADING_PROPOSALS, cloned)
        await record_published(cloned)
        await update_state(cloned.ulid, state="manual_override",
                            reason=f"cloned from {proposal_ulid} · ${default_notional} @ ~${last_price}",
                            actor=f"user:{actor}",
                            extras={
                                "source_proposal_ulid": proposal_ulid,
                                "qty": str(qty),
                                "notional_usd": str(default_notional),
                                "price_at_manual": str(last_price),
                            })
        await _append_child(proposal_ulid, cloned.ulid, "manual_override", actor)
        await publish(Streams.APPROVED_TRADES, decision)
    else:
        await publish(Streams.TRADING_PROPOSALS, cloned)
        await record_published(cloned)
        await update_state(cloned.ulid, state="published",
                            reason=f"manual re-submit of {proposal_ulid}",
                            actor=f"user:{actor}",
                            extras={"source_proposal_ulid": proposal_ulid})
        await _append_child(proposal_ulid, cloned.ulid, "published", actor)

    return RedirectResponse(url=f"/proposals/?symbol={cloned.symbol}", status_code=303)


async def _append_child(parent_ulid: str, child_ulid: str, child_state: str, actor: str) -> None:
    """Append a child re-submission entry to the parent proposal's extras.children list.
    Keeps the audit chain visible from the parent's side too (one-way → two-way)."""
    try:
        from datetime import datetime, timezone
        from plata.core.proposals import get as proposal_get, update_state
        parent = await proposal_get(parent_ulid)
        if not parent:
            return
        children = list((parent.extras or {}).get("children") or [])
        children.append({
            "ulid": child_ulid,
            "state": child_state,
            "actor": actor,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        # Idempotent: update_state will merge with existing extras.
        await update_state(parent_ulid, state=parent.state, extras={"children": children})
    except Exception:  # noqa: BLE001
        pass
