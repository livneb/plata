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
