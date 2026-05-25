"""Open/closed positions from Postgres ledger."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from plata.core.db import TradeLedger, session_scope
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
