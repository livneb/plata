"""Open/closed positions from Postgres ledger."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from plata.core.db import TradeLedger, session_scope
from plata.dashboard.app import BASE_DIR

router = APIRouter(prefix="/trades", tags=["trades"])
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    async with session_scope() as session:
        result = await session.execute(
            select(TradeLedger).order_by(TradeLedger.opened_at.desc()).limit(100)
        )
        rows = result.scalars().all()
    return templates.TemplateResponse(
        "pages/trades.html", {"request": request, "trades": rows, "active": "trades"}
    )
