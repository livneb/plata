"""Error log viewer."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select

from plata.core.db import ErrorLog, session_scope
from plata.dashboard.app import BASE_DIR

router = APIRouter(prefix="/errors", tags=["errors"])
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    async with session_scope() as session:
        result = await session.execute(
            select(ErrorLog).order_by(desc(ErrorLog.ts)).limit(200)
        )
        rows = result.scalars().all()
    return templates.TemplateResponse(
        "pages/errors.html", {"request": request, "errors": rows, "active": "errors"}
    )
