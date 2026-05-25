"""Error log viewer."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import delete, desc, select

from plata.core.db import ErrorLog, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/errors", tags=["errors"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    async with session_scope() as session:
        result = await session.execute(
            select(ErrorLog).order_by(desc(ErrorLog.ts)).limit(200)
        )
        rows = result.scalars().all()
    return templates.TemplateResponse(
        request, "pages/errors.html", {"errors": rows, "active": "errors"}
    )


@router.post("/clear")
async def clear():
    async with session_scope() as session:
        await session.execute(delete(ErrorLog))
    return RedirectResponse(url="/errors/", status_code=303)
