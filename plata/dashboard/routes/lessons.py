"""Lessons library — what the Postmortem agent has learned.

Browse recent lessons, filter by severity / symbol. The strategist
KNN-retrieves the top-3 most-similar lessons on every new event, so this
page is the operator's window into the system's accumulated hindsight.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from plata.core.graph import list_recent_lessons
from plata.dashboard import templates

router = APIRouter(prefix="/lessons", tags=["lessons"])


SEVERITY_META = {
    "glaring":       {"icon": "🚨", "chip": "bg-red-100 text-red-800"},
    "concerning":    {"icon": "⚠",  "chip": "bg-amber-100 text-amber-800"},
    "informational": {"icon": "💡", "chip": "bg-blue-100 text-blue-800"},
}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, severity: str | None = None,
                symbol: str | None = None, limit: int = 100):
    lessons = await list_recent_lessons(limit=limit)
    if severity:
        lessons = [L for L in lessons if (L.get("severity") or "") == severity]
    if symbol:
        s = symbol.upper()
        lessons = [L for L in lessons if (L.get("symbol") or "").upper() == s]
    return templates.TemplateResponse(
        request,
        "pages/lessons.html",
        {
            "active": "lessons",
            "lessons": lessons,
            "severity_meta": SEVERITY_META,
            "severity_filter": severity or "",
            "symbol_filter": symbol or "",
            "total": len(lessons),
        },
    )
