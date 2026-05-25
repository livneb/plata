"""Historian seed controller — bootstrap the graph with N dramatic historical events.

Generates events via LLM, embeds them, then attaches REAL OHLCV-derived price impact
metrics by querying the Bybit oracle for the affected symbols around the event date.
"""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/historian", tags=["historian"])
STATUS_KEY = "historian:status"


async def _status() -> dict[str, Any]:
    redis = get_redis()
    data = await redis.hgetall(STATUS_KEY)
    if not data:
        return {"state": "never_run"}
    return data


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    status = await _status()
    return templates.TemplateResponse(
        request, "pages/historian.html", {"active": "historian", "status": status}
    )


@router.get("/_fragment", response_class=HTMLResponse)
async def fragment(request: Request):
    status = await _status()
    return templates.TemplateResponse(
        request, "pages/_historian_fragment.html", {"status": status}
    )


@router.post("/start")
async def start(
    total: int = Form(100),
    batch_size: int = Form(10),
    start_year: int = Form(2005),
    end_year: int = Form(2025),
):
    total = max(10, min(int(total), 2000))
    batch_size = max(1, min(int(batch_size), 25))
    start_year = max(1990, min(int(start_year), 2100))
    end_year = max(1990, min(int(end_year), 2100))
    if start_year > end_year:
        start_year, end_year = end_year, start_year
    redis = get_redis()
    current = await redis.hget(STATUS_KEY, "state")
    if current == "running":
        return RedirectResponse(url="/historian/", status_code=303)
    from plata.agents.historian import seed
    asyncio.create_task(
        seed(total_events=total, batch_size=batch_size,
             start_year=start_year, end_year=end_year),
        name="historian-seed",
    )
    return RedirectResponse(url="/historian/", status_code=303)
