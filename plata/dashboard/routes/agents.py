"""Agent health page."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    keys = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        keys.append(k)
    agents_data = []
    for k in keys:
        data = await redis.hgetall(k)
        data["name"] = k.split(":")[-1]
        agents_data.append(data)
    return templates.TemplateResponse(
        request, "pages/agents.html", {"agents": agents_data, "active": "agents"}
    )
