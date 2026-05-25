"""Agent health page."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from inkcliq.core.bus import get_redis
from inkcliq.dashboard.app import BASE_DIR

router = APIRouter(prefix="/agents", tags=["agents"])
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


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
        "pages/agents.html",
        {"request": request, "agents": agents_data, "active": "agents"},
    )
