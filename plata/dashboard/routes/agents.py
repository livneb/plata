"""Agent health page."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    today = date.today().isoformat()
    keys = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        keys.append(k)
    agents_data = []
    total_spend_today = 0.0
    for k in keys:
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        data["name"] = name
        # LLM spend (daily + monthly) per agent
        daily_raw = await redis.get(f"cost:daily:{today}:agent:{name}")
        try:
            daily = float(daily_raw or 0.0)
        except (TypeError, ValueError):
            daily = 0.0
        data["spend_today_usd"] = daily
        total_spend_today += daily
        agents_data.append(data)

    # Global totals
    daily_total_raw = await redis.get(f"cost:daily:{today}")
    try:
        daily_total = float(daily_total_raw or 0.0)
    except (TypeError, ValueError):
        daily_total = 0.0

    return templates.TemplateResponse(
        request,
        "pages/agents.html",
        {
            "agents": agents_data,
            "active": "agents",
            "daily_total_usd": daily_total,
            "per_agent_total_usd": total_spend_today,
        },
    )
