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
    from datetime import timedelta
    redis = get_redis()
    today = date.today()
    yesterday = today - timedelta(days=1)
    last_7 = [(today - timedelta(days=i)).isoformat() for i in range(0, 7)]
    last_30 = [(today - timedelta(days=i)).isoformat() for i in range(0, 30)]

    async def _sum(keys: list[str]) -> float:
        if not keys:
            return 0.0
        vals = await redis.mget(*keys)
        total = 0.0
        for v in vals:
            try:
                total += float(v or 0.0)
            except (TypeError, ValueError):
                pass
        return total

    keys = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        keys.append(k)
    agents_data = []
    for k in keys:
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        data["name"] = name
        async def _per_agent(days: list[str]) -> float:
            return await _sum([f"cost:daily:{d}:agent:{name}" for d in days])
        data["spend_today_usd"]      = await _per_agent([today.isoformat()])
        data["spend_yesterday_usd"]  = await _per_agent([yesterday.isoformat()])
        data["spend_7d_usd"]         = await _per_agent(last_7)
        data["spend_30d_usd"]        = await _per_agent(last_30)
        # All-time per-agent: SCAN cost:daily:*:agent:<name> (cheap, agents stay few).
        atvals: list[str] = []
        async for ck in redis.scan_iter(match=f"cost:daily:*:agent:{name}", count=200):
            atvals.append(ck)
        data["spend_all_usd"] = await _sum(atvals)
        agents_data.append(data)

    daily_total      = await _sum([f"cost:daily:{today.isoformat()}"])
    yesterday_total  = await _sum([f"cost:daily:{yesterday.isoformat()}"])
    last_7_total     = await _sum([f"cost:daily:{d}" for d in last_7])
    last_30_total    = await _sum([f"cost:daily:{d}" for d in last_30])
    all_keys: list[str] = []
    async for ck in redis.scan_iter(match="cost:daily:*", count=500):
        # filter out per-agent rows for the global all-time sum
        if ":agent:" not in ck:
            all_keys.append(ck)
    all_time_total = await _sum(all_keys)

    return templates.TemplateResponse(
        request,
        "pages/agents.html",
        {
            "agents": agents_data,
            "active": "agents",
            "daily_total_usd": daily_total,
            "yesterday_total_usd": yesterday_total,
            "last_7_total_usd": last_7_total,
            "last_30_total_usd": last_30_total,
            "all_time_total_usd": all_time_total,
        },
    )
