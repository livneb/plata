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

    # Union of agents we know about:
    #   • currently-running (have an `agent_status:<name>` heartbeat hash)
    #   • have ever spent money (have any `cost:daily:*:agent:<name>` key)
    # Without the cost-scan, agents that crashed or were renamed disappear
    # from the grid but their historical spend stays in the daily totals,
    # which is why "sum of visible agents < total" used to happen.
    agent_names: set[str] = set()
    status_by_name: dict[str, dict] = {}
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        name = k.split(":")[-1]
        agent_names.add(name)
        status_by_name[name] = await redis.hgetall(k)
    async for ck in redis.scan_iter(match="cost:daily:*:agent:*", count=500):
        # cost:daily:<YYYY-MM-DD>:agent:<name>
        try:
            agent_names.add(ck.rsplit(":", 1)[-1])
        except Exception:  # noqa: BLE001
            pass

    agents_data = []
    for name in sorted(agent_names):
        data = dict(status_by_name.get(name) or {})
        data["name"] = name
        # If we only know this agent from cost keys, mark it as stopped so the
        # UI can render it greyed out instead of falsely "RUNNING".
        if not status_by_name.get(name):
            data["last_heartbeat"] = None
            data["halted"] = "stopped"
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
