"""Agent health page."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select

from plata.core.bus import get_redis
from plata.core.db import LLMCost, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    today = date.today()
    yesterday = today - timedelta(days=1)
    last_7_cutoff = datetime.combine(today - timedelta(days=6), datetime.min.time(), tzinfo=timezone.utc)
    last_30_cutoff = datetime.combine(today - timedelta(days=29), datetime.min.time(), tzinfo=timezone.utc)
    today_cutoff = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
    yesterday_cutoff = datetime.combine(yesterday, datetime.min.time(), tzinfo=timezone.utc)

    # Spend: Postgres is now the source of truth (durable, no 36h TTL). One
    # grouped query per (agent, date) and one global per-date pass.
    per_agent: dict[str, dict[str, float]] = {}   # name -> date_iso -> usd
    agent_names: set[str] = set()
    date_col = func.date(LLMCost.ts).label("d")
    async with session_scope() as session:
        rows = (await session.execute(
            select(LLMCost.agent, date_col, func.sum(LLMCost.cost_usd))
            .group_by(LLMCost.agent, date_col)
        )).all()
    for agent, d, total in rows:
        date_iso = d.isoformat() if hasattr(d, "isoformat") else str(d)
        f = float(total or 0)
        agent_names.add(agent)
        bd = per_agent.setdefault(agent, {})
        bd[date_iso] = bd.get(date_iso, 0.0) + f

    # Status hashes from Redis (live-only data, not historical).
    status_by_name: dict[str, dict] = {}
    status_keys: list[str] = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        status_keys.append(k)
    if status_keys:
        pipe = redis.pipeline()
        for k in status_keys:
            pipe.hgetall(k)
        results = await pipe.execute()
        for k, hh in zip(status_keys, results):
            name = k.split(":")[-1]
            status_by_name[name] = hh or {}
            agent_names.add(name)

    STALE_AFTER_SEC = 120
    now_utc = datetime.now(timezone.utc)
    today_iso = today.isoformat()
    yesterday_iso = yesterday.isoformat()

    def _sum_window(by_date: dict[str, float], cutoff_date: date) -> float:
        cutoff = cutoff_date.isoformat()
        return sum(v for d, v in by_date.items() if d >= cutoff)

    agents_data = []
    for name in sorted(agent_names):
        data = dict(status_by_name.get(name) or {})
        data["name"] = name
        if not status_by_name.get(name):
            data["last_heartbeat"] = None
            data["halted"] = "stopped"
        else:
            hb = data.get("last_heartbeat")
            age = None
            if hb:
                try:
                    age = (now_utc - datetime.fromisoformat(hb)).total_seconds()
                except Exception:  # noqa: BLE001
                    age = None
            data["heartbeat_age_sec"] = age
            if age is None or age > STALE_AFTER_SEC:
                data["stale"] = True
        bd = per_agent.get(name, {})
        data["spend_today_usd"]     = bd.get(today_iso, 0.0)
        data["spend_yesterday_usd"] = bd.get(yesterday_iso, 0.0)
        data["spend_7d_usd"]        = _sum_window(bd, today - timedelta(days=6))
        data["spend_30d_usd"]       = _sum_window(bd, today - timedelta(days=29))
        data["spend_all_usd"]       = sum(bd.values())
        agents_data.append(data)

    # Global totals from the same in-memory dataset.
    all_by_date: dict[str, float] = {}
    for _, bd in per_agent.items():
        for d, v in bd.items():
            all_by_date[d] = all_by_date.get(d, 0.0) + v
    daily_total      = all_by_date.get(today_iso, 0.0)
    yesterday_total  = all_by_date.get(yesterday_iso, 0.0)
    last_7_total     = _sum_window(all_by_date, today - timedelta(days=6))
    last_30_total    = _sum_window(all_by_date, today - timedelta(days=29))
    all_time_total   = sum(all_by_date.values())

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
