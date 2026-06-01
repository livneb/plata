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
    last_7 = set((today - timedelta(days=i)).isoformat() for i in range(0, 7))
    last_30 = set((today - timedelta(days=i)).isoformat() for i in range(0, 30))

    # Single pass over the cost keyspace: build per-agent and global-day tallies
    # in one SCAN. The old route did 12+ SCANs (one global + one per agent +
    # one all-time global) which took ~15s with a few thousand keys.
    per_agent: dict[str, dict[str, float]] = {}   # name -> {date_iso -> usd}
    per_day_global: dict[str, float] = {}         # date_iso -> usd (excludes per-agent rows)
    agent_names: set[str] = set()
    cost_keys: list[str] = []
    async for ck in redis.scan_iter(match="cost:daily:*", count=1000):
        cost_keys.append(ck)
    if cost_keys:
        vals = await redis.mget(*cost_keys)
        for k, v in zip(cost_keys, vals):
            try:
                f = float(v or 0.0)
            except (TypeError, ValueError):
                continue
            # Expected formats:
            #   cost:daily:<YYYY-MM-DD>
            #   cost:daily:<YYYY-MM-DD>:agent:<name>
            parts = k.split(":")
            if len(parts) == 3:                       # global day row
                per_day_global[parts[2]] = per_day_global.get(parts[2], 0.0) + f
            elif len(parts) == 5 and parts[3] == "agent":
                date_iso = parts[2]
                name = parts[4]
                agent_names.add(name)
                per_agent.setdefault(name, {})[date_iso] = per_agent.setdefault(name, {}).get(date_iso, 0.0) + f

    # Status hashes (one HSCAN-style pass via scan_iter + hgetall per match).
    status_by_name: dict[str, dict] = {}
    status_keys: list[str] = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        status_keys.append(k)
    # Pipeline the HGETALLs.
    if status_keys:
        pipe = redis.pipeline()
        for k in status_keys:
            pipe.hgetall(k)
        results = await pipe.execute()
        for k, hh in zip(status_keys, results):
            name = k.split(":")[-1]
            status_by_name[name] = hh or {}
            agent_names.add(name)

    from datetime import datetime as _dt, timezone as _tz
    STALE_AFTER_SEC = 120
    now_utc = _dt.now(_tz.utc)

    def _sum_window(days: set[str], by_date: dict[str, float]) -> float:
        return sum(v for d, v in by_date.items() if d in days)

    agents_data = []
    today_iso = today.isoformat()
    yesterday_iso = yesterday.isoformat()
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
                    age = (now_utc - _dt.fromisoformat(hb)).total_seconds()
                except Exception:  # noqa: BLE001
                    age = None
            data["heartbeat_age_sec"] = age
            if age is None or age > STALE_AFTER_SEC:
                data["stale"] = True
        bd = per_agent.get(name, {})
        data["spend_today_usd"]     = bd.get(today_iso, 0.0)
        data["spend_yesterday_usd"] = bd.get(yesterday_iso, 0.0)
        data["spend_7d_usd"]        = _sum_window(last_7, bd)
        data["spend_30d_usd"]       = _sum_window(last_30, bd)
        data["spend_all_usd"]       = sum(bd.values())
        agents_data.append(data)

    daily_total      = per_day_global.get(today_iso, 0.0)
    yesterday_total  = per_day_global.get(yesterday_iso, 0.0)
    last_7_total     = _sum_window(last_7, per_day_global)
    last_30_total    = _sum_window(last_30, per_day_global)
    all_time_total   = sum(per_day_global.values())

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
