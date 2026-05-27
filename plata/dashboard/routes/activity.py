"""Live activity / progress view: pipeline depths, per-agent state, recent signals, API status."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, func, select

from plata.config.settings import get_settings
from plata.core.bus import Streams, get_redis
from plata.core.db import SignalArchive, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/activity", tags=["activity"])


PIPELINE = [
    ("Scraper", Streams.RAW_SIGNALS, "raw signals"),
    ("Graph Ingestion", Streams.ENRICHED_EVENTS, "enriched events"),
    ("Strategist", Streams.TRADING_PROPOSALS, "trading proposals"),
    ("Executor", Streams.TRADE_CLOSURES, "closed trades"),
]


def _api_statuses() -> list[dict[str, Any]]:
    s = get_settings()
    # Also consult the DB-managed credentials store (Settings → 🔑 API Keys).
    # Without this, anything saved via the UI shows up as NOT SET here even
    # though the agents are happily using it.
    try:
        from plata.config import credentials as _creds
        def ok(value, provider: str | None = None) -> bool:
            if value:
                return True
            if provider:
                return bool(_creds.get_sync(provider))
            return False
    except Exception:  # noqa: BLE001
        def ok(value, provider: str | None = None) -> bool:  # type: ignore[no-redef]
            return bool(value)
    return [
        {"name": "Reddit", "desc": "Polls finance/crypto subreddits for new posts.",
         "configured": ok(s.reddit_client_id) and ok(s.reddit_client_secret)},
        {"name": "CryptoPanic", "desc": "Aggregated crypto news headlines.",
         "configured": ok(s.cryptopanic_api_key, "cryptopanic")},
        {"name": "GDELT", "desc": "Global news/event database — no key (rate-limited).",
         "configured": True},
        {"name": "NewsAPI", "desc": "General news headlines (optional).",
         "configured": ok(s.newsapi_key, "newsapi")},
        {"name": "CryptoNews", "desc": "Crypto-specific news aggregator (optional).",
         "configured": ok(s.cryptonews_api_key, "cryptonews")},
        {"name": "LunarCrush", "desc": "Social-sentiment metrics for crypto (optional).",
         "configured": ok(s.lunarcrush_api_key, "lunarcrush")},
        {"name": "WhaleAlert", "desc": "Large on-chain transactions (optional).",
         "configured": ok(s.whalealert_api_key, "whalealert")},
        {"name": "OpenRouter", "desc": "LLM gateway for all agents.",
         "configured": ok(s.openrouter_api_key, "openrouter")},
        {"name": "Voyage", "desc": "Embeddings for semantic search of past events.",
         "configured": ok(s.voyage_api_key, "voyage")},
        {"name": "Langfuse", "desc": "LLM observability (traces, prompts, cost).",
         "configured": (ok(s.langfuse_public_key, "langfuse_public")
                        and ok(s.langfuse_secret_key, "langfuse_secret"))},
        {"name": "Telegram", "desc": "HITL approval channel for proposals.",
         "configured": ok(s.telegram_bot_token, "telegram")},
        {"name": "Bybit",
         "desc": f"Crypto-perp exchange ({'TESTNET' if s.bybit_testnet else 'MAINNET'}).",
         "configured": (ok(s.bybit_api_key, "bybit_key")
                        and ok(s.bybit_api_secret, "bybit_secret"))},
        {"name": "Alpaca",
         "desc": f"US equities + ETFs broker ({'PAPER' if s.alpaca_paper else 'LIVE'}).",
         "configured": (ok(s.alpaca_api_key, "alpaca_key")
                        and ok(s.alpaca_api_secret, "alpaca_secret"))},
    ]


async def _pipeline_depths() -> list[dict[str, Any]]:
    redis = get_redis()
    out = []
    for stage, stream, label in PIPELINE:
        try:
            length = await redis.xlen(stream)
            err = None
        except Exception as exc:  # noqa: BLE001
            length, err = 0, str(exc)
        try:
            dlq_len = await redis.xlen(f"dlq:{stream}")
        except Exception:  # noqa: BLE001
            dlq_len = 0
        out.append({"stage": stage, "stream": stream, "label": label,
                    "length": length, "dlq": dlq_len, "err": err})
    return out


async def _agent_states() -> list[dict[str, Any]]:
    redis = get_redis()
    keys: list[str] = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        keys.append(k)
    out = []
    for k in sorted(keys):
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        dlq_stats = await redis.hgetall(f"dlq:stats:{name}")
        stats = await redis.hgetall(f"agent_stats:{name}")
        activity = await redis.lrange(f"agent_activity:{name}", 0, 7)
        parsed_activity = []
        for entry in activity:
            parts = entry.split("|", 2)
            if len(parts) == 3:
                parsed_activity.append({"ts": parts[0], "kind": parts[1], "summary": parts[2]})
        out.append({
            "name": name,
            "container": data.get("container", "?"),
            "last_heartbeat": data.get("last_heartbeat"),
            "in_flight": int(data.get("in_flight") or 0),
            "halted": data.get("halted") == "True",
            "last_processed_ulid": data.get("last_processed_ulid"),
            "errors_total": int(dlq_stats.get("count") or 0),
            "processed_total": int(stats.get("processed_total") or 0),
            "dropped": {k.replace("dropped_", ""): int(v) for k, v in stats.items() if k.startswith("dropped_")},
            "activity": parsed_activity,
        })
    return out


async def _llm_spend() -> dict[str, Any]:
    redis = get_redis()
    today = date.today().isoformat()
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    daily = float(await redis.get(f"cost:daily:{today}") or 0)
    monthly = float(await redis.get(f"cost:monthly:{month}") or 0)
    cfg = await redis.hgetall("risk_config")
    daily_cap = float(cfg.get("llm.daily_budget_usd_total", 0) or 0)
    return {"daily_usd": daily, "monthly_usd": monthly, "daily_cap_usd": daily_cap}


async def _system_state() -> dict[str, Any]:
    redis = get_redis()
    state = await redis.get("system:state") or "UNKNOWN"
    cfg = await redis.hgetall("risk_config")
    paper = cfg.get("paper_trading_mode", "true").lower() in ("true", "1", "yes")
    return {"state": state, "paper_mode": paper}


async def _signal_stats() -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    async with session_scope() as session:
        total = (await session.execute(select(func.count()).select_from(SignalArchive))).scalar() or 0
        recent_24h = (await session.execute(
            select(func.count()).select_from(SignalArchive).where(SignalArchive.fetched_at >= cutoff)
        )).scalar() or 0
        recent_1h = (await session.execute(
            select(func.count()).select_from(SignalArchive).where(
                SignalArchive.fetched_at >= datetime.now(timezone.utc) - timedelta(hours=1)
            )
        )).scalar() or 0
        dups_24h = (await session.execute(
            select(func.count()).select_from(SignalArchive)
            .where(SignalArchive.fetched_at >= cutoff)
            .where(SignalArchive.is_duplicate.is_(True))
        )).scalar() or 0
        by_source = (await session.execute(
            select(SignalArchive.source, func.count())
            .where(SignalArchive.fetched_at >= cutoff)
            .group_by(SignalArchive.source)
        )).all()
        recent_rows = (await session.execute(
            select(SignalArchive).order_by(desc(SignalArchive.fetched_at)).limit(40)
        )).scalars().all()
    return {
        "total_signals": total,
        "recent_24h": recent_24h,
        "recent_1h": recent_1h,
        "dups_24h": dups_24h,
        "by_source": list(by_source),
        "recent_rows": recent_rows,
    }


async def _api_statuses_with_limits() -> list[dict[str, Any]]:
    """Merge `_api_statuses()` with any active `api_limit:<provider>` flags."""
    import json as _json
    rows = _api_statuses()
    redis = get_redis()
    flags: dict[str, dict[str, Any]] = {}
    async for k in redis.scan_iter(match="api_limit:*", count=200):
        try:
            raw = await redis.get(k)
            if raw:
                d = _json.loads(raw)
                flags[(d.get("provider") or "").lower()] = d
        except Exception:  # noqa: BLE001
            continue
    for r in rows:
        prov = (r.get("name") or "").lower()
        if prov in flags:
            r["limit_flag"] = flags[prov]
    return rows


async def _gather() -> dict[str, Any]:
    depths = await _pipeline_depths()
    stats = await _signal_stats()
    agents = await _agent_states()
    spend = await _llm_spend()
    system = await _system_state()
    return {
        "pipeline": depths,
        "agents": agents,
        "apis": await _api_statuses_with_limits(),
        "spend": spend,
        "system": system,
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        **stats,
    }


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request, "pages/activity.html", {"active": "activity", **await _gather()}
    )


@router.get("/_fragment", response_class=HTMLResponse)
async def fragment(request: Request):
    return templates.TemplateResponse(
        request, "pages/_activity_fragment.html", await _gather()
    )


@router.get("/history", response_class=HTMLResponse)
async def history(
    request: Request,
    agent: str | None = None,
    kind: str | None = None,
    q: str | None = None,
    limit: int = 200,
):
    """Durable history of agent actions. Reads from Postgres agent_activity_log
    (Redis Done lane only keeps the last 200 per agent)."""
    from sqlalchemy import desc, distinct, select
    from plata.core.db import AgentActivityLog, session_scope

    rows: list = []
    agents: list[str] = []
    try:
        async with session_scope() as session:
            agents = sorted([r for (r,) in (await session.execute(
                select(distinct(AgentActivityLog.agent))
            )).all() if r])
            stmt = select(AgentActivityLog).order_by(desc(AgentActivityLog.ts)).limit(min(int(limit), 1000))
            if agent:
                stmt = stmt.where(AgentActivityLog.agent == agent)
            if kind:
                stmt = stmt.where(AgentActivityLog.kind == kind)
            if q:
                stmt = stmt.where(AgentActivityLog.summary.ilike(f"%{q}%"))
            rows = (await session.execute(stmt)).scalars().all()
    except Exception:  # noqa: BLE001
        pass

    return templates.TemplateResponse(
        request,
        "pages/activity_history.html",
        {
            "active": "activity",
            "rows": rows,
            "agents": agents,
            "filter_agent": agent or "",
            "filter_kind": kind or "",
            "filter_q": q or "",
            "limit": limit,
        },
    )
