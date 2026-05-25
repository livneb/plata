"""Kanban-style workflow view.

Lanes:
  BACKGROUND — persistent watchers (always-on tasks: scraper polls, orchestrator, telegram).
  READY      — messages queued in Redis streams waiting to be consumed.
  DOING      — agents with in_flight > 0; shows what each is currently handling.
  DONE       — most recently completed handler invocations (merged across agents).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from plata.config.settings import get_settings
from plata.core.bus import Streams, get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/workflow", tags=["workflow"])

# Map stream → producer agent + downstream consumer (for "Ready" lane labels).
STREAM_FLOW = [
    (Streams.RAW_SIGNALS, "scraper", "graph_ingestion"),
    (Streams.ENRICHED_EVENTS, "graph_ingestion", "strategist"),
    (Streams.TRADING_PROPOSALS, "strategist", "risk_manager"),
    (Streams.TRADE_CLOSURES, "executor", "reviewer"),
]

SOURCE_LABELS = {
    "reddit": "Polling Reddit",
    "cryptopanic": "Polling CryptoPanic",
    "gdelt": "Polling GDELT",
    "newsapi": "Polling NewsAPI",
    "cryptonews": "Polling CryptoNews",
    "lunarcrush": "Polling LunarCrush",
    "whalealert": "Polling WhaleAlert",
}

AGENT_VERB = {
    "scraper": "Polling external sources",
    "graph_ingestion": "Extracting entities + embedding",
    "strategist": "Analyzing event + searching analogs",
    "reviewer": "Reviewing closed trades",
    "risk_manager": "Sizing + risk gating",
    "executor": "Submitting orders to Bybit",
    "orchestrator": "Watching heartbeats + DLQ",
    "telegram_bot": "Awaiting Telegram commands",
}

CATEGORY = {
    "scraper": "ingestion",
    "graph_ingestion": "intelligence",
    "strategist": "intelligence",
    "reviewer": "intelligence",
    "risk_manager": "execution",
    "executor": "execution",
    "orchestrator": "ops",
    "telegram_bot": "hitl",
}


async def _background_cards() -> list[dict[str, Any]]:
    """Persistent watchers — always-on tasks that don't 'finish'."""
    redis = get_redis()
    cards: list[dict[str, Any]] = []

    # Scraper sources — collapse the brief "idle" between polls into "running".
    async for k in redis.scan_iter(match="scraper:source:*", count=100):
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        raw = data.get("status") or "running"
        status = {
            "polling": "polling",   # mid-fetch (brief, pulses)
            "idle": "running",      # between polls — healthy
            "halted": "halted",
            "error": "error",
        }.get(raw, "running")
        cards.append({
            "lane": "background",
            "category": "ingestion",
            "agent": "scraper",
            "title": SOURCE_LABELS.get(name, f"Polling {name}"),
            "subtitle": f"every {data.get('interval_sec', '?')}s",
            "status": status,
            "ts": data.get("last_poll_at"),
            "extra": f"fetched {data.get('last_fetched', '0')} last cycle",
            "error": data.get("last_error") or "",
        })

    # Orchestrator + telegram (driven by agent_status hash)
    for name in ("orchestrator", "telegram_bot"):
        data = await redis.hgetall(f"agent_status:{name}")
        if not data:
            continue
        cards.append({
            "lane": "background",
            "category": CATEGORY.get(name, "ops"),
            "agent": name,
            "title": AGENT_VERB.get(name, name),
            "subtitle": data.get("container", ""),
            "status": "halted" if data.get("halted") == "True" else "running",
            "ts": data.get("last_heartbeat"),
            "extra": "",
        })

    return cards


async def _ready_cards() -> list[dict[str, Any]]:
    """One card per consumer group with pending (unread) entries."""
    redis = get_redis()
    cards = []
    for stream, producer, consumer in STREAM_FLOW:
        try:
            length = await redis.xlen(stream)
        except Exception:  # noqa: BLE001
            continue
        pending_count = 0
        # Best-effort: XPENDING needs a group; we use the consumer's own group name
        # convention from BaseAgent (`<agent>-grp`). Falls back silently if absent.
        group = f"{consumer.replace('_', '-')}-grp"
        try:
            info = await redis.xpending(stream, group)
            pending_count = int(info.get("pending") or 0) if isinstance(info, dict) else int(info[0] or 0)
        except Exception:  # noqa: BLE001
            pending_count = 0
        if pending_count <= 0 and length == 0:
            continue
        cards.append({
            "lane": "ready",
            "category": CATEGORY.get(consumer, "intelligence"),
            "agent": consumer,
            "title": f"{pending_count} waiting for {consumer}",
            "subtitle": f"from {producer} → {stream.split(':')[0]}",
            "status": "waiting" if pending_count > 0 else "empty",
            "ts": None,
            "extra": f"stream total: {length}",
        })
    return cards


async def _doing_cards() -> list[dict[str, Any]]:
    """One card per agent with in_flight > 0."""
    redis = get_redis()
    cards = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        in_flight = int(data.get("in_flight") or 0)
        if in_flight <= 0 or name in ("orchestrator", "telegram_bot"):
            continue
        recent = await redis.lrange(f"agent_activity:{name}", 0, 0)
        last_summary = ""
        if recent:
            parts = recent[0].split("|", 2)
            if len(parts) == 3 and parts[1] == "ok":
                last_summary = parts[2]
        cards.append({
            "lane": "doing",
            "category": CATEGORY.get(name, "intelligence"),
            "agent": name,
            "title": AGENT_VERB.get(name, name),
            "subtitle": f"×{in_flight} in flight",
            "status": "running",
            "ts": data.get("last_heartbeat"),
            "extra": last_summary,
        })
    return cards


async def _done_cards(limit: int = 24) -> list[dict[str, Any]]:
    """Recent successful handler calls across all agents, newest first."""
    redis = get_redis()
    entries: list[tuple[str, str, str]] = []  # (iso_ts, agent, summary)
    async for k in redis.scan_iter(match="agent_activity:*", count=100):
        agent = k.split(":")[-1]
        rows = await redis.lrange(k, 0, limit)
        for row in rows:
            parts = row.split("|", 2)
            if len(parts) != 3 or parts[1] != "ok":
                continue
            entries.append((parts[0], agent, parts[2]))
    entries.sort(key=lambda x: x[0], reverse=True)
    return [
        {
            "lane": "done",
            "category": CATEGORY.get(agent, "intelligence"),
            "agent": agent,
            "title": summary[:140] if summary else "(no summary)",
            "subtitle": AGENT_VERB.get(agent, agent),
            "status": "ok",
            "ts": ts,
            "extra": "",
        }
        for ts, agent, summary in entries[:limit]
    ]


async def _gather() -> dict[str, Any]:
    settings = get_settings()
    redis = get_redis()
    state = await redis.get("system:state")
    return {
        "system_state": state or "RUNNING",
        "paper_mode": settings.default_paper_trading_mode,
        "background": await _background_cards(),
        "ready": await _ready_cards(),
        "doing": await _doing_cards(),
        "done": await _done_cards(),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request, "pages/workflow.html", {"active": "workflow", **await _gather()}
    )


@router.get("/_fragment", response_class=HTMLResponse)
async def fragment(request: Request):
    return templates.TemplateResponse(
        request, "pages/_workflow_fragment.html", await _gather()
    )
