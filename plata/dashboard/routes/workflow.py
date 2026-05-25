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
    "reddit": "Reddit",
    "cryptopanic": "CryptoPanic",
    "gdelt": "GDELT",
    "newsapi": "NewsAPI",
    "cryptonews": "CryptoNews",
    "lunarcrush": "LunarCrush",
    "whalealert": "WhaleAlert",
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


async def _source_cards() -> list[dict[str, Any]]:
    """Per-source cards. Returned with `lane` set based on the source's current state."""
    redis = get_redis()
    cards: list[dict[str, Any]] = []
    async for k in redis.scan_iter(match="scraper:source:*", count=100):
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        raw = data.get("status") or "sleeping"
        # Status badge value
        status = {
            "polling": "polling",
            "idle": "sleeping",
            "halted": "halted",
            "error": "error",
        }.get(raw, "sleeping")
        # Lane is driven by state — actively polling = Doing, errored = Active so it's prominent.
        if status == "polling":
            lane = "doing"
        elif status == "error":
            lane = "active"
        else:
            lane = "sleeping"
        cards.append({
            "lane": lane,
            "category": "ingestion",
            "agent": "scraper",
            "title": SOURCE_LABELS.get(name, name),
            "subtitle": f"polls every {data.get('interval_sec', '?')}s",
            "status": status,
            "ts": data.get("last_poll_at"),
            "extra": f"fetched {data.get('last_fetched', '0')} last cycle",
            "error": data.get("last_error") or "",
        })
    return cards


async def _historian_batch_cards() -> list[dict[str, Any]]:
    """One card per active historian batch (running / failed)."""
    redis = get_redis()
    out: list[dict[str, Any]] = []
    async for k in redis.scan_iter(match="historian:batch:*", count=200):
        data = await redis.hgetall(k)
        if not data:
            continue
        state = data.get("state") or "running"
        i = data.get("i", "?")
        total = data.get("total_batches", "?")
        evs = data.get("events_in_batch", "0")
        size = data.get("size", "?")
        lane = "doing"
        status = "running"
        if state == "done":
            lane = "done"; status = "ok"
        elif state == "failed":
            lane = "active"; status = "error"
        out.append({
            "lane": lane,
            "category": "intelligence",
            "agent": "historian",
            "title": f"Batch {int(i)+1 if str(i).isdigit() else i}/{total} · {evs}/{size} events",
            "subtitle": data.get("last_event_date") or "",
            "status": status,
            "ts": data.get("started_at"),
            "extra": "",
            "error": data.get("last_error") or "",
        })
    # Newest batches first
    out.sort(key=lambda c: c.get("ts") or "", reverse=True)
    return out


async def _historian_card() -> dict[str, Any] | None:
    """A card describing the current Historian seed run, if any."""
    redis = get_redis()
    data = await redis.hgetall("historian:status")
    if not data:
        return None
    state = data.get("state") or ""
    written = int(data.get("written") or 0)
    target = int(data.get("total_target") or 0)
    pct = (written * 100 // target) if target > 0 else 0
    brief = (data.get("brief") or "").strip()
    last = data.get("last_event_date") or ""
    # Promote stale "running" rows to a visible failure so users notice + reset.
    if state == "running":
        last_raw = data.get("last_progress_at") or data.get("started_at") or ""
        try:
            last_dt = datetime.fromisoformat(last_raw)
            age = (datetime.utcnow() - last_dt).total_seconds()
            if age > 180:
                state = "stale"
        except Exception:  # noqa: BLE001
            pass
    if state == "stale":
        return {
            "lane": "active",
            "category": "intelligence",
            "agent": "historian",
            "title": f"Stale seed: {written}/{target} written",
            "subtitle": "No progress in 3+ min — process likely died. Reset on /historian/.",
            "status": "error",
            "ts": data.get("last_progress_at") or data.get("started_at"),
            "extra": "",
        }
    if state == "running":
        return {
            "lane": "doing",
            "category": "intelligence",
            "agent": "historian",
            "title": f"Seeding {written}/{target} ({pct}%)",
            "subtitle": brief[:80] or "largest market-moving events",
            "status": "running",
            "ts": data.get("started_at"),
            "extra": f"last event: {last}" if last else "",
        }
    if state == "done":
        return {
            "lane": "done",
            "category": "intelligence",
            "agent": "historian",
            "title": f"Seeded {written} historical events",
            "subtitle": brief[:80] or "largest market-moving events",
            "status": "ok",
            "ts": data.get("finished_at"),
            "extra": "",
        }
    if state == "failed":
        return {
            "lane": "active",
            "category": "intelligence",
            "agent": "historian",
            "title": "Historian seed failed",
            "subtitle": (data.get("last_error") or "")[:80],
            "status": "error",
            "ts": data.get("started_at"),
            "extra": "",
        }
    return None


async def _active_cards() -> list[dict[str, Any]]:
    """Event-driven observers — orchestrator, telegram bot."""
    redis = get_redis()
    cards: list[dict[str, Any]] = []
    for name in ("orchestrator", "telegram_bot"):
        data = await redis.hgetall(f"agent_status:{name}")
        if not data:
            continue
        recent = await redis.lrange(f"agent_activity:{name}", 0, 0)
        last_action = ""
        if recent:
            parts = recent[0].split("|", 2)
            if len(parts) == 3:
                last_action = parts[2]
        cards.append({
            "lane": "active",
            "category": CATEGORY.get(name, "ops"),
            "agent": name,
            "title": AGENT_VERB.get(name, name),
            "subtitle": data.get("container", ""),
            "status": "halted" if data.get("halted") == "True" else "active",
            "ts": data.get("last_heartbeat"),
            "extra": last_action,
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
        oldest_ts = None
        group = f"{consumer.replace('_', '-')}-grp"
        try:
            info = await redis.xpending(stream, group)
            # Newer redis-py returns a dict; older returns a list.
            if isinstance(info, dict):
                pending_count = int(info.get("pending") or 0)
                min_id = info.get("min")
            else:
                pending_count = int(info[0] or 0) if info else 0
                min_id = info[1] if len(info) > 1 else None
            # Redis stream IDs are "<ms>-<seq>"; the ms part is the message creation time.
            if pending_count > 0 and min_id:
                try:
                    ms = int(str(min_id).split("-")[0])
                    oldest_ts = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    oldest_ts = None
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
            "status": "waiting" if pending_count > 0 else "caught up",
            "ts": oldest_ts,
            "ts_label": "oldest" if oldest_ts else "",
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
    """Recent successful handler calls across pipeline agents.

    Adjacent entries from the same agent within 5 s are merged into a single card
    showing the latest summary and a (N) count.
    """
    redis = get_redis()
    skip = {"orchestrator", "telegram_bot", "scraper"}
    entries: list[tuple[str, str, str]] = []
    async for k in redis.scan_iter(match="agent_activity:*", count=100):
        agent = k.split(":")[-1]
        if agent in skip:
            continue
        rows = await redis.lrange(k, 0, limit * 4)
        for row in rows:
            parts = row.split("|", 2)
            if len(parts) != 3 or parts[1] != "ok":
                continue
            entries.append((parts[0], agent, parts[2]))
    entries.sort(key=lambda x: x[0], reverse=True)

    # Group consecutive same-agent entries within a 5-second window into a single card.
    cards: list[dict[str, Any]] = []
    for ts, agent, summary in entries:
        try:
            ts_dt = datetime.fromisoformat(ts)
        except Exception:  # noqa: BLE001
            ts_dt = None
        if cards:
            last = cards[-1]
            if (last["agent"] == agent
                    and ts_dt is not None
                    and last.get("_ts_dt") is not None
                    and (last["_ts_dt"] - ts_dt).total_seconds() <= 5
                    and (last["_ts_dt"] - ts_dt).total_seconds() >= 0):
                last["count"] = last.get("count", 1) + 1
                # keep the newest summary visible (already the first one we saw)
                last["older_summaries"].append(summary)
                continue
        cards.append({
            "lane": "done",
            "category": CATEGORY.get(agent, "intelligence"),
            "agent": agent,
            "title": summary or AGENT_VERB.get(agent, agent),
            "subtitle": "",
            "status": "ok",
            "ts": ts,
            "_ts_dt": ts_dt,
            "extra": "",
            "count": 1,
            "older_summaries": [],
        })
        if len(cards) >= limit:
            break
    # Drop helper field before returning; format title with (N) if grouped.
    out: list[dict[str, Any]] = []
    for c in cards:
        c.pop("_ts_dt", None)
        n = c.get("count", 1)
        if n > 1:
            c["title"] = f"{c['title']}  ({n})"
        out.append(c)
    return out


async def _gather() -> dict[str, Any]:
    settings = get_settings()
    redis = get_redis()
    state = await redis.get("system:state")
    sources = await _source_cards()
    active = await _active_cards()
    doing = await _doing_cards()
    done = await _done_cards()
    sleeping_lane: list[dict] = []
    for c in sources:
        if c["lane"] == "doing":
            doing.append(c)
        elif c["lane"] == "active":
            active.append(c)
        else:
            sleeping_lane.append(c)

    historian_card = await _historian_card()
    if historian_card:
        if historian_card["lane"] == "doing":
            doing.insert(0, historian_card)
        elif historian_card["lane"] == "active":
            active.append(historian_card)
        elif historian_card["lane"] == "done":
            done.insert(0, historian_card)

    # Per-batch cards from the active historian run, capped so they don't drown the lanes.
    for bc in (await _historian_batch_cards())[:8]:
        if bc["lane"] == "doing":
            doing.append(bc)
        elif bc["lane"] == "active":
            active.append(bc)
        elif bc["lane"] == "done":
            done.append(bc)

    return {
        "system_state": state or "RUNNING",
        "paper_mode": settings.default_paper_trading_mode,
        "sleeping": sleeping_lane,
        "active": active,
        "ready": await _ready_cards(),
        "doing": doing,
        "done": done,
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
