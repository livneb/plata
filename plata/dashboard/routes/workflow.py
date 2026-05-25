"""Workflow view: high-level live tasks the system is currently performing.

Not signal-pov — this is operational pov: 'Polling Reddit', 'Analyzing N signals',
'Awaiting approval on M proposals', 'Executing X', 'Reviewing Y'.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, func, select

from plata.config.settings import get_settings
from plata.core.bus import get_redis
from plata.core.db import TradeLedger, session_scope
from plata.dashboard import templates
from plata.hitl.approval_store import list_pending

router = APIRouter(prefix="/workflow", tags=["workflow"])


SOURCE_DESCRIPTIONS = {
    "reddit": "Polling finance/crypto subreddits",
    "cryptopanic": "Polling CryptoPanic news feed",
    "gdelt": "Polling GDELT global news",
    "newsapi": "Polling NewsAPI headlines",
    "cryptonews": "Polling CryptoNews aggregator",
    "lunarcrush": "Polling LunarCrush sentiment",
    "whalealert": "Polling WhaleAlert on-chain",
}


async def _source_tasks() -> list[dict[str, Any]]:
    redis = get_redis()
    out = []
    async for k in redis.scan_iter(match="scraper:source:*", count=100):
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        out.append({
            "name": name,
            "title": SOURCE_DESCRIPTIONS.get(name, f"Polling {name}"),
            "status": data.get("status") or "unknown",
            "last_poll_at": data.get("last_poll_at"),
            "started_at": data.get("started_at"),
            "interval_sec": int(data.get("interval_sec") or 0),
            "last_fetched": int(data.get("last_fetched") or 0),
            "last_error": data.get("last_error") or "",
        })
    return sorted(out, key=lambda x: x["name"])


async def _agent_tasks() -> list[dict[str, Any]]:
    """One task per agent currently doing work (in_flight > 0) or recently active."""
    redis = get_redis()
    keys = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        keys.append(k)
    AGENT_VERBS = {
        "scraper": "Polling sources",
        "graph_ingestion": "Extracting entities + embedding signals",
        "strategist": "Analyzing events vs analogs",
        "reviewer": "Reviewing closed trades",
        "risk_manager": "Sizing positions + risk-gating",
        "executor": "Executing approved trades",
        "orchestrator": "Watching heartbeats + DLQ",
        "telegram_bot": "Awaiting Telegram commands",
    }
    out = []
    for k in sorted(keys):
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        recent = await redis.lrange(f"agent_activity:{name}", 0, 0)
        last_summary = ""
        if recent:
            parts = recent[0].split("|", 2)
            if len(parts) == 3:
                last_summary = parts[2]
        out.append({
            "name": name,
            "verb": AGENT_VERBS.get(name, name),
            "in_flight": int(data.get("in_flight") or 0),
            "halted": data.get("halted") == "True",
            "container": data.get("container") or "",
            "last_heartbeat": data.get("last_heartbeat"),
            "last_summary": last_summary,
        })
    return out


async def _hitl_tasks() -> list[dict[str, Any]]:
    pending = await list_pending()
    out = []
    for p in pending:
        prop = p.get("proposal", {}) if isinstance(p, dict) else {}
        out.append({
            "proposal_ulid": p.get("proposal_ulid") or p.get("ulid") or "?",
            "symbol": prop.get("symbol") or prop.get("asset") or "?",
            "side": prop.get("side") or "?",
            "reason": p.get("reason") or "approval required",
            "created_at": p.get("created_at"),
        })
    return out


async def _open_trades() -> list[dict[str, Any]]:
    async with session_scope() as session:
        result = await session.execute(
            select(TradeLedger)
            .where(TradeLedger.closed_at.is_(None))
            .order_by(desc(TradeLedger.opened_at))
            .limit(20)
        )
        rows = result.scalars().all()
    return [
        {
            "ulid": r.trade_ulid,
            "symbol": r.symbol,
            "side": r.side,
            "qty": float(r.qty or 0),
            "entry": float(r.entry_price or 0),
            "mode": r.mode,
            "opened_at": r.opened_at,
        }
        for r in rows
    ]


async def _gather() -> dict[str, Any]:
    settings = get_settings()
    redis = get_redis()
    state = await redis.get("system:state")
    return {
        "system_state": state or "RUNNING",
        "paper_mode": settings.default_paper_trading_mode,
        "sources": await _source_tasks(),
        "agents": await _agent_tasks(),
        "hitl": await _hitl_tasks(),
        "open_trades": await _open_trades(),
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
