"""Live activity / progress view: pipeline depths, recent signals, API status."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
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

    def ok(value) -> bool:
        return bool(value)

    return [
        {
            "name": "Reddit",
            "desc": "Polls finance/crypto subreddits for new posts (signal source).",
            "configured": ok(s.reddit_client_id) and ok(s.reddit_client_secret),
        },
        {
            "name": "CryptoPanic",
            "desc": "Aggregated crypto news headlines (signal source).",
            "configured": ok(s.cryptopanic_api_key),
        },
        {
            "name": "GDELT",
            "desc": "Global news/event database — no key required (rate-limited).",
            "configured": True,
        },
        {
            "name": "NewsAPI",
            "desc": "General news headlines (optional signal source).",
            "configured": ok(s.newsapi_key),
        },
        {
            "name": "CryptoNews",
            "desc": "Crypto-specific news aggregator (optional).",
            "configured": ok(s.cryptonews_api_key),
        },
        {
            "name": "LunarCrush",
            "desc": "Social-sentiment metrics for crypto assets (optional).",
            "configured": ok(s.lunarcrush_api_key),
        },
        {
            "name": "WhaleAlert",
            "desc": "Large on-chain transactions (optional).",
            "configured": ok(s.whalealert_api_key),
        },
        {
            "name": "OpenRouter",
            "desc": "LLM gateway used by Strategist/Reviewer/GraphIngestion.",
            "configured": ok(s.openrouter_api_key),
        },
        {
            "name": "Voyage",
            "desc": "Embeddings provider for semantic search of past events.",
            "configured": ok(s.voyage_api_key),
        },
        {
            "name": "Langfuse",
            "desc": "LLM observability — traces, prompts, cost tracking.",
            "configured": ok(s.langfuse_public_key) and ok(s.langfuse_secret_key),
        },
        {
            "name": "Telegram",
            "desc": "HITL approval channel — proposals are sent here for approve/reject.",
            "configured": ok(s.telegram_bot_token),
        },
        {
            "name": "Bybit",
            "desc": (
                f"Exchange for order execution ({'TESTNET' if s.bybit_testnet else 'MAINNET'})."
            ),
            "configured": ok(s.bybit_api_key) and ok(s.bybit_api_secret),
        },
    ]


async def _pipeline_depths() -> list[dict[str, Any]]:
    redis = get_redis()
    out = []
    for stage, stream, label in PIPELINE:
        try:
            length = await redis.xlen(stream)
            ok = True
        except Exception as exc:  # noqa: BLE001
            length = str(exc)
            ok = False
        out.append({"stage": stage, "label": label, "length": length, "ok": ok})
    return out


async def _signal_stats() -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    async with session_scope() as session:
        total = (
            await session.execute(select(func.count()).select_from(SignalArchive))
        ).scalar() or 0
        recent_24h = (
            await session.execute(
                select(func.count())
                .select_from(SignalArchive)
                .where(SignalArchive.fetched_at >= cutoff)
            )
        ).scalar() or 0
        by_source = (
            await session.execute(
                select(SignalArchive.source, func.count())
                .where(SignalArchive.fetched_at >= cutoff)
                .group_by(SignalArchive.source)
            )
        ).all()
        recent_rows = (
            await session.execute(
                select(SignalArchive)
                .order_by(desc(SignalArchive.fetched_at))
                .limit(40)
            )
        ).scalars().all()
    return {
        "total_signals": total,
        "recent_24h": recent_24h,
        "by_source": list(by_source),
        "recent_rows": recent_rows,
    }


async def _gather() -> dict[str, Any]:
    depths = await _pipeline_depths()
    stats = await _signal_stats()
    return {
        "pipeline": depths,
        "apis": _api_statuses(),
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
