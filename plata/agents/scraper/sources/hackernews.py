"""Hacker News — front page stories.

HN's official Firebase API exposes the front page as a JSON list. Stories
with high scores often correlate with tech-stock catalysts (cloud
earnings, AI launches, regulatory actions, security breaches).

API: https://hacker-news.firebaseio.com/v0/topstories.json -> [id, ...]
     https://hacker-news.firebaseio.com/v0/item/<id>.json -> story
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from plata.agents.scraper.news_config import (
    DEFAULTS as NEWS_DEFAULTS,
    get_config as get_news_config,
    record_poll_probe,
)
from plata.agents.scraper.sources.base_source import BaseSource
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.hackernews")

TOPSTORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"


class HackerNewsSource(BaseSource):
    name = "hackernews"
    poll_interval_sec = 10 * 60

    def __init__(self) -> None:
        self._seen_ids: set[int] = set()

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("hackernews_enabled", True):
            await record_poll_probe("hackernews", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        top_n = int(cfg.get("hackernews_top_n") or NEWS_DEFAULTS["hackernews_top_n"])
        min_score = int(cfg.get("hackernews_min_score")
                         or NEWS_DEFAULTS["hackernews_min_score"])
        signals: list[RawSignal] = []
        probe: dict = {"top_n": top_n, "min_score": min_score}
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                r = await client.get(TOPSTORIES_URL)
                probe["http_status"] = r.status_code
                if r.status_code >= 400:
                    probe["error_type"] = f"HTTP{r.status_code}"
                    await record_poll_probe("hackernews", **probe)
                    return []
                ids = (r.json() or [])[:top_n]
            except Exception as exc:  # noqa: BLE001
                probe["error_type"] = type(exc).__name__
                probe["error_message"] = str(exc)[:200]
                await record_poll_probe("hackernews", **probe)
                return []
            # Fetch each story in parallel-ish (HN API is fast).
            async def _fetch(sid: int):
                try:
                    rr = await client.get(ITEM_URL.format(sid))
                    return rr.json() if rr.status_code == 200 else None
                except Exception:  # noqa: BLE001
                    return None
            stories = await asyncio.gather(*(_fetch(i) for i in ids if i not in self._seen_ids))
        for s in stories:
            if not s:
                continue
            sid = s.get("id")
            if not sid or sid in self._seen_ids:
                continue
            self._seen_ids.add(sid)
            score = int(s.get("score") or 0)
            if score < min_score:
                continue
            title = (s.get("title") or "").strip()
            url = s.get("url") or f"https://news.ycombinator.com/item?id={sid}"
            ts = datetime.fromtimestamp(int(s.get("time") or 0), tz=timezone.utc) \
                if s.get("time") else datetime.now(timezone.utc)
            signals.append(RawSignal(
                source=SignalSource.HACKERNEWS,
                url=url,
                title=title,
                body=title,
                source_published_at=ts,
                metadata={
                    "score": score,
                    "descendants": int(s.get("descendants") or 0),
                    "by": s.get("by"),
                    "hn_id": sid,
                },
            ))
        probe["item_count"] = len(signals)
        await record_poll_probe("hackernews", **probe)
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(list(self._seen_ids)[-2500:])
        return signals
