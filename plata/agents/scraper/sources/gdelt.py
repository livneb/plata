"""GDELT 2.0 Document API — global news with built-in tone scoring. Keyless."""
from __future__ import annotations

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

_log = get_logger("scraper.gdelt")

# GDELT 2.0 Doc API. Query string is live-editable via /settings/?tab=news.
BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


class GdeltSource(BaseSource):
    name = "gdelt"
    poll_interval_sec = 15 * 60  # GDELT updates every 15 min

    def __init__(self) -> None:
        self._seen_urls: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("gdelt_enabled", True):
            return []
        query = cfg.get("gdelt_query") or NEWS_DEFAULTS["gdelt_query"]
        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": "75",
            "sort": "datedesc",
        }
        signals: list[RawSignal] = []
        probe_kwargs: dict = {"url": BASE_URL, "query": query[:200]}
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(BASE_URL, params=params)
                probe_kwargs["http_status"] = r.status_code
                probe_kwargs["final_url"] = str(r.url)
                probe_kwargs["response_size"] = len(r.content or b"")
                probe_kwargs["sample"] = (r.text or "")[:300]
                r.raise_for_status()
                data = r.json()
        except Exception as exc:
            _log.exception("gdelt_fetch_failed")
            probe_kwargs["error_type"] = type(exc).__name__
            probe_kwargs["error_message"] = str(exc)[:240]
            await record_poll_probe("gdelt", **probe_kwargs)
            return []
        articles = data.get("articles") or []
        probe_kwargs["item_count"] = len(articles)
        await record_poll_probe("gdelt", **probe_kwargs)
        for art in articles:
            url = art.get("url")
            if not url or url in self._seen_urls:
                continue
            self._seen_urls.add(url)
            tone = art.get("tone")
            try:
                seen_at = datetime.strptime(art.get("seendate", ""), "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                seen_at = None
            signals.append(RawSignal(
                source=SignalSource.GDELT,
                url=url,
                title=art.get("title"),
                body=art.get("title") or url,
                source_published_at=seen_at,
                metadata={
                    "domain": art.get("domain"),
                    "language": art.get("language"),
                    "tone": tone,
                    "sourcecountry": art.get("sourcecountry"),
                    "socialimage": art.get("socialimage"),
                },
            ))
        if len(self._seen_urls) > 10000:
            self._seen_urls = set(list(self._seen_urls)[-5000:])
        return signals
