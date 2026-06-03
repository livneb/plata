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
    # GDELT updates every 15 min, but enforces a 5s minimum BETWEEN REQUESTS
    # at the IP level. On shared egress (Railway) other tenants may exhaust
    # the budget — 30 min keeps us a good citizen and below 429 territory.
    poll_interval_sec = 30 * 60

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
        # GDELT enforces a 5s minimum between requests across the *whole IP*.
        # On shared infra (Railway egress IP), other tenants hitting GDELT mean
        # we see frequent 429s. When we hit one, back off long enough that the
        # next scheduled poll has a fresh budget.
        redis = get_redis() if False else None  # placeholder, set below
        from plata.core.bus import get_redis as _gr
        redis = _gr()
        if await redis.exists("gdelt:backoff_until"):
            probe_kwargs["error_type"] = "BackingOff"
            probe_kwargs["error_message"] = "GDELT 429'd recently; skipping this poll to let the rate-limit window clear."
            await record_poll_probe("gdelt", **probe_kwargs)
            return []
        try:
            async with httpx.AsyncClient(
                timeout=30,
                headers={
                    # Identify ourselves so GDELT can rate-limit per-app instead
                    # of per-IP if/when they support it.
                    "User-Agent": "Plata/1.0 (+https://github.com/livneb/plata)",
                }
            ) as client:
                r = await client.get(BASE_URL, params=params)
                probe_kwargs["http_status"] = r.status_code
                probe_kwargs["final_url"] = str(r.url)
                probe_kwargs["response_size"] = len(r.content or b"")
                probe_kwargs["sample"] = (r.text or "")[:300]
                if r.status_code == 429:
                    # Treat as soft failure; back off for 10 min.
                    await redis.set("gdelt:backoff_until", "1", ex=10 * 60)
                    probe_kwargs["error_type"] = "RateLimited"
                    probe_kwargs["error_message"] = "GDELT returned 429 (IP-level limit). Backing off 10 min."
                    await record_poll_probe("gdelt", **probe_kwargs)
                    return []
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
