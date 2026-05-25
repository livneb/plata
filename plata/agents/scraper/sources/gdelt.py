"""GDELT 2.0 Document API — global news with built-in tone scoring. Keyless."""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

from plata.agents.scraper.sources.base_source import BaseSource
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.gdelt")

# GDELT 2.0 Doc API
BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
QUERY = 'sourcelang:eng (war OR sanction OR cyberattack OR "central bank" OR inflation OR crypto OR bitcoin OR regulation)'


class GdeltSource(BaseSource):
    name = "gdelt"
    poll_interval_sec = 15 * 60  # GDELT updates every 15 min

    def __init__(self) -> None:
        self._seen_urls: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        params = {
            "query": QUERY,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": "75",
            "sort": "datedesc",
        }
        signals: list[RawSignal] = []
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(BASE_URL, params=params)
                r.raise_for_status()
                data = r.json()
        except Exception:
            _log.exception("gdelt_fetch_failed")
            return []
        for art in data.get("articles", []):
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
