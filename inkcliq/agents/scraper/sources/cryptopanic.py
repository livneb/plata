"""CryptoPanic — crypto news aggregator with auto-tagged sentiment."""
from __future__ import annotations

from datetime import datetime

import httpx

from inkcliq.agents.scraper.sources.base_source import BaseSource
from inkcliq.config.settings import get_settings
from inkcliq.core.observability import get_logger
from inkcliq.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.cryptopanic")

BASE_URL = "https://cryptopanic.com/api/v1/posts/"


class CryptoPanicSource(BaseSource):
    name = "cryptopanic"
    poll_interval_sec = 120

    def __init__(self) -> None:
        settings = get_settings()
        self._key = settings.cryptopanic_api_key.get_secret_value() if settings.cryptopanic_api_key else None
        self._seen_ids: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        if not self._key:
            return []
        params = {"auth_token": self._key, "kind": "news", "public": "true"}
        signals: list[RawSignal] = []
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(BASE_URL, params=params)
            r.raise_for_status()
            data = r.json()
        for post in data.get("results", []):
            pid = str(post.get("id"))
            if pid in self._seen_ids:
                continue
            self._seen_ids.add(pid)
            title = post.get("title") or ""
            currencies = [c.get("code") for c in post.get("currencies") or [] if c.get("code")]
            votes = post.get("votes") or {}
            try:
                published = datetime.fromisoformat(post.get("published_at", "").replace("Z", "+00:00"))
            except ValueError:
                published = None
            signals.append(RawSignal(
                source=SignalSource.CRYPTOPANIC,
                url=post.get("url"),
                title=title,
                body=title,  # CryptoPanic returns titles only on free tier
                source_published_at=published,
                metadata={
                    "currencies": currencies,
                    "votes": votes,
                    "kind": post.get("kind"),
                    "domain": post.get("domain"),
                },
            ))
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(list(self._seen_ids)[-2500:])
        return signals
