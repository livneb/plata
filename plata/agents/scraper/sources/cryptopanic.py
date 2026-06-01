"""CryptoPanic — crypto news aggregator with auto-tagged sentiment."""
from __future__ import annotations

from datetime import datetime

import httpx

from plata.agents.scraper.news_config import record_poll_probe
from plata.agents.scraper.sources.base_source import BaseSource
from plata.config.settings import get_settings
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

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
            await record_poll_probe("cryptopanic", error_type="MissingApiKey",
                                     error_message="cryptopanic_api_key is not set — free CryptoPanic still requires an auth_token from cryptopanic.com",
                                     url=BASE_URL)
            return []
        params = {"auth_token": self._key, "kind": "news", "public": "true"}
        signals: list[RawSignal] = []
        probe_kwargs: dict = {"url": BASE_URL}
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
            _log.exception("cryptopanic_fetch_failed")
            probe_kwargs["error_type"] = type(exc).__name__
            probe_kwargs["error_message"] = str(exc)[:240]
            await record_poll_probe("cryptopanic", **probe_kwargs)
            return []
        results = data.get("results") or []
        probe_kwargs["item_count"] = len(results)
        await record_poll_probe("cryptopanic", **probe_kwargs)
        for post in results:
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
