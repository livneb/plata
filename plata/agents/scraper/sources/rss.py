"""Generic RSS source — reads a list of feed URLs from news_config and polls them.

Feed list is live-editable on /settings/?tab=news. Each entry is
{name, url, enabled}.
"""
from __future__ import annotations

from datetime import datetime, timezone
from time import mktime
from typing import Any

import httpx

from plata.agents.scraper.news_config import get_config as get_news_config
from plata.agents.scraper.sources.base_source import BaseSource
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.rss")


class RssSource(BaseSource):
    name = "rss"
    poll_interval_sec = 5 * 60

    def __init__(self) -> None:
        self._seen_urls: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("rss_enabled", True):
            return []
        feeds: list[dict[str, Any]] = cfg.get("rss_feeds") or []
        if not feeds:
            return []
        try:
            import feedparser  # local import — optional dep
        except ImportError:  # pragma: no cover
            _log.warning("feedparser_not_installed")
            return []

        signals: list[RawSignal] = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True,
                                     headers={"User-Agent": "Plata/1.0 (+rss)"}) as client:
            for feed in feeds:
                if not isinstance(feed, dict):
                    continue
                if feed.get("enabled") is False:
                    continue
                url = (feed.get("url") or "").strip()
                if not url:
                    continue
                name = (feed.get("name") or url).strip()
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    parsed = feedparser.parse(r.content)
                except Exception:  # noqa: BLE001
                    _log.exception("rss_fetch_failed", url=url)
                    continue
                for entry in parsed.entries[:50]:
                    link = getattr(entry, "link", "") or ""
                    if not link or link in self._seen_urls:
                        continue
                    self._seen_urls.add(link)
                    title = getattr(entry, "title", "") or ""
                    summary = getattr(entry, "summary", "") or title
                    pub_at = None
                    pp = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
                    if pp:
                        try:
                            pub_at = datetime.fromtimestamp(mktime(pp), tz=timezone.utc)
                        except (TypeError, ValueError, OverflowError):
                            pub_at = None
                    signals.append(RawSignal(
                        source=SignalSource.RSS,
                        url=link,
                        title=title[:500] or None,
                        body=(summary or title)[:4000],
                        source_published_at=pub_at,
                        metadata={"feed_name": name, "feed_url": url},
                    ))
        if len(self._seen_urls) > 10000:
            self._seen_urls = set(list(self._seen_urls)[-5000:])
        return signals
