"""Reddit source — fetches new posts from finance/crypto subreddits."""
from __future__ import annotations

from datetime import datetime, timezone

from plata.agents.scraper.sources.base_source import BaseSource
from plata.config.settings import get_settings
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.reddit")

SUBREDDITS = ["CryptoCurrency", "wallstreetbets", "Bitcoin", "ethfinance"]


class RedditSource(BaseSource):
    name = "reddit"
    poll_interval_sec = 60

    def __init__(self) -> None:
        settings = get_settings()
        self._cid = settings.reddit_client_id.get_secret_value() if settings.reddit_client_id else None
        self._secret = settings.reddit_client_secret.get_secret_value() if settings.reddit_client_secret else None
        self._ua = settings.reddit_user_agent
        self._seen_ids: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        if not self._cid or not self._secret:
            return []
        try:
            import asyncpraw  # local import — optional dep until enabled
        except ImportError:  # pragma: no cover
            _log.warning("asyncpraw_not_installed")
            return []

        reddit = asyncpraw.Reddit(
            client_id=self._cid,
            client_secret=self._secret,
            user_agent=self._ua,
        )
        signals: list[RawSignal] = []
        try:
            for sub_name in SUBREDDITS:
                sub = await reddit.subreddit(sub_name)
                async for post in sub.new(limit=15):
                    if post.id in self._seen_ids:
                        continue
                    self._seen_ids.add(post.id)
                    signals.append(RawSignal(
                        source=SignalSource.REDDIT,
                        url=f"https://reddit.com{post.permalink}",
                        title=post.title,
                        body=(post.selftext or post.title)[:4000],
                        source_published_at=datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
                        metadata={
                            "subreddit": sub_name,
                            "score": post.score,
                            "num_comments": post.num_comments,
                            "author": str(post.author) if post.author else None,
                            "post_id": post.id,
                        },
                    ))
        finally:
            await reddit.close()
        # Cap seen-ids growth
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(list(self._seen_ids)[-2500:])
        return signals
