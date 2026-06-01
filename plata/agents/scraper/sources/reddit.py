"""Reddit source — fetches new posts from finance/crypto subreddits."""
from __future__ import annotations

from datetime import datetime, timezone

from plata.agents.scraper.news_config import (
    DEFAULTS as NEWS_DEFAULTS,
    get_config as get_news_config,
    record_poll_probe,
)
from plata.agents.scraper.sources.base_source import BaseSource
from plata.config.settings import get_settings
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.reddit")


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
            await record_poll_probe("reddit", error_type="MissingCredentials",
                                     error_message="reddit_client_id and/or reddit_client_secret are not set in /settings/?tab=api")
            return []
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("reddit_enabled", True):
            await record_poll_probe("reddit", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        subreddits = cfg.get("reddit_subreddits") or NEWS_DEFAULTS["reddit_subreddits"]
        try:
            import asyncpraw  # local import — optional dep until enabled
        except ImportError:  # pragma: no cover
            _log.warning("asyncpraw_not_installed")
            await record_poll_probe("reddit", error_type="ImportError",
                                     error_message="asyncpraw is not installed")
            return []

        reddit = asyncpraw.Reddit(
            client_id=self._cid,
            client_secret=self._secret,
            user_agent=self._ua,
        )
        signals: list[RawSignal] = []
        probe_kwargs: dict = {"subreddits": ",".join(subreddits)}
        try:
            for sub_name in subreddits:
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
        except Exception as exc:  # noqa: BLE001
            probe_kwargs["error_type"] = type(exc).__name__
            probe_kwargs["error_message"] = str(exc)[:240]
            await record_poll_probe("reddit", **probe_kwargs)
            await reddit.close()
            raise
        else:
            probe_kwargs["item_count"] = len(signals)
            await record_poll_probe("reddit", **probe_kwargs)
        finally:
            try:
                await reddit.close()
            except Exception:  # noqa: BLE001
                pass
        # Cap seen-ids growth
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(list(self._seen_ids)[-2500:])
        return signals
