"""Reddit source — no-auth JSON polling.

Previously required a Reddit OAuth app + asyncpraw. That involved creating
a developer app on reddit.com, copy-paste of client_id / client_secret,
and was an unpleasant onboarding step. Now we just hit the public
`/r/<sub>/new.json` endpoint directly — same data, no auth, just a
sane User-Agent.

Reddit will rate-limit unauthenticated requests aggressively if you spam
them. We're polite: one subreddit per HTTP call, sequential, 25 posts max,
60s default poll interval. From a Railway egress IP this works steadily.
If we ever get 429s we'll back off and skip the rest of the cycle.
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

_log = get_logger("scraper.reddit")

USER_AGENT = "plata-trading-bot/2.0 (no-auth public reads)"


class RedditSource(BaseSource):
    name = "reddit"
    poll_interval_sec = 60

    # Reddit's unauthenticated rate limit is ~10 req/min/IP. With 17+
    # subreddits at 1s spacing we burn through it and start eating 429s.
    # Cap how many subs we hit per cycle; rotate the offset across cycles
    # so every sub eventually gets polled.
    PER_CYCLE_LIMIT = 6
    INTER_REQUEST_SLEEP_SEC = 2.0

    def __init__(self) -> None:
        self._seen_ids: set[str] = set()
        self._rotation_offset = 0

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("reddit_enabled", True):
            await record_poll_probe("reddit", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        all_subs = list(cfg.get("reddit_subreddits") or NEWS_DEFAULTS["reddit_subreddits"])
        # Rotate which subs we poll each cycle — first call hits subs
        # [0:6], next [6:12], etc., wrapping at the end. Each sub gets
        # polled every (len/6) cycles instead of every cycle, keeping us
        # under Reddit's anonymous limit.
        if not all_subs:
            await record_poll_probe("reddit", item_count=0,
                                     error_message="no subreddits configured")
            return []
        n = len(all_subs)
        per_cycle = min(self.PER_CYCLE_LIMIT, n)
        start = self._rotation_offset % n
        # Slice with wrap-around.
        subreddits = (all_subs[start:start + per_cycle]
                       + all_subs[: max(0, (start + per_cycle) - n)])
        self._rotation_offset = (start + per_cycle) % n
        signals: list[RawSignal] = []
        probe_kwargs: dict = {
            "subreddits_this_cycle": ",".join(subreddits),
            "total_configured": n,
            "rotation_offset": start,
        }
        last_status: int | None = None
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=15.0,
            follow_redirects=True,
        ) as client:
            for sub_name in subreddits:
                url = f"https://www.reddit.com/r/{sub_name}/new.json?limit=25"
                try:
                    resp = await client.get(url)
                    last_status = resp.status_code
                    if resp.status_code == 429:
                        probe_kwargs["error_type"] = "RateLimited"
                        probe_kwargs["error_message"] = "429 from reddit.com — backing off this cycle"
                        break
                    if resp.status_code >= 400:
                        probe_kwargs["error_type"] = f"HTTP{resp.status_code}"
                        probe_kwargs["error_message"] = resp.text[:200]
                        continue
                    data = resp.json()
                except Exception as exc:  # noqa: BLE001
                    probe_kwargs["error_type"] = type(exc).__name__
                    probe_kwargs["error_message"] = str(exc)[:200]
                    continue
                children = (data.get("data") or {}).get("children") or []
                for child in children:
                    post = child.get("data") or {}
                    pid = post.get("id")
                    if not pid or pid in self._seen_ids:
                        continue
                    self._seen_ids.add(pid)
                    title = (post.get("title") or "").strip()
                    body = (post.get("selftext") or title)[:4000]
                    created = post.get("created_utc")
                    try:
                        ts = (datetime.fromtimestamp(float(created), tz=timezone.utc)
                              if created else datetime.now(timezone.utc))
                    except (TypeError, ValueError):
                        ts = datetime.now(timezone.utc)
                    permalink = post.get("permalink") or ""
                    signals.append(RawSignal(
                        source=SignalSource.REDDIT,
                        url=f"https://reddit.com{permalink}" if permalink else (post.get("url") or ""),
                        title=title,
                        body=body,
                        source_published_at=ts,
                        metadata={
                            "subreddit": sub_name,
                            "score": int(post.get("score") or 0),
                            "num_comments": int(post.get("num_comments") or 0),
                            "author": post.get("author"),
                            "post_id": pid,
                        },
                    ))
                # 2s between subreddits keeps us safely under Reddit's
                # ~10 req/min unauthenticated cap with margin for the page
                # load itself.
                await asyncio.sleep(self.INTER_REQUEST_SLEEP_SEC)
        probe_kwargs["item_count"] = len(signals)
        if last_status is not None:
            probe_kwargs["http_status"] = last_status
        await record_poll_probe("reddit", **probe_kwargs)
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(list(self._seen_ids)[-2500:])
        return signals
