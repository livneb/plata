"""Reddit source — no-auth JSON polling with RSS fallback.

v2.24.198 switched off OAuth/asyncpraw to public JSON. v2.24.201 added
rotation + slower spacing to dodge 429 rate limits. v2.24.202 now
hits old.reddit.com (anti-bot is laxer than www.) with browser-like
headers, and falls back to the per-subreddit RSS endpoint when JSON
returns 403 — Reddit's WAF blocks JSON from cloud egress IPs but
generally lets the RSS through because it's served as a public web
standard.
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

# Descriptive UA gets us further than a generic "python-requests/x.x".
# Reddit's docs ask for "<platform>:<app>:<version> (by /u/<username>)" —
# we use a research-bot variant since we're not associated with a
# specific Reddit account.
USER_AGENT = ("Mozilla/5.0 (compatible; plata-research-bot/2.0; "
              "+contact: ops@plata.local)")

# Headers Reddit's WAF expects to see — without these the 403 rate
# goes way up. Mirroring a real browser tilts the heuristic our way.
BROWSER_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "DNT": "1",
}


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
        # `subreddits` is the field the /news/ diagnostics card renders;
        # `total_configured` + `rotation_offset` get appended so the
        # operator can see "polled 6 of 17, offset 12" at a glance.
        probe_kwargs: dict = {
            "subreddits": (",".join(subreddits)
                           + f"  ({len(subreddits)}/{n}, offset {start})"),
        }
        last_status: int | None = None
        fallback_used = 0
        async with httpx.AsyncClient(
            headers=BROWSER_HEADERS,
            timeout=15.0,
            follow_redirects=True,
        ) as client:
            for sub_name in subreddits:
                # 1. Try old.reddit.com JSON first — anti-bot WAF is laxer
                #    here than on the modern www. endpoint.
                items, status, err = await self._fetch_json(client, sub_name)
                last_status = status
                # 2. If JSON 403s (cloud-egress IP block), fall back to
                #    the per-subreddit RSS feed. RSS is served by a
                #    different code path on Reddit's side and rarely
                #    blocked. Successfully recovered fetches DON'T break
                #    the cycle; they just feed RawSignals normally.
                if status == 403:
                    rss_items = await self._fetch_rss(client, sub_name)
                    if rss_items:
                        items = rss_items
                        fallback_used += 1
                        last_status = 200  # the RSS call succeeded
                    else:
                        # Avoid stuffing the 30KB HTML body into
                        # error_message — operator can't read it on the
                        # tiny news-page chip. Concise summary instead.
                        probe_kwargs["error_type"] = "HTTP403"
                        probe_kwargs["error_message"] = (
                            f"r/{sub_name}: JSON blocked by Reddit WAF "
                            "(IP or UA fingerprint). RSS fallback also "
                            "failed — likely the same block. Try "
                            "reducing reddit_subreddits count or wait "
                            "for the cloudflare cooldown.")
                        continue
                elif status == 429:
                    probe_kwargs["error_type"] = "RateLimited"
                    probe_kwargs["error_message"] = (
                        "429 — backing off this cycle "
                        f"(sub={sub_name})")
                    break
                elif status >= 400:
                    probe_kwargs["error_type"] = f"HTTP{status}"
                    probe_kwargs["error_message"] = (err or "")[:200]
                    continue
                for item in items:
                    pid = item["id"]
                    if pid in self._seen_ids:
                        continue
                    self._seen_ids.add(pid)
                    signals.append(RawSignal(
                        source=SignalSource.REDDIT,
                        url=item["url"],
                        title=item["title"],
                        body=item["body"],
                        source_published_at=item["ts"],
                        metadata={
                            "subreddit": sub_name,
                            "score": item.get("score", 0),
                            "num_comments": item.get("num_comments", 0),
                            "author": item.get("author"),
                            "post_id": pid,
                            "via": item.get("via", "json"),
                        },
                    ))
                await asyncio.sleep(self.INTER_REQUEST_SLEEP_SEC)
        probe_kwargs["item_count"] = len(signals)
        if last_status is not None:
            probe_kwargs["http_status"] = last_status
        if fallback_used:
            probe_kwargs["rss_fallback_used"] = fallback_used
        await record_poll_probe("reddit", **probe_kwargs)
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(list(self._seen_ids)[-2500:])
        return signals

    async def _fetch_json(self, client: httpx.AsyncClient, sub: str
                          ) -> tuple[list[dict], int, str | None]:
        """Try old.reddit.com JSON. Returns (items, http_status, error_body)."""
        url = f"https://old.reddit.com/r/{sub}/new/.json?limit=25"
        try:
            resp = await client.get(url)
        except Exception as exc:  # noqa: BLE001
            return [], 0, f"{type(exc).__name__}: {str(exc)[:160]}"
        if resp.status_code != 200:
            return [], resp.status_code, resp.text[:200]
        try:
            data = resp.json()
        except Exception:  # noqa: BLE001
            return [], resp.status_code, "non-JSON response"
        items: list[dict] = []
        for child in (data.get("data") or {}).get("children") or []:
            post = child.get("data") or {}
            pid = post.get("id")
            if not pid:
                continue
            created = post.get("created_utc")
            try:
                ts = (datetime.fromtimestamp(float(created), tz=timezone.utc)
                      if created else datetime.now(timezone.utc))
            except (TypeError, ValueError):
                ts = datetime.now(timezone.utc)
            title = (post.get("title") or "").strip()
            permalink = post.get("permalink") or ""
            items.append({
                "id": pid,
                "url": f"https://reddit.com{permalink}" if permalink else (post.get("url") or ""),
                "title": title,
                "body": (post.get("selftext") or title)[:4000],
                "ts": ts,
                "score": int(post.get("score") or 0),
                "num_comments": int(post.get("num_comments") or 0),
                "author": post.get("author"),
                "via": "json",
            })
        return items, resp.status_code, None

    async def _fetch_rss(self, client: httpx.AsyncClient, sub: str
                          ) -> list[dict]:
        """Per-subreddit RSS fallback. Used when JSON returned 403.
        Reddit's RSS endpoint is served by a different code path and
        is generally not blocked. Parsed with feedparser (already a
        dep)."""
        try:
            import feedparser
        except ImportError:
            return []
        url = f"https://www.reddit.com/r/{sub}/new/.rss"
        try:
            resp = await client.get(url)
        except Exception:  # noqa: BLE001
            return []
        if resp.status_code != 200:
            return []
        feed = feedparser.parse(resp.text)
        items: list[dict] = []
        for entry in feed.entries:
            eid = entry.get("id") or entry.get("link") or ""
            # Reddit RSS IDs look like "t3_xxxxxx" — strip prefix to match
            # the JSON `id` field so the dedup set is consistent across paths.
            if eid.startswith("t3_"):
                pid = eid[3:]
            elif "/comments/" in eid:
                pid = eid.split("/comments/")[1].split("/")[0]
            else:
                pid = eid
            if not pid:
                continue
            ts = datetime.now(timezone.utc)
            if entry.get("published_parsed"):
                ts = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            elif entry.get("updated_parsed"):
                ts = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            title = (entry.get("title") or "").strip()
            link = entry.get("link") or ""
            # RSS summary is the post body HTML wrapped in <div>. Strip
            # tags lightly — feedparser doesn't expose the raw selftext.
            summary = entry.get("summary") or title
            items.append({
                "id": pid,
                "url": link,
                "title": title,
                "body": summary[:4000],
                "ts": ts,
                "author": entry.get("author"),
                "via": "rss",
            })
        return items
