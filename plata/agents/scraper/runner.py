"""Scraper agent: poll all configured sources, dedup, archive to Postgres, publish to Redis stream."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from plata.agents.base import BaseAgent
from plata.agents.scraper.dedup import check_duplicate, ensure_recent_index
from plata.agents.scraper.news_config import get_config as get_news_config, should_drop
from plata.agents.scraper.sanitizer import detect_likely_injection, sanitize
from plata.agents.scraper.sources.base_source import BaseSource
from plata.agents.scraper.sources.cryptopanic import CryptoPanicSource
from plata.agents.scraper.sources.gdelt import GdeltSource
from plata.agents.scraper.sources.hackernews import HackerNewsSource
from plata.agents.scraper.sources.markets_misc import (
    DefiLlamaSource, FearGreedSource, PolymarketSource, StockTwitsSource,
)
from plata.agents.scraper.sources.reddit import RedditSource
from plata.agents.scraper.sources.rss import RssSource
from plata.agents.scraper.sources.sec_edgar import SecEdgarSource
from plata.agents.scraper.sources.market_ticker import MarketTickerSource
from plata.core.bus import Streams, publish
from plata.core.db import SignalArchive, session_scope
from plata.core.schemas import RawSignal


def all_sources() -> list[BaseSource]:
    return [
        RedditSource(), CryptoPanicSource(), GdeltSource(), RssSource(),
        MarketTickerSource(),
        # v2.24.198 — no-auth, high-signal feeds.
        SecEdgarSource(), HackerNewsSource(), StockTwitsSource(),
        PolymarketSource(), FearGreedSource(), DefiLlamaSource(),
    ]


class Scraper(BaseAgent):
    name = "scraper"

    def __init__(self) -> None:
        super().__init__()
        self._sources = all_sources()

    async def setup(self) -> None:
        # Wait for Redis to finish loading the RDB before doing anything --
        # same defense as the dashboard lifespan v2.24.207. Without this,
        # ensure_recent_index() raises BusyLoadingError during Railway's
        # post-deploy cold-start window and the agent crashes before its
        # poll loops ever start. The supervisor restarts but each restart
        # races the same window. v2.24.210 fix.
        from plata.core.bus import get_redis as _gr
        r = _gr()
        for attempt in range(15):
            try:
                await r.ping()
                break
            except Exception as exc:  # noqa: BLE001 -- BusyLoading + Connection both
                self.log.info("scraper_waiting_for_redis",
                                attempt=attempt + 1, error=str(exc)[:120])
                await asyncio.sleep(2.0)
        await ensure_recent_index()
        for src in self._sources:
            asyncio.create_task(self._poll_loop(src), name=f"scraper-{src.name}")
        self.log.info("scraper_started", sources=[s.name for s in self._sources])

    async def handle(self, payload):  # not used: Scraper produces, doesn't consume.
        return None

    async def _poll_loop(self, src: BaseSource) -> None:
        # Stagger first runs to avoid thundering herd.
        from plata.core.bus import get_redis
        redis = get_redis()
        key = f"scraper:source:{src.name}"
        await asyncio.sleep(2)
        while True:
            if self._halted.is_set():
                await redis.hset(key, mapping={"status": "halted", "halted_by": "system"})
                await asyncio.sleep(5)
                continue
            current = (await redis.hget(key, "status")) or ""
            if current == "halted":
                await asyncio.sleep(5)
                continue
            now_dt = datetime.now(timezone.utc)
            now = now_dt.isoformat()
            interval = src.poll_interval_sec
            await redis.hset(key, mapping={
                "status": "polling", "started_at": now, "interval_sec": interval,
            })
            await redis.hdel(key, "run_now")
            try:
                # Hard outer timeout so a hung source can't wedge the loop
                # forever. Reddit RSS body parsing via feedparser is
                # SYNCHRONOUS — if a malformed feed loops inside the
                # parser, the entire event loop blocks. v2.24.206 fix:
                # bound any single poll() at 90s wall and recycle on
                # timeout. Other sources keep polling normally.
                signals = await asyncio.wait_for(src.poll(), timeout=90.0)
                outcomes: dict[str, int] = {"dup": 0, "injection": 0, "published": 0}
                filtered_reasons: dict[str, int] = {}
                for s in signals:
                    r = await self._process_one(s)
                    if r.startswith("filtered:"):
                        reason = r.split(":", 2)[1] if ":" in r[len("filtered:"):] else r[len("filtered:"):]
                        # Use the leading bucket (e.g. "blocked" from "blocked:neighbor")
                        bucket = reason.split(":", 1)[0]
                        filtered_reasons[bucket] = filtered_reasons.get(bucket, 0) + 1
                    elif r in outcomes:
                        outcomes[r] += 1
                # Per-source totals + last-poll detail row pushed onto a 20-entry
                # ring so /news/?row=<n>/log shows recent activity.
                raw = len(signals)
                published = outcomes["published"]
                dup = outcomes["dup"]
                filt = sum(filtered_reasons.values())
                inj = outcomes["injection"]
                await redis.hincrby(key, "lifetime_raw", raw)
                await redis.hincrby(key, "lifetime_published", published)
                await redis.hincrby(key, "lifetime_dup", dup)
                await redis.hincrby(key, "lifetime_filtered", filt)
                await redis.hincrby(key, "lifetime_polls", 1)
                next_at = (datetime.now(timezone.utc).timestamp() + interval)
                filt_blob = ",".join(f"{k}:{v}" for k, v in filtered_reasons.items()) or ""
                await redis.hset(key, mapping={
                    "status": "idle", "last_poll_at": now,
                    "last_fetched": raw,
                    "last_published": published,
                    "last_dup": dup,
                    "last_filtered": filt,
                    "last_filtered_reasons": filt_blob,
                    "last_error": "",
                    "next_poll_at": str(int(next_at)),
                })
                # Push last-poll detail into a recent-polls ring (latest first).
                log_key = f"{key}:log"
                import json as _json
                entry = _json.dumps({
                    "ts": now,
                    "raw": raw, "published": published, "dup": dup,
                    "filtered": filt, "filtered_reasons": filtered_reasons,
                    "injection": inj,
                    "sample_titles": [(s.title or "")[:120] for s in signals[:3]],
                })
                await redis.lpush(log_key, entry)
                await redis.ltrim(log_key, 0, 19)
            except asyncio.TimeoutError:
                next_at = (datetime.now(timezone.utc).timestamp() + interval)
                await redis.hset(key, mapping={
                    "status": "error", "last_poll_at": now,
                    "last_error": "TimeoutError: poll() exceeded 90s wall budget — likely a sync parse stall (feedparser) or a slow upstream. Loop recovered and will retry on next interval.",
                    "next_poll_at": str(int(next_at)),
                })
                self.log.warning("source_poll_timeout", source=src.name)
            except Exception as e:
                next_at = (datetime.now(timezone.utc).timestamp() + interval)
                await self.error_reporter.capture_exception(
                    e, agent=self.name, severity="ERROR",
                    context={"source": src.name},
                )
                await redis.hset(key, mapping={
                    "status": "error", "last_poll_at": now,
                    "last_error": f"{type(e).__name__}: {str(e)[:200]}",
                    "next_poll_at": str(int(next_at)),
                })
            # Tick-sleep so the user's "Run now" or a config change is picked up
            # within ~2s instead of waiting up to poll_interval_sec.
            elapsed = 0
            while elapsed < interval:
                if self._halted.is_set():
                    break
                if (await redis.hget(key, "run_now")) == "1":
                    break
                if (await redis.hget(key, "status")) == "halted":
                    break
                await asyncio.sleep(2)
                elapsed += 2

    async def _process_one(self, signal: RawSignal) -> str:
        """Returns the outcome label: 'dup' | 'injection' | 'filtered:<reason>' | 'published'."""
        # Sanitize body in place so archive stores clean content too.
        clean_body = sanitize(signal.body, max_chars=8000)
        signal = signal.model_copy(update={"body": clean_body})

        # Dedup before publishing
        is_dup, master = await check_duplicate(signal)
        signal = signal.model_copy(update={"is_duplicate": is_dup, "dedup_master_ulid": master})

        # Archive to Postgres regardless
        async with session_scope() as session:
            session.add(SignalArchive(
                signal_ulid=signal.ulid,
                source=str(signal.source),
                fetched_at=datetime.now(timezone.utc),
                source_published_at=signal.source_published_at,
                url=signal.url,
                title=signal.title,
                body=signal.body,
                metadata_=dict(signal.metadata),
                is_duplicate=is_dup,
                dedup_master_ulid=master,
            ))

        if is_dup:
            self.log.debug("dedup_match", source=str(signal.source), master_ulid=master)
            return "dup"

        if detect_likely_injection(signal.body):
            await self.error_reporter.capture(
                agent=self.name, severity="WARN",
                error_type="LikelyInjection",
                message="Source content contained injection-like phrases; archived only.",
                context={"source": str(signal.source), "signal_ulid": signal.ulid},
            )
            return "injection"

        # Content filter — drop off-topic stories before they reach the LLM.
        # Market-ticker signals are deterministic price-action alerts, not
        # news — bypass the news keyword filter for them.
        try:
            news_cfg = await get_news_config()
            if str(signal.source) == "market_ticker":
                drop_reason = None
            else:
                drop_reason = should_drop(signal.title, signal.body, news_cfg)
        except Exception:  # noqa: BLE001
            drop_reason = None
        if drop_reason:
            from plata.core.bus import get_redis as _gr
            _r = _gr()
            await _r.hincrby("scraper:filter_drops", drop_reason.split(":", 1)[0], 1)
            self.log.debug("content_filter_drop", source=str(signal.source),
                           reason=drop_reason, title=(signal.title or "")[:80])
            return f"filtered:{drop_reason}"

        await publish(Streams.RAW_SIGNALS, signal)
        return "published"
