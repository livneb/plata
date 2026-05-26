"""Scraper agent: poll all configured sources, dedup, archive to Postgres, publish to Redis stream."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from plata.agents.base import BaseAgent
from plata.agents.scraper.dedup import check_duplicate, ensure_recent_index
from plata.agents.scraper.sanitizer import detect_likely_injection, sanitize
from plata.agents.scraper.sources.base_source import BaseSource
from plata.agents.scraper.sources.cryptopanic import CryptoPanicSource
from plata.agents.scraper.sources.gdelt import GdeltSource
from plata.agents.scraper.sources.reddit import RedditSource
from plata.core.bus import Streams, publish
from plata.core.db import SignalArchive, session_scope
from plata.core.schemas import RawSignal


def all_sources() -> list[BaseSource]:
    return [RedditSource(), CryptoPanicSource(), GdeltSource()]


class Scraper(BaseAgent):
    name = "scraper"

    def __init__(self) -> None:
        super().__init__()
        self._sources = all_sources()

    async def setup(self) -> None:
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
                # Mark this halt as "system" so the resume action can clear it
                # automatically (vs a user-clicked cancel which stays sticky).
                await redis.hset(key, mapping={"status": "halted", "halted_by": "system"})
                await asyncio.sleep(5)
                continue
            # Per-source manual cancel from the Kanban: if the status was set to
            # "halted" by /workflow/cancel/source/<name>, honour it. The user can
            # POST it back to "idle" (or remove the field) to resume.
            current = (await redis.hget(key, "status")) or ""
            if current == "halted":
                await asyncio.sleep(5)
                continue
            now = datetime.now(timezone.utc).isoformat()
            await redis.hset(key, mapping={
                "status": "polling", "started_at": now, "interval_sec": src.poll_interval_sec,
            })
            try:
                signals = await src.poll()
                for s in signals:
                    await self._process_one(s)
                await redis.hset(key, mapping={
                    "status": "idle", "last_poll_at": now,
                    "last_fetched": len(signals), "last_error": "",
                })
            except Exception as e:
                await self.error_reporter.capture_exception(
                    e, agent=self.name, severity="ERROR",
                    context={"source": src.name},
                )
                await redis.hset(key, mapping={
                    "status": "error", "last_poll_at": now,
                    "last_error": f"{type(e).__name__}: {str(e)[:200]}",
                })
            await asyncio.sleep(src.poll_interval_sec)

    async def _process_one(self, signal: RawSignal) -> None:
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
            return

        if detect_likely_injection(signal.body):
            await self.error_reporter.capture(
                agent=self.name, severity="WARN",
                error_type="LikelyInjection",
                message="Source content contained injection-like phrases; archived only.",
                context={"source": str(signal.source), "signal_ulid": signal.ulid},
            )
            return

        await publish(Streams.RAW_SIGNALS, signal)
