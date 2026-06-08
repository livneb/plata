"""Historian — one-shot synthetic seeding of the graph with ~1000 historical events."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from typing import Any

from plata.core.embeddings import embed
from plata.core.graph import attach_price_impact, ensure_indexes, upsert_event
from plata.core.llm import LLMClient
from plata.core.observability import get_logger
from plata.core.schemas import EntityRef, EntityType, SignalSource
from plata.core.ulid import new_ulid
from plata.oracle.price_oracle import compute_and_store
from plata.oracle.symbol_mapping import symbols_for_entities

_log = get_logger("historian")

BATCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["events"],
    "properties": {
        "events": {
            "type": "array",
            "minItems": 1,
            "maxItems": 15,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["date", "region", "category", "affected_assets", "narrative"],
                "properties": {
                    "date": {"type": "string", "description": "YYYY-MM-DD"},
                    "region": {"type": "string"},
                    "category": {"type": "string"},
                    "affected_assets": {"type": "array", "items": {"type": "string"}},
                    "narrative": {"type": "string", "minLength": 30, "maxLength": 600},
                },
            },
        }
    },
}

SYSTEM = """You are a financial historian. List dramatic events that
moved markets (wars, financial crises, central bank surprises, hacks, regulation).
Output JSON only, matching the schema."""


async def seed(
    total_events: int = 1000,
    batch_size: int = 10,
    start_date: str = "2005-01-01",
    end_date: str = "2025-12-31",
    brief: str = "",
    focus: str = "",
    resume: bool = False,
) -> None:
    """Seed historical events.

    - resume=True: continue a previously-interrupted run. Skips batches we've already
      completed (tracked in `historian:status.next_batch`) and keeps the written counter.
    """
    from plata.core.bus import get_redis
    redis = get_redis()
    status_key = "historian:status"

    start_batch = 0
    if resume:
        existing = await redis.hgetall(status_key)
        try:
            start_batch = int(existing.get("next_batch") or 0)
            prior_written = int(existing.get("written") or 0)
            prior_failed = int(existing.get("failed_batches") or 0)
        except (TypeError, ValueError):
            start_batch, prior_written, prior_failed = 0, 0, 0
        print(f"[historian] resuming from batch {start_batch} (already written {prior_written})", flush=True)
    else:
        prior_written = 0
        prior_failed = 0

    await redis.hset(status_key, mapping={
        "state": "running",
        "started_at": datetime.utcnow().isoformat(),
        "last_progress_at": datetime.utcnow().isoformat(),
        "total_target": total_events,
        "batch_size": batch_size,
        "start_date": start_date,
        "end_date": end_date,
        "brief": brief[:240],
        "focus": focus[:240],
        "written": prior_written,
        "failed_batches": prior_failed,
        "next_batch": start_batch,
        "last_event_date": "" if not resume else (existing.get("last_event_date") or ""),
        "last_event_category": "" if not resume else (existing.get("last_event_category") or ""),
        "last_error": "",
        "resumed_count": int((existing.get("resumed_count") or 0)) + (1 if resume else 0)
                          if resume else 0,
    })
    try:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
    except Exception:
        await redis.hset(status_key, mapping={"state": "failed", "last_error": "invalid date"})
        return

    await ensure_indexes()
    llm = LLMClient("historian")
    batches = total_events // batch_size
    written = prior_written
    failed = prior_failed

    # Build the steering paragraph once.
    steering = []
    if brief.strip():
        steering.append(f"RESEARCH BRIEF (in user's own words; translate if needed):\n{brief.strip()}")
    if focus.strip():
        steering.append(f"FOCUS ASSETS / TOPICS: {focus.strip()}")
    steering_block = "\n\n".join(steering) + ("\n\n" if steering else "")

    for i in range(start_batch, batches):
        # Record where we are so a resume can pick up here after a restart.
        await redis.hset(status_key, "next_batch", i)
        # Heartbeat at the start of every batch so dashboards can tell apart
        # "actively working" from "process died mid-flight".
        await redis.hset(status_key, "last_progress_at", datetime.utcnow().isoformat())
        # Per-batch card visible in the Kanban (capped TTL so it disappears once stale).
        batch_key = f"historian:batch:{i}"
        await redis.hset(batch_key, mapping={
            "i": i, "total_batches": batches, "size": batch_size,
            "state": "running", "started_at": datetime.utcnow().isoformat(),
            "events_in_batch": 0,
        })
        await redis.expire(batch_key, 3600)
        prompt = (
            f"{steering_block}"
            f"Generate batch #{i+1} of {batches}. {batch_size} unique events not in any prior batch.\n"
            f"All event dates MUST fall between {start_date} and {end_date} inclusive.\n"
            f"Cover varied dates within that window. Rank by market impact (largest first). "
            f"Be specific (named entities, real dates). If a research brief is given, every event "
            f"should be relevant to it; otherwise return the largest market-moving events of the window."
        )
        try:
            data = await llm.structured(
                messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt}],
                schema=BATCH_SCHEMA, schema_name="historian_batch",
            )
        except Exception as exc:
            _log.exception("historian_batch_failed", batch=i)
            failed += 1
            try:
                from plata.core.error_reporter import get_error_reporter
                await get_error_reporter().capture_exception(
                    exc, agent="historian", severity="ERROR",
                    context={"batch": i, "phase": "llm_batch"},
                )
            except Exception:  # noqa: BLE001
                pass
            await redis.hset(status_key, mapping={
                "failed_batches": failed,
                "last_error": f"batch {i}: {type(exc).__name__}: {str(exc)[:160]}",
            })
            await redis.hset(batch_key, mapping={
                "state": "failed",
                "last_error": f"{type(exc).__name__}: {str(exc)[:160]}",
                "finished_at": datetime.utcnow().isoformat(),
            })
            continue
        for ev in data.get("events", []):
            try:
                ts = datetime.fromisoformat(ev["date"])
            except Exception:
                continue
            # Drop events outside the requested window (LLM sometimes ignores instructions).
            if ts < start_dt or ts > end_dt:
                continue
            event_ulid = new_ulid()
            embedding = await embed(ev["narrative"], input_type="document")
            entity_refs = [
                EntityRef(type=EntityType.TICKER, id=a.upper(), name=a, sentiment=-0.2)
                for a in ev.get("affected_assets", [])[:10]
            ]
            await upsert_event(
                ulid=event_ulid,
                summary=ev["narrative"],
                embedding=embedding,
                source=SignalSource.HISTORIAN.value,
                category=ev.get("category", "macro"),
                ts=ts,
                entity_refs=[r.model_dump() for r in entity_refs],
                extra={"region": ev.get("region")},
            )

            symbols = symbols_for_entities(entity_refs)
            for sym in symbols[:3]:
                try:
                    metrics = await compute_and_store(
                        event_ulid=event_ulid, symbol=sym, event_ts=ts
                    )
                    if metrics:
                        await attach_price_impact(event_ulid, sym, metrics)
                except Exception:
                    _log.exception("historian_oracle_failed", ulid=event_ulid, symbol=sym)
            written += 1
            await redis.hset(status_key, mapping={
                "written": written,
                "last_event_date": ev.get("date") or "",
                "last_event_category": ev.get("category") or "",
                "last_progress_at": datetime.utcnow().isoformat(),
            })
            await redis.hincrby(batch_key, "events_in_batch", 1)
            await redis.hset(batch_key, "last_event_date", ev.get("date") or "")
            # Surface this event as a transient sub-card on the workflow Kanban.
            try:
                live_entry = json.dumps({
                    "ulid": event_ulid,
                    "summary": (ev.get("narrative") or "")[:120],
                    "category": ev.get("category"),
                    "date": ev.get("date"),
                    "batch_i": i,
                    "ts": datetime.utcnow().isoformat(),
                })
                await redis.lpush("historian:events_live", live_entry)
                await redis.ltrim("historian:events_live", 0, 29)
                await redis.expire("historian:events_live", 90)
            except Exception:  # noqa: BLE001
                pass
        await redis.hset(batch_key, mapping={
            "state": "done",
            "finished_at": datetime.utcnow().isoformat(),
        })
        # Mark completion of this batch so resume starts from the next one.
        await redis.hset(status_key, "next_batch", i + 1)
        _log.info("historian_batch_done", batch=i, written=written)
    await redis.hset(status_key, mapping={
        "state": "done",
        "finished_at": datetime.utcnow().isoformat(),
        "written": written,
        "failed_batches": failed,
    })
    _log.info("historian_seed_complete", total=written)


# ---------------------------------------------------------------------------
# Live consumer: HistorianResearchAgent
#
# Listens on Streams.HISTORIAN_RESEARCH_REQUESTS. Fired by the strategist
# when a high-sentiment event is dropped due to weak/missing historical
# analogs. The agent asks the LLM for ~6 real similar past events, ingests
# each into the graph (so they become embed-searchable), and re-publishes
# the original event back to ENRICHED_EVENTS with `re_research_done=True`
# so the strategist re-evaluates with the now-richer analog pool.
# ---------------------------------------------------------------------------

RESEARCH_BATCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["events"],
    "properties": {
        "events": {
            "type": "array",
            "minItems": 1,
            "maxItems": 8,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["date", "category", "narrative", "affected_assets"],
                "properties": {
                    "date": {"type": "string", "description": "YYYY-MM-DD"},
                    "category": {"type": "string"},
                    "affected_assets": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
                    "narrative": {"type": "string", "minLength": 40, "maxLength": 600},
                    "observed_impact": {"type": "string", "maxLength": 300,
                                          "description": "What happened to the named assets in the days/weeks after."},
                },
            },
        },
    },
}

RESEARCH_SYSTEM = """You are a financial historian.

You will be given a recent event whose strategic implications are unclear because the
similarity search produced no concrete historical analogs with observable price data.

Your job: enumerate up to 8 REAL past events from the lookback window that are
structurally similar to the current event, and describe each one's observable market
impact. Be specific (real dates, named entities, accurate factual claims). Do not
fabricate events. If you can't think of strong analogs, return fewer events rather
than padding with weak ones.
Output JSON only, matching the schema."""


async def _should_trigger_research(
    *, sentiment_magnitude: float, top_analog_similarity: float, category: str,
    drop_reason: str | None,
) -> bool:
    """Conservative trigger: only fire on high-sentiment events whose top
    analog is weak AND whose drop reasoning indicates lack of evidence.
    All thresholds live in risk_config so they're tunable from /settings/."""
    from plata.core.bus import get_redis
    try:
        cfg = await get_redis().hgetall("risk_config") or {}
        min_sent = float(cfg.get("research_min_sentiment") or 0.6)
        max_top = float(cfg.get("research_max_analog_score") or 0.85)
    except Exception:  # noqa: BLE001
        min_sent, max_top = 0.6, 0.85
    if sentiment_magnitude < min_sent:
        return False
    if top_analog_similarity >= max_top:
        # We already have a strong analog; deeper research unlikely to help.
        return False
    # Heuristic on the LLM's drop reasoning — it tends to say one of these
    # when the issue is genuinely lack of evidence (rather than e.g.
    # "low conviction because direction is ambiguous").
    if not drop_reason:
        return True  # missing reasoning → assume worth researching
    low = drop_reason.lower()
    return any(s in low for s in (
        "no historical analog", "no analog", "insufficient evidence",
        "no concrete", "lack of", "no clear precedent",
        "no relevant historical", "no comparable",
    ))


async def _research_daily_count() -> int:
    from datetime import date as _date
    from plata.core.bus import get_redis
    try:
        v = await get_redis().get(f"historian:research:daily:{_date.today().isoformat()}")
        return int(v or 0)
    except Exception:  # noqa: BLE001
        return 0


async def _research_daily_cap() -> int:
    from plata.core.bus import get_redis
    try:
        cfg = await get_redis().hgetall("risk_config") or {}
        return int(cfg.get("research_max_per_day") or 20)
    except Exception:  # noqa: BLE001
        return 20


class HistorianResearchAgent:
    """Consumer-style agent for follow-up research requests.

    Not a `BaseAgent` subclass because that ties in to the workflow-Kanban
    activity stream — this is a side-band researcher, not part of the main
    proposal pipeline. Spawned from `entrypoints._run_intelligence_sandbox`.
    """

    name = "historian"
    input_stream: str  # filled at runtime from Streams.HISTORIAN_RESEARCH_REQUESTS
    group = "historian-research-grp"

    def __init__(self) -> None:
        from plata.core.bus import Streams
        from plata.core.observability import get_logger as _gl
        self.input_stream = Streams.HISTORIAN_RESEARCH_REQUESTS
        self.log = _gl(self.name)
        self._llm = LLMClient(self.name)

    async def run(self) -> None:
        from plata.core.bus import consume, ack, ensure_consumer_group
        await ensure_indexes()
        await ensure_consumer_group(self.input_stream, self.group)
        consumer_name = f"{self.name}-{os.environ.get('HOSTNAME', 'local')}"
        async for msg in consume(self.input_stream, self.group, consumer_name):
            try:
                await self.handle(msg.payload)
            except Exception as exc:  # noqa: BLE001
                self.log.exception("historian_research_failed", error=str(exc)[:200])
                try:
                    from plata.core.error_reporter import get_error_reporter
                    await get_error_reporter().capture_exception(
                        exc, agent=self.name, severity="ERROR",
                        context={"redis_id": msg.redis_id, "phase": "research"},
                    )
                except Exception:  # noqa: BLE001
                    pass
            finally:
                await ack(self.input_stream, self.group, msg.redis_id)

    async def handle(self, payload: dict[str, Any]) -> None:
        from plata.core.bus import get_redis, publish, Streams
        from plata.core.schemas import EnrichedEvent
        redis = get_redis()
        # Daily cap.
        today_count = await _research_daily_count()
        cap = await _research_daily_cap()
        if today_count >= cap:
            self.log.info("historian_research_skipped_daily_cap",
                          count=today_count, cap=cap)
            return
        req_ulid = payload.get("triggering_event_ulid")
        summary = payload.get("summary") or ""
        category = payload.get("category") or "other"
        lookback_years = int(payload.get("lookback_years") or 5)
        if not req_ulid or not summary:
            return
        # Idempotency: don't research the same event twice within 7 days.
        seen_key = f"historian:research:seen:{req_ulid}"
        if await redis.exists(seen_key):
            self.log.info("historian_research_already_done", ulid=req_ulid)
            return
        await redis.setex(seen_key, 7 * 24 * 3600, "1")
        # Bump daily counter.
        from datetime import date as _date
        await redis.incr(f"historian:research:daily:{_date.today().isoformat()}")
        await redis.expire(f"historian:research:daily:{_date.today().isoformat()}",
                            48 * 3600)

        prompt = (
            f"CURRENT EVENT (no strong analogs found):\n"
            f"category: {category}\n"
            f"summary: {summary}\n\n"
            f"LOOKBACK: last {lookback_years} years.\n\n"
            f"Enumerate up to 8 real past events from this window that are structurally "
            f"similar (same category / mechanism / asset class). For each, give the date, "
            f"category, the directly affected tickers / instruments, a 1-3 sentence "
            f"narrative, and what was observed in price action after."
        )
        try:
            data = await self._llm.structured(
                messages=[
                    {"role": "system", "content": RESEARCH_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                schema=RESEARCH_BATCH_SCHEMA,
                schema_name="historian_research_batch",
                metadata={"triggering_event_ulid": req_ulid},
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning("historian_research_llm_failed",
                             ulid=req_ulid, error=str(exc)[:200])
            return

        new_events = data.get("events") or []
        ingested = 0
        for ev in new_events:
            try:
                ts = datetime.fromisoformat(ev["date"])
            except Exception:
                continue
            event_ulid = new_ulid()
            try:
                embedding = await embed(ev["narrative"], input_type="document")
            except Exception as exc:  # noqa: BLE001
                self.log.warning("historian_research_embed_failed",
                                 error=str(exc)[:160])
                continue
            entity_refs = [
                EntityRef(type=EntityType.TICKER, id=str(a).upper(), name=str(a),
                          sentiment=-0.2)
                for a in (ev.get("affected_assets") or [])[:8]
            ]
            try:
                await upsert_event(
                    ulid=event_ulid,
                    summary=ev["narrative"],
                    embedding=embedding,
                    source=SignalSource.HISTORIAN.value,
                    category=ev.get("category", category),
                    ts=ts,
                    entity_refs=[r.model_dump() for r in entity_refs],
                    extra={
                        "research_for_ulid": req_ulid,
                        "observed_impact": ev.get("observed_impact"),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self.log.warning("historian_research_upsert_failed",
                                 error=str(exc)[:160])
                continue
            # Best-effort price-impact backfill for the most-relevant symbol.
            symbols = symbols_for_entities(entity_refs)
            for sym in symbols[:2]:
                try:
                    metrics = await compute_and_store(
                        event_ulid=event_ulid, symbol=sym, event_ts=ts
                    )
                    if metrics:
                        await attach_price_impact(event_ulid, sym, metrics)
                except Exception:  # noqa: BLE001
                    pass
            ingested += 1

        self.log.info("historian_research_done",
                      ulid=req_ulid, ingested=ingested, total=len(new_events))

        # Re-publish the original event with re_research_done=True so the
        # strategist re-evaluates with the now-richer analog pool. Pull the
        # full original event from the graph so we don't lose fields.
        try:
            from plata.core.graph import get_event
            doc = await get_event(req_ulid) or {}
        except Exception:  # noqa: BLE001
            doc = {}
        try:
            replay = EnrichedEvent(
                source_signal_ulid=doc.get("source_signal_ulid") or req_ulid,
                source=doc.get("source") or SignalSource.HISTORIAN.value,
                summary=doc.get("summary") or summary,
                category=doc.get("category") or category,
                sentiment_magnitude=float(doc.get("sentiment_magnitude")
                                           or payload.get("sentiment_magnitude") or 0.6),
                entities=[],  # entities aren't required for re-eval
                re_research_done=True,
            )
            await publish(Streams.ENRICHED_EVENTS, replay)
            self.log.info("historian_research_re_published",
                          original_ulid=req_ulid, new_ulid=replay.ulid)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("historian_research_re_publish_failed",
                             error=str(exc)[:200])


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(seed())
