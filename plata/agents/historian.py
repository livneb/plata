"""Historian — one-shot synthetic seeding of the graph with ~1000 historical events."""
from __future__ import annotations

import asyncio
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
) -> None:
    """Seed historical events.

    - start_date / end_date: ISO YYYY-MM-DD inclusive bounds for event dates.
    - brief: optional free-text research direction (any language). Steers what the LLM looks for.
    - focus: optional comma-separated assets/tickers/topics to bias the output toward.
    """
    from plata.core.bus import get_redis
    redis = get_redis()
    status_key = "historian:status"
    await redis.hset(status_key, mapping={
        "state": "running",
        "started_at": datetime.utcnow().isoformat(),
        "total_target": total_events,
        "batch_size": batch_size,
        "start_date": start_date,
        "end_date": end_date,
        "brief": brief[:240],
        "focus": focus[:240],
        "written": 0,
        "failed_batches": 0,
        "last_event_date": "",
        "last_event_category": "",
        "last_error": "",
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
    written = 0
    failed = 0

    # Build the steering paragraph once.
    steering = []
    if brief.strip():
        steering.append(f"RESEARCH BRIEF (in user's own words; translate if needed):\n{brief.strip()}")
    if focus.strip():
        steering.append(f"FOCUS ASSETS / TOPICS: {focus.strip()}")
    steering_block = "\n\n".join(steering) + ("\n\n" if steering else "")

    for i in range(batches):
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
        await redis.hset(batch_key, mapping={
            "state": "done",
            "finished_at": datetime.utcnow().isoformat(),
        })
        _log.info("historian_batch_done", batch=i, written=written)
    await redis.hset(status_key, mapping={
        "state": "done",
        "finished_at": datetime.utcnow().isoformat(),
        "written": written,
        "failed_batches": failed,
    })
    _log.info("historian_seed_complete", total=written)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(seed())
