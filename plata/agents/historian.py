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
    start_year: int = 2005,
    end_year: int = 2025,
) -> None:
    from plata.core.bus import get_redis  # local: avoid cycle at import
    redis = get_redis()
    status_key = "historian:status"
    await redis.hset(status_key, mapping={
        "state": "running",
        "started_at": datetime.utcnow().isoformat(),
        "total_target": total_events,
        "batch_size": batch_size,
        "start_year": start_year,
        "end_year": end_year,
        "written": 0,
        "failed_batches": 0,
        "last_event_date": "",
        "last_event_category": "",
        "last_error": "",
    })
    await ensure_indexes()
    llm = LLMClient("historian")
    batches = total_events // batch_size
    written = 0
    failed = 0
    for i in range(batches):
        prompt = (
            f"Generate batch #{i+1} of {batches}. {batch_size} unique events not in any prior batch.\n"
            f"All event dates MUST fall between {start_year}-01-01 and {end_year}-12-31 inclusive.\n"
            f"Cover varied years within that window, varied regions, varied categories. "
            f"Rank by market impact (largest first). Be specific (named entities, real dates)."
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
            continue
        for ev in data.get("events", []):
            try:
                ts = datetime.fromisoformat(ev["date"])
            except Exception:
                continue
            # Drop events outside the requested window (LLM sometimes ignores instructions).
            if ts.year < start_year or ts.year > end_year:
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
