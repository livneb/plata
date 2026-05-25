"""Dead-letter queue viewer + replay."""
from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from plata.core.bus import Streams, get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/dlq", tags=["dlq"])

# Original streams whose DLQs we surface.
ORIGINAL_STREAMS = [
    Streams.RAW_SIGNALS,
    Streams.ENRICHED_EVENTS,
    Streams.TRADING_PROPOSALS,
    Streams.TRADE_CLOSURES,
]


async def _gather() -> list[dict]:
    redis = get_redis()
    out = []
    for stream in ORIGINAL_STREAMS:
        dlq = Streams.dlq_for(stream)
        try:
            length = await redis.xlen(dlq)
        except Exception:  # noqa: BLE001
            length = 0
        entries = []
        if length > 0:
            # Newest first
            raw = await redis.xrevrange(dlq, count=30)
            for redis_id, fields in raw:
                payload_raw = fields.get("payload", "{}")
                try:
                    payload = json.loads(payload_raw)
                except Exception:  # noqa: BLE001
                    payload = {"_raw": payload_raw[:200]}
                entries.append({
                    "redis_id": redis_id,
                    "original_id": fields.get("original_id"),
                    "agent": fields.get("agent"),
                    "error_type": fields.get("error_type"),
                    "payload_preview": _preview(payload),
                })
        out.append({"stream": stream, "dlq": dlq, "length": length, "entries": entries})
    return out


def _preview(payload: dict) -> str:
    for key in ("title", "summary", "symbol", "ulid"):
        v = payload.get(key)
        if v:
            return str(v)[:120]
    return json.dumps(payload, default=str)[:120]


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    items = await _gather()
    return templates.TemplateResponse(
        request, "pages/dlq.html", {"items": items, "active": "dlq"}
    )


async def _replay(dlq: str, original_stream: str, max_items: int | None = None) -> int:
    """Replay DLQ entries to the source stream one at a time with a small delay between each.

    Wire format matches `plata.core.bus.publish`: a single `{"data": <json>}` field.
    Returns the number of messages replayed.
    """
    import asyncio
    redis = get_redis()
    replayed = 0
    while True:
        batch = await redis.xrange(dlq, count=20)
        if not batch:
            break
        for redis_id, fields in batch:
            payload_raw = fields.get("payload", "{}")
            try:
                json.loads(payload_raw)  # validate
            except Exception:  # noqa: BLE001
                continue  # leave malformed entries in DLQ
            # Re-publish in the same wire format publish() uses: one "data" field.
            await redis.xadd(original_stream, {"data": payload_raw})
            await redis.xdel(dlq, redis_id)
            replayed += 1
            # Throttle so consumers process gradually instead of being slammed.
            await asyncio.sleep(0.05)
            if max_items is not None and replayed >= max_items:
                return replayed
    return replayed


@router.post("/{stream_name}/replay")
async def replay_stream(stream_name: str):
    import asyncio
    # Look up the original stream by name (sans dlq: prefix)
    original = next((s for s in ORIGINAL_STREAMS if s == stream_name), None)
    if original is None:
        return RedirectResponse(url="/dlq/", status_code=303)
    dlq = Streams.dlq_for(original)
    # Kick off replay as a background task so the HTTP request returns immediately;
    # the throttled drain keeps consumers from being slammed.
    asyncio.create_task(_replay(dlq, original), name=f"dlq-replay-{stream_name}")
    return RedirectResponse(url="/dlq/", status_code=303)


@router.post("/{stream_name}/clear")
async def clear_stream(stream_name: str):
    original = next((s for s in ORIGINAL_STREAMS if s == stream_name), None)
    if original is None:
        return RedirectResponse(url="/dlq/", status_code=303)
    redis = get_redis()
    await redis.delete(Streams.dlq_for(original))
    return RedirectResponse(url="/dlq/", status_code=303)
