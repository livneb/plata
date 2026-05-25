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
    """Read each DLQ entry, re-XADD its payload to the original stream, delete from DLQ.

    Returns the number of messages replayed.
    """
    redis = get_redis()
    replayed = 0
    while True:
        batch = await redis.xrange(dlq, count=50)
        if not batch:
            break
        for redis_id, fields in batch:
            payload_raw = fields.get("payload", "{}")
            try:
                payload = json.loads(payload_raw)
            except Exception:  # noqa: BLE001
                # Skip unparseable entries — leave them in DLQ for manual inspection.
                continue
            await redis.xadd(original_stream, {k: str(v) for k, v in payload.items()})
            await redis.xdel(dlq, redis_id)
            replayed += 1
            if max_items is not None and replayed >= max_items:
                return replayed
    return replayed


@router.post("/{stream_name}/replay")
async def replay_stream(stream_name: str):
    # Look up the original stream by name (sans dlq: prefix)
    original = next(
        (s for s in ORIGINAL_STREAMS if s == stream_name),
        None,
    )
    if original is None:
        return RedirectResponse(url="/dlq/", status_code=303)
    dlq = Streams.dlq_for(original)
    await _replay(dlq, original)
    return RedirectResponse(url="/dlq/", status_code=303)


@router.post("/{stream_name}/clear")
async def clear_stream(stream_name: str):
    original = next((s for s in ORIGINAL_STREAMS if s == stream_name), None)
    if original is None:
        return RedirectResponse(url="/dlq/", status_code=303)
    redis = get_redis()
    await redis.delete(Streams.dlq_for(original))
    return RedirectResponse(url="/dlq/", status_code=303)
