"""Server-Sent Events stream for live dashboard updates.

Subscribes to Redis pub/sub channel `dashboard:events`. Each message becomes one
`data: <json>\\n\\n` line to the browser. Browser uses `EventSource('/sse')` which
auto-reconnects on drop.

To push an event from anywhere:
    from plata.core.bus import publish_channel
    await publish_channel('dashboard:events', {'kind': 'proposal_pending', ...})
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from plata.core.bus import subscribe
from plata.core.observability import get_logger

_log = get_logger("sse")

CHANNEL = "dashboard:events"
router = APIRouter(tags=["sse"])


@router.get("/sse")
async def sse_stream(request: Request) -> StreamingResponse:
    async def gen():
        # Initial hello so the client knows the stream is alive.
        yield "retry: 5000\n\n"
        yield f"event: hello\ndata: {json.dumps({'ok': True})}\n\n"
        # Heartbeat task to prevent intermediaries closing idle connections.
        last_ping = asyncio.get_event_loop().time()
        try:
            async for channel, payload in subscribe(CHANNEL):
                if await request.is_disconnected():
                    break
                if isinstance(payload, (dict, list)):
                    body = json.dumps(payload, default=str)
                else:
                    body = str(payload)
                yield f"data: {body}\n\n"
                now = asyncio.get_event_loop().time()
                if now - last_ping >= 20:
                    yield ": ping\n\n"
                    last_ping = now
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
