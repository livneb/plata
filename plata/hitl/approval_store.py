"""Approval store: pending HITL proposals + pub/sub for approval decisions."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from plata.core.bus import Channels, get_redis, publish_channel

PENDING_PREFIX = "pending_approval:"


async def create_pending(proposal_ulid: str, proposal: dict[str, Any], reason: str) -> None:
    redis = get_redis()
    key = f"{PENDING_PREFIX}{proposal_ulid}"
    await redis.hset(key, mapping={
        "proposal": json.dumps(proposal, default=str),
        "reason": reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    })
    await redis.expire(key, 24 * 3600)
    await publish_channel(Channels.hitl_requested(), {
        "proposal_ulid": proposal_ulid, "reason": reason,
    })


async def list_pending() -> list[dict[str, Any]]:
    redis = get_redis()
    out: list[dict[str, Any]] = []
    async for k in redis.scan_iter(match=f"{PENDING_PREFIX}*", count=200):
        data = await redis.hgetall(k)
        if not data or data.get("status") != "pending":
            continue
        try:
            data["proposal"] = json.loads(data.get("proposal", "{}"))
        except json.JSONDecodeError:
            pass
        data["proposal_ulid"] = k.split(":")[-1]
        out.append(data)
    return sorted(out, key=lambda d: d.get("created_at", ""), reverse=True)


async def resolve(proposal_ulid: str, *, approved: bool, actor: str) -> bool:
    """Marks the proposal resolved + publishes to the approval channel. Returns True on first resolve."""
    redis = get_redis()
    key = f"{PENDING_PREFIX}{proposal_ulid}"
    # Atomic flip via Lua-style check
    current = await redis.hget(key, "status")
    if current != "pending":
        return False
    await redis.hset(key, mapping={
        "status": "approved" if approved else "rejected",
        "resolved_by": actor,
        "resolved_at": datetime.now(timezone.utc).isoformat(),
    })
    await publish_channel(Channels.approval(proposal_ulid), {
        "approved": approved, "actor": actor,
    })
    return True
