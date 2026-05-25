"""Redis Streams pub/sub abstraction.

Streams: append-only logs consumed by consumer groups (multiple consumers, exactly-once semantics).
Pub/Sub: ephemeral broadcasts (system:halt, config_updated, approval:*).

DLQ pattern: on handler failure, BaseAgent will XACK the message and forward the
payload + traceback to `dlq:{stream_name}` for later inspection.
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import redis.asyncio as aioredis
from pydantic import BaseModel

from inkcliq.config.settings import get_settings
from inkcliq.core.observability import get_logger

_log = get_logger("bus")


# ---------------------------------------------------------------------------
# Stream names
# ---------------------------------------------------------------------------

class Streams:
    RAW_SIGNALS = "raw_signals:stream"
    ENRICHED_EVENTS = "enriched_events:stream"
    TRADING_PROPOSALS = "trading_proposals:stream"
    RISK_DECISIONS = "risk_decisions:stream"
    APPROVED_TRADES = "approved_trades:stream"
    EXECUTED_TRADES = "executed_trades:stream"
    TRADE_CLOSURES = "trade_closures:stream"
    HEARTBEATS = "agent_heartbeats:stream"

    @staticmethod
    def dlq_for(stream: str) -> str:
        return f"dlq:{stream}"


class Channels:
    SYSTEM_HALT = "system:halt"
    SYSTEM_RESUME = "system:resume"
    CONFIG_UPDATED = "config_updated"

    @staticmethod
    def approval(proposal_ulid: str) -> str:
        return f"approval:{proposal_ulid}"

    @staticmethod
    def hitl_requested() -> str:
        return "hitl_requested"


# ---------------------------------------------------------------------------
# Redis client
# ---------------------------------------------------------------------------

@lru_cache
def get_redis() -> aioredis.Redis:
    settings = get_settings()
    return aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )


# ---------------------------------------------------------------------------
# Stream pub/consume
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class StreamMessageRef:
    """Reference to a message read from a stream — includes Redis-side id for XACK."""

    stream: str
    redis_id: str
    payload: dict[str, Any]


async def publish(stream: str, message: BaseModel) -> str:
    """Publish a Pydantic model to a Redis Stream. Returns the assigned id."""
    redis = get_redis()
    body = message.model_dump_json()
    msg_id = await redis.xadd(stream, {"data": body})
    _log.debug("published", stream=stream, redis_id=msg_id)
    return msg_id


async def publish_raw(stream: str, payload: dict[str, Any]) -> str:
    redis = get_redis()
    body = json.dumps(payload, default=str)
    return await redis.xadd(stream, {"data": body})


async def ensure_consumer_group(stream: str, group: str) -> None:
    """Create the consumer group if it doesn't exist. Idempotent."""
    redis = get_redis()
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def consume(
    stream: str,
    group: str,
    consumer: str,
    *,
    block_ms: int = 5000,
    count: int = 10,
) -> AsyncIterator[StreamMessageRef]:
    """Async iterator that yields messages from a consumer group.

    Caller is responsible for `ack()` after successful handling.
    On unhandled exception, caller should `dlq()` then `ack()` to remove the
    poison pill from the pending list.
    """
    redis = get_redis()
    await ensure_consumer_group(stream, group)

    while True:
        response = await redis.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block_ms,
        )
        if not response:
            continue
        for _stream_name, messages in response:
            for redis_id, fields in messages:
                raw = fields.get("data", "{}")
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    payload = {"_raw": raw, "_decode_error": True}
                yield StreamMessageRef(stream=stream, redis_id=redis_id, payload=payload)


async def ack(stream: str, group: str, redis_id: str) -> None:
    redis = get_redis()
    await redis.xack(stream, group, redis_id)


async def to_dlq(
    stream: str,
    redis_id: str,
    payload: dict[str, Any],
    error_type: str,
    traceback_str: str,
    agent: str,
) -> None:
    """Send poison message to DLQ stream + increment counter."""
    redis = get_redis()
    dlq_stream = Streams.dlq_for(stream)
    dlq_payload = {
        "original_id": redis_id,
        "original_stream": stream,
        "agent": agent,
        "error_type": error_type,
        "traceback": traceback_str,
        "payload": json.dumps(payload, default=str),
    }
    await redis.xadd(dlq_stream, dlq_payload)
    await redis.hincrby(f"dlq:stats:{agent}", "count", 1)


async def trim_stream(stream: str, maxlen: int = 100_000, approximate: bool = True) -> int:
    redis = get_redis()
    return await redis.xtrim(stream, maxlen=maxlen, approximate=approximate)


# ---------------------------------------------------------------------------
# Pub/Sub helpers
# ---------------------------------------------------------------------------

async def publish_channel(channel: str, payload: dict[str, Any] | str) -> int:
    redis = get_redis()
    body = payload if isinstance(payload, str) else json.dumps(payload, default=str)
    return await redis.publish(channel, body)


async def subscribe(*channels: str) -> AsyncIterator[tuple[str, Any]]:
    """Yield (channel, decoded_message) tuples until cancelled."""
    redis = get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe(*channels)
    try:
        async for raw in pubsub.listen():
            if raw["type"] != "message":
                continue
            channel = raw["channel"]
            data = raw["data"]
            try:
                decoded = json.loads(data)
            except (TypeError, json.JSONDecodeError):
                decoded = data
            yield channel, decoded
    finally:
        await pubsub.unsubscribe(*channels)
        await pubsub.close()
