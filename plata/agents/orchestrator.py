"""Orchestrator: monitors heartbeats, watches DLQ growth, auto-halts on anomalies."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from plata.agents.base import BaseAgent
from plata.core.bus import Channels, Streams, get_redis, publish_channel

DEAD_AGENT_THRESHOLD_SEC = 60
DLQ_SPIKE_THRESHOLD = 10  # errors/min for one agent triggers halt


class Orchestrator(BaseAgent):
    name = "orchestrator"
    input_stream = Streams.HEARTBEATS
    group = "orchestrator-grp"

    async def setup(self) -> None:
        self.log.info("orchestrator_starting")
        # Make sure system:state is set to RUNNING on first boot
        redis = get_redis()
        existing = await redis.get("system:state")
        if existing is None:
            await redis.set("system:state", "RUNNING")
        asyncio.create_task(self._dlq_watcher(), name="orch-dlq-watcher")
        asyncio.create_task(self._liveness_watcher(), name="orch-liveness-watcher")

    async def handle(self, payload: dict[str, Any]) -> None:
        # Just touch a Redis HASH so the dashboard can read latest seen.
        redis = get_redis()
        agent = payload.get("agent", "unknown")
        await redis.hset(
            f"orchestrator:seen:{agent}",
            mapping={"last_seen": datetime.now(timezone.utc).isoformat(), "raw": json.dumps(payload)},
        )

    async def _dlq_watcher(self) -> None:
        redis = get_redis()
        while True:
            await asyncio.sleep(60)
            keys = []
            async for k in redis.scan_iter(match="dlq:stats:*", count=100):
                keys.append(k)
            if not keys:
                continue
            pipe = redis.pipeline()
            for k in keys:
                pipe.hget(k, "count")
                pipe.hget(k, "_last_seen")
            results = await pipe.execute()
            # results alternates count, last_seen, count, last_seen, ...
            now = datetime.now(timezone.utc)
            for i, k in enumerate(keys):
                count_raw = results[i * 2]
                last_seen_raw = results[i * 2 + 1]
                count = int(count_raw or 0)
                last_seen = int(last_seen_raw or 0)
                window_count = count - last_seen
                if window_count >= DLQ_SPIKE_THRESHOLD:
                    self.log.error("dlq_spike_detected", key=k, count_in_window=window_count)
                    await publish_channel(Channels.SYSTEM_HALT, {
                        "reason": "dlq_spike", "key": k, "count": window_count,
                    })
                await redis.hset(k, "_last_seen", count)
                await redis.hset(k, "_last_checked", now.isoformat())

    async def _liveness_watcher(self) -> None:
        redis = get_redis()
        while True:
            await asyncio.sleep(30)
            keys = []
            async for k in redis.scan_iter(match="agent_status:*", count=100):
                keys.append(k)
            now = datetime.now(timezone.utc)
            for k in keys:
                status = await redis.hgetall(k)
                last_hb = status.get("last_heartbeat")
                if not last_hb:
                    continue
                last_dt = datetime.fromisoformat(last_hb)
                if (now - last_dt).total_seconds() > DEAD_AGENT_THRESHOLD_SEC:
                    agent_name = k.split(":")[-1]
                    self.log.warning("agent_appears_dead", agent=agent_name, last_heartbeat=last_hb)
                    # If a trading-critical agent is dead → halt.
                    if agent_name in {"risk_manager", "executor"}:
                        await publish_channel(Channels.SYSTEM_HALT, {
                            "reason": "critical_agent_dead", "agent": agent_name,
                        })
