"""BaseAgent: common stream-consumer loop with heartbeat, DLQ, halt-aware behavior."""
from __future__ import annotations

import asyncio
import os
import traceback
from abc import ABC, abstractmethod
from typing import Any

from plata.core.bus import (
    Channels,
    Streams,
    ack,
    consume,
    get_redis,
    publish,
    subscribe,
    to_dlq,
)
from plata.core.error_reporter import get_error_reporter
from plata.core.observability import get_logger
from plata.core.schemas import AgentHeartbeat

HEARTBEAT_INTERVAL_SEC = 10
SYSTEM_STATE_KEY = "system:state"


class BaseAgent(ABC):
    """Subclass and override `handle()` (and optionally `input_stream` + `group`)."""

    name: str = "base"
    input_stream: str | None = None
    group: str | None = None
    consume_in_paper_mode: bool = True

    def __init__(self) -> None:
        self.log = get_logger(self.name)
        self.error_reporter = get_error_reporter()
        self._halted = asyncio.Event()
        self._last_processed_ulid: str | None = None
        self._in_flight = 0
        self._error_count_60s = 0

    @property
    def container(self) -> str:
        return os.environ.get("SERVICE_ENTRYPOINT", "unknown")

    @property
    def consumer_name(self) -> str:
        return f"{self.name}-{os.environ.get('HOSTNAME', 'local')}"

    # ------------------------------------------------------------------
    # Subclass API
    # ------------------------------------------------------------------

    @abstractmethod
    async def handle(self, payload: dict[str, Any]) -> None:
        """Process one message. Raise to send the message to DLQ."""

    async def setup(self) -> None:  # pragma: no cover
        """Override for one-time init (e.g., create RediSearch indexes)."""
        return None

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------

    async def run(self) -> None:
        await self.setup()
        tasks: list[asyncio.Task] = [
            asyncio.create_task(self._heartbeat_loop(), name=f"{self.name}-heartbeat"),
            asyncio.create_task(self._halt_subscriber(), name=f"{self.name}-halt-sub"),
        ]
        if self.input_stream and self.group:
            tasks.append(asyncio.create_task(self._consume_loop(), name=f"{self.name}-consume"))
        else:
            tasks.append(asyncio.create_task(self._idle_loop(), name=f"{self.name}-idle"))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()

    async def _idle_loop(self) -> None:  # pragma: no cover
        """For agents that don't consume from a stream (e.g., dashboard, telegram)."""
        while True:
            await asyncio.sleep(3600)

    async def _consume_loop(self) -> None:
        assert self.input_stream and self.group
        async for msg in consume(self.input_stream, self.group, self.consumer_name):
            if self._halted.is_set():
                self.log.info("paused_due_to_halt", redis_id=msg.redis_id)
                continue
            self._in_flight += 1
            try:
                await self.handle(msg.payload)
                self._last_processed_ulid = msg.payload.get("ulid")
            except Exception as e:
                self._error_count_60s += 1
                tb = traceback.format_exc()
                await self.error_reporter.capture(
                    agent=self.name,
                    severity="ERROR",
                    error_type=type(e).__name__,
                    message=str(e) or repr(e),
                    traceback_str=tb,
                    context={"redis_id": msg.redis_id, "stream": msg.stream},
                )
                await to_dlq(
                    stream=msg.stream,
                    redis_id=msg.redis_id,
                    payload=msg.payload,
                    error_type=type(e).__name__,
                    traceback_str=tb,
                    agent=self.name,
                )
            finally:
                self._in_flight -= 1
                await ack(msg.stream, self.group, msg.redis_id)

    async def _heartbeat_loop(self) -> None:
        last_error_count = 0
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
            errors_window = max(0, self._error_count_60s - last_error_count)
            last_error_count = self._error_count_60s
            hb = AgentHeartbeat(
                agent=self.name,
                container=self.container,
                last_processed_ulid=self._last_processed_ulid,
                in_flight=self._in_flight,
                error_count_60s=errors_window,
            )
            try:
                await publish(Streams.HEARTBEATS, hb)
                redis = get_redis()
                await redis.hset(
                    f"agent_status:{self.name}",
                    mapping={
                        "last_heartbeat": hb.created_at.isoformat(),
                        "container": self.container,
                        "in_flight": str(self._in_flight),
                        "halted": str(self._halted.is_set()),
                    },
                )
            except Exception:  # pragma: no cover
                self.log.exception("heartbeat_failed")

    async def _halt_subscriber(self) -> None:
        """Listen for system:halt / system:resume and toggle local state."""
        redis = get_redis()
        # Check initial state from Redis on boot
        state = await redis.get(SYSTEM_STATE_KEY)
        if state == "HALTED":
            self._halted.set()
            self.log.warning("booted_in_halted_state")

        async for channel, payload in subscribe(Channels.SYSTEM_HALT, Channels.SYSTEM_RESUME):
            # Optional targeting: payload may include {"agent": "<name>"} for per-agent control.
            # Absent/empty agent => applies to all.
            target = (payload or {}).get("agent") if isinstance(payload, dict) else None
            if target and target != self.name:
                continue
            if channel == Channels.SYSTEM_HALT:
                self._halted.set()
                if not target:
                    await redis.set(SYSTEM_STATE_KEY, "HALTED")
                self.log.warning("halt_received", target=target or "all")
            elif channel == Channels.SYSTEM_RESUME:
                self._halted.clear()
                if not target:
                    await redis.set(SYSTEM_STATE_KEY, "RUNNING")
                self.log.info("resume_received", target=target or "all")
