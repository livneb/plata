"""Researcher agent — the "brain behind the system".

Runs a periodic synthesis loop (default 30 min, key `researcher_interval_min`
in risk_config). Each cycle:

  1. Gathers the last ~30 enriched events the pipeline saw, plus the
     existing "current" briefing for continuity.
  2. Asks the LLM (paid model by default — quality matters) for a compact
     market briefing: regime, top narratives, sector outlook, watchlist
     deltas.
  3. Persists the briefing at `knowledge_briefing:current` (Redis hash).
  4. Publishes on Channels.KNOWLEDGE_UPDATED so listeners can warm their
     local copy.

The strategist and position_monitor prepend the current briefing to their
LLM prompts (see `plata.core.llm.LLMClient.complete`) so every trade
decision reflects the latest market context — making the system a self-
improving brain rather than a stateless reactor.

Not a `BaseAgent` because there's no input stream — it's a pure
periodic task. Spawned from `entrypoints._run_intelligence_sandbox`.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

from plata.core.bus import Channels, get_redis, publish_channel
from plata.core.llm import LLMClient
from plata.core.observability import get_logger

_log = get_logger("researcher")


BRIEFING_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["regime", "regime_rationale", "narratives", "sector_outlook"],
    "properties": {
        "regime": {
            "type": "string",
            "enum": ["risk_on", "risk_off", "mixed", "uncertain"],
        },
        "regime_rationale": {"type": "string", "minLength": 20, "maxLength": 400},
        "narratives": {
            "type": "array", "minItems": 1, "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "summary"],
                "properties": {
                    "title": {"type": "string", "maxLength": 80},
                    "summary": {"type": "string", "minLength": 30, "maxLength": 300},
                    "conviction": {"type": "number", "minimum": 0, "maximum": 1},
                },
            },
        },
        "sector_outlook": {
            "type": "object",
            "additionalProperties": False,
            "required": ["crypto", "equities", "commodities"],
            "properties": {
                "crypto":      {"type": "string", "maxLength": 300},
                "equities":    {"type": "string", "maxLength": 300},
                "commodities": {"type": "string", "maxLength": 300},
            },
        },
        "watchlist_add":  {"type": "array", "items": {"type": "string"}, "maxItems": 10},
        "watchlist_drop": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
    },
}


SYSTEM_PROMPT = """You are the in-house macro/markets researcher for a small
autonomous trading shop. You write a concise briefing every 30 minutes that
the trading agents inject into every decision. Your job:

1. Read the latest events the pipeline ingested.
2. Decide the current market regime (risk_on, risk_off, mixed, uncertain).
3. Identify the top 1-5 market narratives, with short summary and your
   conviction in each. Be specific and falsifiable.
4. One-sentence outlook for each of: crypto, equities, commodities.
5. List symbols to ADD or DROP from the watchlist if applicable.

Be tight. Each section should be useful for someone making a trade RIGHT NOW.
Output strict JSON matching the schema."""


async def _gather_recent_events(limit: int = 30) -> list[dict]:
    """Pull the last `limit` ENRICHED_EVENTS via XREVRANGE for compact input.
    Cheap (Redis O(N) on a tiny tail). Returns list of {summary, category,
    sentiment_magnitude} dicts."""
    from plata.core.bus import Streams
    try:
        r = get_redis()
        entries = await r.xrevrange(Streams.ENRICHED_EVENTS, count=limit)
    except Exception:  # noqa: BLE001
        return []
    out: list[dict] = []
    for _id, fields in entries:
        try:
            payload = json.loads(fields.get("payload") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        out.append({
            "summary": (payload.get("summary") or "")[:240],
            "category": payload.get("category"),
            "sentiment_magnitude": payload.get("sentiment_magnitude"),
        })
    return out


async def _read_current_briefing() -> dict | None:
    try:
        r = get_redis()
        h = await r.hgetall("knowledge_briefing:current")
        if not h:
            return None
        body = h.get("body")
        return json.loads(body) if body else None
    except Exception:  # noqa: BLE001
        return None


class Researcher:
    """Periodic-task researcher. See module docstring."""

    name = "researcher"

    def __init__(self) -> None:
        self._llm = LLMClient(self.name)
        self.log = _log

    async def run(self) -> None:
        # See Postmortem.run for the rationale: split heartbeat (10s) from
        # the slow cycle loop (30min default) so the sysop's stale-agent
        # watcher doesn't flag us as dead between cycles.
        self._last_status: dict = {"cycle": "pending_first"}
        await asyncio.sleep(5)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._heartbeat_loop(), name="researcher-heartbeat")
            tg.create_task(self._cycle_loop(),     name="researcher-cycle")

    async def _heartbeat_loop(self) -> None:
        import os as _os
        container = _os.environ.get("SERVICE_ENTRYPOINT", "unknown")
        while True:
            try:
                await get_redis().hset(
                    f"agent_status:{self.name}",
                    mapping={
                        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
                        "container": container,
                        "halted": "false",
                        **{f"cycle_{k}": str(v) for k, v in self._last_status.items()},
                    },
                )
            except Exception:  # noqa: BLE001
                self.log.warning("researcher_heartbeat_failed")
            await asyncio.sleep(10)

    async def _cycle_loop(self) -> None:
        await asyncio.sleep(10)
        while True:
            try:
                await self.cycle()
                self._last_status = {"last_ok_at": datetime.now(timezone.utc).isoformat()}
            except Exception as exc:  # noqa: BLE001
                self._last_status = {"error": str(exc)[:200]}
                self.log.warning("researcher_cycle_failed",
                                  error=str(exc)[:200])
            interval_sec = await self._interval_sec()
            await asyncio.sleep(interval_sec)

    async def _interval_sec(self) -> int:
        try:
            r = get_redis()
            cfg = await r.hgetall("risk_config") or {}
            min_m = int(cfg.get("researcher_interval_min") or 30)
        except Exception:  # noqa: BLE001
            min_m = 30
        return max(60, min_m * 60)

    async def cycle(self) -> None:
        # Bail early if disabled.
        try:
            r = get_redis()
            cfg = await r.hgetall("risk_config") or {}
            if (cfg.get("researcher_enabled") or "true").lower() not in ("true", "1", "yes"):
                self.log.debug("researcher_disabled_skip")
                return
        except Exception:  # noqa: BLE001
            pass

        recent = await _gather_recent_events()
        prior = await _read_current_briefing()
        prior_block = ""
        if prior:
            prior_block = (
                "\nPREVIOUS BRIEFING (write the next one as a coherent update — "
                "don't reset thinking, evolve it):\n"
                + json.dumps(prior, indent=2)[:1500]
            )
        events_block = "\n".join(
            f"- [{e.get('category')}] (mag={e.get('sentiment_magnitude')}) {e['summary']}"
            for e in recent
        ) or "(no recent events)"
        user_msg = (
            f"RECENT EVENTS (last {len(recent)}):\n{events_block}\n"
            f"{prior_block}\n\n"
            f"Now write today's briefing. Output JSON only."
        )
        try:
            briefing = await self._llm.structured(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                schema=BRIEFING_SCHEMA,
                schema_name="market_briefing",
                metadata={"cycle_at": datetime.now(timezone.utc).isoformat()},
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning("researcher_llm_failed", error=str(exc)[:200])
            return

        # Persist + notify.
        now = datetime.now(timezone.utc)
        try:
            await get_redis().hset("knowledge_briefing:current", mapping={
                "body": json.dumps(briefing),
                "ts": now.isoformat(),
                "model": self._llm.model or "",
                "cycle_id": now.strftime("%Y%m%d-%H%M"),
            })
        except Exception as exc:  # noqa: BLE001
            self.log.warning("researcher_persist_failed", error=str(exc)[:160])
            return
        try:
            await publish_channel(Channels.KNOWLEDGE_UPDATED, {
                "ts": now.isoformat(),
                "regime": briefing.get("regime"),
            })
        except Exception:  # noqa: BLE001
            pass
        self.log.info("researcher_cycle_done",
                       regime=briefing.get("regime"),
                       narratives=len(briefing.get("narratives") or []))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(Researcher().run())
