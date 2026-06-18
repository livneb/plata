"""Postmortem agent — the system's hindsight loop.

For every proposal (executed AND dropped/rejected), wakes up at scheduled
checkpoints (1d / 1w / 1mo / longer depending on the horizon bucket) and
asks: "what did we predict, what actually happened, what's the lesson?"
Writes each lesson into a searchable Redis library that the strategist
queries when a similar event lands later.

Why both executed and dropped:
  - Executed: did the LLM call the move right? Did the milestones hold?
  - Dropped (llm_no_trade / rejected / hitl_rejected): if the asset
    rallied / dumped anyway, we missed something. Especially valuable
    when the drop reason was "no historical analog" — now we have one.

Per-bucket schedule (in minutes; configurable via risk_config):
  few_hours: 1d, 1w, 1mo
  few_days:  3d, 2w, 1mo
  few_weeks: 2w, 2mo
  long_term: 1mo, 3mo

Not a BaseAgent — it's a pure periodic task. Spawned in
intelligence_sandbox alongside the Researcher.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from plata.core.bus import get_redis
from plata.core.db import Proposal, session_scope
from plata.core.embeddings import embed
from plata.core.graph import upsert_lesson
from plata.core.llm import LLMClient
from plata.core.observability import get_logger
from plata.core.ulid import new_ulid

_log = get_logger("postmortem")


_DEFAULT_SCHEDULE_MIN: dict[str, list[int]] = {
    # When (in minutes from proposal.created_at) to run each checkpoint.
    "few_hours": [1440, 10080, 43200],            # 1d, 1w, 1mo
    "few_days":  [4320, 20160, 43200],            # 3d, 2w, 1mo
    "few_weeks": [20160, 86400],                  # 2w, 2mo
    "long_term": [43200, 129600],                 # 1mo, 3mo
}


LESSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["lesson", "severity", "signal_to_watch"],
    "properties": {
        "lesson": {"type": "string", "minLength": 30, "maxLength": 800,
                    "description": "Concise, actionable takeaway in 1-3 sentences."},
        "severity": {"type": "string",
                      "enum": ["informational", "concerning", "glaring"],
                      "description": "How badly the original decision missed."},
        "signal_to_watch": {"type": "string", "minLength": 5, "maxLength": 300,
                              "description": "A specific signal/tell to look for next time."},
    },
}


SYSTEM_PROMPT = """You are the system's hindsight reviewer. For each closed
proposal you see what the strategist predicted vs what actually happened to
the underlying asset since then. Write a SHORT lesson:

- 1-3 sentences.
- Concrete. Mention the symbol, what was predicted, what actually
  happened, and what signal would have steered us right.
- Severity = "glaring" if a dropped proposal would have made significant
  money OR an executed proposal lost significantly more than predicted.
  "concerning" if outcome was wrong but not catastrophic. "informational"
  if the prediction was roughly right.

Output strict JSON: {lesson, severity, signal_to_watch}."""


async def _schedule_for(bucket: str) -> list[int]:
    try:
        r = get_redis()
        raw = await r.hget("risk_config", f"postmortem_schedule_{bucket}")
        if raw:
            return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except Exception:  # noqa: BLE001
        pass
    return _DEFAULT_SCHEDULE_MIN.get(bucket, [1440, 10080, 43200])


def _decision_label(state: str | None) -> str:
    s = (state or "").lower()
    if s in ("executed", "approved", "manual_override"):
        return "traded"
    if s in ("hitl_rejected", "hitl_timeout"):
        return "hitl_declined"
    if s in ("rejected",):
        return "risk_rejected"
    if s in ("dropped", "published"):
        return "dropped"
    return s or "unknown"


async def _current_price(symbol: str) -> float | None:
    try:
        r = get_redis()
        sym = await r.hgetall(f"symbol:latest:{symbol}") or {}
        v = sym.get("price")
        return float(v) if v else None
    except Exception:  # noqa: BLE001
        return None


async def _predicted_move(proposal: Proposal, checkpoint_min: int
                           ) -> float | None:
    """Interpolate the milestone trajectory at `checkpoint_min` minutes."""
    if not proposal.milestones:
        return None
    try:
        milestones = sorted(
            proposal.milestones,
            key=lambda m: int(m.get("eta_minutes") or 0),
        )
    except Exception:  # noqa: BLE001
        return None
    # If checkpoint is beyond the last milestone, use the last one's prediction.
    last = milestones[-1] if milestones else None
    for m in milestones:
        try:
            eta = int(m.get("eta_minutes") or 0)
        except (TypeError, ValueError):
            continue
        if eta >= checkpoint_min:
            return float(m.get("expected_pct_move") or 0)
    return float(last.get("expected_pct_move") or 0) if last else None


class Postmortem:
    """Periodic-task agent. See module docstring."""

    name = "postmortem"

    def __init__(self) -> None:
        self._llm = LLMClient(self.name)
        self.log = _log

    async def run(self) -> None:
        await asyncio.sleep(30)  # stagger boot
        while True:
            stats: dict = {"cycle_started": datetime.now(timezone.utc).isoformat()}
            try:
                stats = await self.cycle()
                self.log.info("postmortem_cycle_done", **stats)
            except Exception as exc:  # noqa: BLE001
                stats["error"] = str(exc)[:200]
                self.log.warning("postmortem_cycle_failed",
                                  error=str(exc)[:200])
            # Self-report status so /agents/ and the operator can see the
            # agent is alive + what it's doing each cycle. Mirrors the
            # BaseAgent heartbeat field shape (this class isn't a
            # BaseAgent — it's a pure periodic task — so we write it
            # ourselves).
            try:
                await get_redis().hset(
                    f"agent_status:{self.name}",
                    mapping={
                        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
                        "halted": "false",
                        **{k: str(v) for k, v in stats.items()},
                    },
                )
            except Exception:  # noqa: BLE001
                pass
            interval = await self._interval_sec()
            await asyncio.sleep(interval)

    async def _interval_sec(self) -> int:
        try:
            r = get_redis()
            cfg = await r.hgetall("risk_config") or {}
            min_m = int(cfg.get("postmortem_interval_min") or 60)
        except Exception:  # noqa: BLE001
            min_m = 60
        return max(300, min_m * 60)

    async def cycle(self) -> dict:
        """Scan proposals whose next checkpoint is due, write lessons.
        Returns a stats dict with candidate counts + skip reasons so the
        operator can see WHY the library isn't growing when it isn't."""
        from datetime import timedelta as _td
        stats: dict = {"candidates": 0, "checkpoints_due": 0, "written": 0,
                        "skipped_no_price": 0, "skipped_llm_fail": 0,
                        "skipped_embed_fail": 0}
        try:
            r = get_redis()
            cfg = await r.hgetall("risk_config") or {}
            if (cfg.get("postmortem_enabled") or "true").lower() not in ("true", "1", "yes"):
                stats["skipped_disabled"] = True
                return stats
            per_cycle_cap = int(cfg.get("postmortem_max_per_cycle") or 50)
        except Exception:  # noqa: BLE001
            per_cycle_cap = 50

        # Only fetch proposals OLD ENOUGH for at least one checkpoint.
        # Smallest checkpoint across all buckets is 1440 min (few_hours[0]).
        # Order OLDEST-FIRST so we work through the backlog instead of
        # repeatedly looking at fresh proposals that aren't due yet. This was
        # the bug that left the library empty after 24h — the previous
        # newest-first / limit-500 query pushed eligible old rows off the end.
        now = datetime.now(timezone.utc)
        oldest_eligible_cutoff = now - _td(minutes=1440)
        async with session_scope() as session:
            rows = (await session.execute(
                select(Proposal)
                .where(Proposal.created_at <= oldest_eligible_cutoff)
                .order_by(Proposal.created_at.asc())
                .limit(2000)
            )).scalars().all()
        stats["candidates"] = len(rows)

        for p in rows:
            if stats["written"] >= per_cycle_cap:
                stats["hit_cycle_cap"] = True
                break
            extras = dict(p.extras or {})
            bucket = extras.get("horizon_bucket") or "few_days"
            schedule = await _schedule_for(bucket)
            done = set(extras.get("postmortem_done") or [])
            age_min = int((now - p.created_at).total_seconds() // 60)
            persisted_this_proposal = False
            for ckpt in schedule:
                key = f"{ckpt}"
                if key in done:
                    continue
                if age_min < ckpt:
                    continue
                stats["checkpoints_due"] += 1
                try:
                    outcome = await self._run_checkpoint(p, ckpt)
                except Exception as exc:  # noqa: BLE001
                    self.log.warning("postmortem_checkpoint_failed",
                                      proposal=p.proposal_ulid, ckpt=ckpt,
                                      error=str(exc)[:200])
                    outcome = "error"
                if outcome == "ok":
                    stats["written"] += 1
                    done.add(key)
                    persisted_this_proposal = True
                elif outcome == "no_price":
                    stats["skipped_no_price"] += 1
                elif outcome == "llm_fail":
                    stats["skipped_llm_fail"] += 1
                elif outcome == "embed_fail":
                    stats["skipped_embed_fail"] += 1
                if stats["written"] >= per_cycle_cap:
                    break
            # Persist `done` ONCE per proposal at the end, not per checkpoint.
            if persisted_this_proposal:
                async with session_scope() as session:
                    fresh = (await session.execute(
                        select(Proposal).where(Proposal.proposal_ulid == p.proposal_ulid)
                    )).scalar_one_or_none()
                    if fresh is not None:
                        merged = dict(fresh.extras or {})
                        merged["postmortem_done"] = sorted(done)
                        fresh.extras = merged
        return stats

    async def _run_checkpoint(self, p: Proposal, checkpoint_min: int) -> str:
        """One LLM call for one (proposal, checkpoint) combination. Returns
        "ok" / "no_price" / "llm_fail" / "embed_fail" so the cycle stats
        can show the operator what's failing."""
        # First: prefer the trade's exit_price (closed) or entry_price as a
        # reference. The symbol watcher only refreshes for OPEN positions,
        # so for closed-position symbols `symbol:latest:<sym>` is stale.
        entry_price: float | None = None
        exit_price: float | None = None
        try:
            from plata.core.db import TradeLedger
            async with session_scope() as session:
                tl = (await session.execute(
                    select(TradeLedger).where(TradeLedger.proposal_id == p.proposal_ulid)
                )).scalar_one_or_none()
            if tl:
                entry_price = float(tl.entry_price) if tl.entry_price else None
                exit_price = float(tl.exit_price) if tl.exit_price else None
        except Exception:  # noqa: BLE001
            pass

        # "Current" reference price: live mark when we have it, else
        # exit_price (for closed trades), else None — in which case we
        # can still write a lesson using entry_price's catalyst context
        # but without an actual-move number.
        cur_price = await _current_price(p.symbol)
        if cur_price is None or cur_price <= 0:
            cur_price = exit_price  # closed-trade fallback
        if cur_price is None and entry_price is None:
            # Nothing to compare against — skip silently, retry next cycle.
            return "no_price"
        if entry_price is None:
            entry_price = cur_price  # dropped proposals: anchor at first known
        if cur_price is None:
            cur_price = entry_price  # truly no movement data; lesson still possible
        actual_pct = (cur_price - entry_price) / entry_price if entry_price else 0.0
        predicted_pct = await _predicted_move(p, checkpoint_min)

        # Build LLM input.
        bucket = (p.extras or {}).get("horizon_bucket") or "few_days"
        decision_label = _decision_label(p.state)
        predicted_line = (
            f"Predicted move at this checkpoint: {(predicted_pct*100):.2f}%"
            if predicted_pct is not None
            else "Predicted move at this checkpoint: (no milestone)"
        )
        prompt = (
            f"Proposal {p.proposal_ulid} — {bucket}, decision was {decision_label}.\n"
            f"Symbol: {p.symbol}\n"
            f"Side: {p.side}\n"
            f"Strategist conviction: {p.conviction}\n"
            f"Strategist reasoning: {p.reasoning}\n"
            f"Time since proposal: {checkpoint_min} min.\n"
            f"{predicted_line}\n"
            f"Actual move since {decision_label}: {(actual_pct*100):.2f}%\n\n"
            f"Write the lesson. JSON only."
        )

        try:
            verdict = await self._llm.structured(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                schema=LESSON_SCHEMA,
                schema_name="postmortem_lesson",
                metadata={"proposal_ulid": p.proposal_ulid,
                          "checkpoint_min": checkpoint_min},
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning("postmortem_llm_failed",
                              proposal=p.proposal_ulid, error=str(exc)[:200])
            return "llm_fail"

        # Embed the lesson text (with symbol + category for grouping) so the
        # strategist can KNN-retrieve later.
        category = (p.extras or {}).get("category") or "other"
        try:
            embedding = await embed(
                f"{p.symbol} {category} — " + verdict["lesson"],
                input_type="document",
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning("postmortem_embed_failed",
                              proposal=p.proposal_ulid, error=str(exc)[:200])
            return "embed_fail"

        await upsert_lesson(
            ulid=new_ulid(),
            proposal_ulid=p.proposal_ulid,
            symbol=p.symbol,
            category=str(category),
            horizon_bucket=bucket,
            severity=verdict["severity"],
            lesson=verdict["lesson"],
            signal_to_watch=verdict.get("signal_to_watch"),
            embedding=embedding,
            decision_at_time=decision_label,
            checkpoint_min=checkpoint_min,
            predicted_pct=predicted_pct,
            actual_pct=actual_pct,
            ts=datetime.now(timezone.utc),
        )
        self.log.info("postmortem_lesson_written",
                       proposal=p.proposal_ulid, symbol=p.symbol,
                       checkpoint_min=checkpoint_min,
                       severity=verdict["severity"])
        return "ok"


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(Postmortem().run())
