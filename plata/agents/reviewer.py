"""Reviewer Agent — post-trade analysis, writes feedback edges to the graph."""
from __future__ import annotations

from typing import Any

from sqlalchemy import update

from plata.agents.base import BaseAgent
from plata.core.bus import Streams
from plata.core.db import TradeLedger, session_scope
from plata.core.graph import upsert_edge
from plata.core.llm import LLMClient
from plata.core.schemas import TradeClosure

REVIEW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "key_factor", "calibration_note"],
    "properties": {
        "verdict": {"type": "string", "enum": ["success", "failure", "mixed"]},
        "key_factor": {"type": "string", "minLength": 5, "maxLength": 300},
        "calibration_note": {"type": "string", "minLength": 5, "maxLength": 300},
    },
}

SYSTEM_PROMPT = """You are a post-trade analyst. Given a closed trade and the
events that triggered it, return a short structured verdict so future strategist
runs can learn from it.

Be honest: if the trade lost money, classify as 'failure' and identify what about
the triggering event was misleading."""


class Reviewer(BaseAgent):
    name = "reviewer"
    input_stream = Streams.TRADE_CLOSURES
    group = "reviewer-grp"

    def __init__(self) -> None:
        super().__init__()
        self._llm = LLMClient(self.name)

    async def handle(self, payload: dict[str, Any]) -> None:
        closure = TradeClosure(**payload)

        # Update Postgres ledger row
        async with session_scope() as session:
            await session.execute(
                update(TradeLedger)
                .where(TradeLedger.trade_ulid == closure.trade_ulid)
                .values(
                    exit_price=closure.exit_price,
                    gross_pnl=closure.gross_pnl,
                    net_pnl=closure.net_pnl,
                    fees=closure.fees,
                    closed_at=closure.closed_at,
                    close_reason=str(closure.close_reason),
                )
            )
        # Push trade closure to the dashboard SSE pipe.
        try:
            from plata.core.bus import publish_channel
            await publish_channel("dashboard:events", {
                "kind": "trade_closed",
                "trade_ulid": closure.trade_ulid,
                "symbol": closure.symbol,
                "net_pnl": float(closure.net_pnl or 0),
                "close_reason": str(closure.close_reason),
            })
        except Exception:  # noqa: BLE001
            pass

        prompt = (
            f"Trade: {closure.symbol} {closure.side} qty={closure.qty}\n"
            f"Entry: {closure.entry_price} → Exit: {closure.exit_price}\n"
            f"Net PnL: {closure.net_pnl}  Close reason: {closure.close_reason}\n"
        )
        review = await self._llm.structured(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            schema=REVIEW_SCHEMA,
            schema_name="trade_review",
            metadata={"trade_ulid": closure.trade_ulid},
        )

        # Weight signed by PnL so edges accumulate calibration
        weight = float(closure.net_pnl)
        await upsert_edge(
            src=f"proposal:{closure.proposal_ulid}",
            rel="caused",
            dst=f"trade:{closure.trade_ulid}",
            weight=weight,
            extra={
                "verdict": review["verdict"],
                "key_factor": review["key_factor"],
                "calibration_note": review["calibration_note"],
            },
        )
        self.log.info(
            "trade_reviewed", trade_ulid=closure.trade_ulid,
            verdict=review["verdict"], net_pnl=str(closure.net_pnl),
        )

        # --- Layer-2 self-improvement: accumulate per-bucket win-rate stats. ---
        try:
            await self._update_tuning_stats(closure, review)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("tuning_stats_failed", error=str(exc)[:160])

    # ---------------------- Self-improvement helpers ----------------------

    @staticmethod
    def _conviction_bucket(c: float | None) -> str:
        try:
            v = float(c or 0)
        except (TypeError, ValueError):
            return "unknown"
        if v < 0.6: return "<0.6"
        if v < 0.7: return "0.6-0.7"
        if v < 0.8: return "0.7-0.8"
        if v < 0.9: return "0.8-0.9"
        return "0.9-1.0"

    async def _update_tuning_stats(self, closure: TradeClosure, review: dict[str, Any]) -> None:
        """Increment per (symbol, category, conviction-bucket) counters in Redis and,
        every 25 closures, ask the LLM whether any guard config tweak is warranted.
        Tweaks are queued in Postgres `audit_log` for user review on /settings/?tab=tuning.
        """
        from plata.core.bus import get_redis
        from plata.core.db import AuditLog
        import json as _json

        redis = get_redis()
        # Pull triggering event category from the graph, if any.
        try:
            from plata.core.graph import event_key, get_event as _get_event
            # closure doesn't carry the triggering event ulid directly; best-effort: skip if missing.
            triggering_ulid = getattr(closure, "triggering_event_ulid", None)
            cat = ""
            if triggering_ulid:
                doc = await _get_event(triggering_ulid)
                cat = (doc or {}).get("category") or ""
        except Exception:  # noqa: BLE001
            cat = ""

        bucket = self._conviction_bucket(getattr(closure, "proposal_conviction", None))
        key = f"reviewer:stats:{(closure.symbol or '?')}:{cat or '?'}:{bucket}"
        pnl = float(closure.net_pnl or 0)
        won = 1 if pnl > 0 else 0
        lost = 1 if pnl < 0 else 0
        async with redis.pipeline() as pipe:
            pipe.hincrby(key, "trades", 1)
            pipe.hincrby(key, "wins", won)
            pipe.hincrby(key, "losses", lost)
            pipe.hincrbyfloat(key, "sum_pnl", pnl)
            pipe.expire(key, 60 * 60 * 24 * 90)
            await pipe.execute()

        total_closures = int(await redis.incr("reviewer:closures_since_tune") or 0)
        # Threshold is operator-tunable. Default lowered to 10 (was 25) so
        # tuning suggestions appear several times a day at typical closure
        # rates. Min-trades-per-bucket also configurable.
        try:
            cfg = await redis.hgetall("risk_config") or {}
            tune_every = int(cfg.get("tuning_every_n_closures") or 10)
            min_bucket_trades = int(cfg.get("tuning_min_bucket_trades") or 3)
        except Exception:  # noqa: BLE001
            tune_every, min_bucket_trades = 10, 3
        if total_closures < tune_every:
            return
        # Reset counter and propose a tweak.
        await redis.set("reviewer:closures_since_tune", 0)

        worst = await self._find_worst_bucket()
        if not worst or worst["trades"] < min_bucket_trades:
            return

        tweak = await self._propose_tweak(worst)
        if not tweak:
            return
        async with session_scope() as session:
            session.add(AuditLog(
                actor="reviewer.tuner",
                action="proposed_config_tweak",
                target=tweak.get("key", ""),
                payload={
                    "key": tweak.get("key"),
                    "old": tweak.get("old"),
                    "new": tweak.get("new"),
                    "rationale": tweak.get("rationale"),
                    "evidence": worst,
                    "status": "pending",
                },
            ))

    async def _find_worst_bucket(self) -> dict | None:
        from plata.core.bus import get_redis
        redis = get_redis()
        worst = None
        async for k in redis.scan_iter(match="reviewer:stats:*", count=200):
            data = await redis.hgetall(k)
            trades = int(data.get("trades") or 0)
            wins = int(data.get("wins") or 0)
            if trades < 3:
                continue
            wr = wins / trades
            if worst is None or wr < worst["win_rate"]:
                parts = k.split(":")
                worst = {
                    "key": k,
                    "symbol": parts[2] if len(parts) > 2 else "?",
                    "category": parts[3] if len(parts) > 3 else "?",
                    "conviction_bucket": parts[4] if len(parts) > 4 else "?",
                    "trades": trades, "wins": wins,
                    "losses": int(data.get("losses") or 0),
                    "sum_pnl": float(data.get("sum_pnl") or 0),
                    "win_rate": wr,
                }
        return worst

    async def _propose_tweak(self, worst: dict) -> dict | None:
        """Use the LLM to suggest one numeric tweak to a `guard_*` risk_config key.
        Returns {key, old, new, rationale} or None if nothing to do."""
        from plata.core.bus import get_redis
        redis = get_redis()
        cfg = await redis.hgetall("risk_config")
        cfg_view = {k: cfg.get(k) for k in cfg if k.startswith("guard_") or k.startswith("risk_per_trade") or k.startswith("max_")}
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["should_tweak"],
            "properties": {
                "should_tweak": {"type": "boolean"},
                "key": {"type": "string"},
                "new_value": {"type": "string"},
                "rationale": {"type": "string", "maxLength": 240},
            },
        }
        sys = (
            "You are a risk auto-tuner. Given the WORST-performing slice of recent trades and the current risk_config, "
            "decide if ONE existing key should be tightened. Allowed keys: guard_min_conviction, guard_symbol_cooldown_min, "
            "guard_max_per_category_day, risk_per_trade_pct, max_open_positions. "
            "Tweaks should be small and conservative (raise conviction by ≤0.05, halve a count, double a cooldown). "
            "Output JSON only."
        )
        user = (
            f"WORST SLICE: {worst}\n\nCURRENT CONFIG: {cfg_view}\n\n"
            "Respond with should_tweak=false if no clear, small, safe improvement is warranted."
        )
        try:
            d = await self._llm.structured(
                messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}],
                schema=schema, schema_name="risk_tweak",
                metadata={"worst_key": worst.get("key", "")},
            )
        except Exception:  # noqa: BLE001
            return None
        if not d.get("should_tweak"):
            return None
        key = d.get("key")
        new_val = d.get("new_value")
        if not key or new_val is None:
            return None
        return {"key": key, "old": cfg.get(key), "new": new_val, "rationale": d.get("rationale") or ""}
