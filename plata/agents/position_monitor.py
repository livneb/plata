"""Position monitor — judges open positions and reacts.

Two concurrent loops:

A) Periodic check (every `monitor_check_interval_sec`, default 60s):
   For each open trade in `trade_ledger`:
     1. SL/TP hit → publish TradeClosure (close_reason=sl|tp), no LLM.
     2. Held > `monitor_max_hold_min` → publish TradeClosure (timeout), no LLM.
     3. If proposal has milestones → compute deviation vs predicted trajectory
        at the trade's current hours-from-entry. Bucket: on_track / drifting /
        off_track. Write `position:health:<ulid>` Redis hash for the UI.
     4. If off_track + not LLM-evaluated recently → call LLM → write a
        `Proposal` row with `state=adjustment_suggested`. User approves on
        /proposals/ (HITL).
     Trades without milestones get 1) and 2) only — Health is "untracked".

B) Event reaction (consumes Streams.ENRICHED_EVENTS):
   For each event:
     1. Look up open trades whose symbol or category matches the event.
     2. If `event.sentiment_magnitude >= monitor_event_sentiment_min` and there
        IS at least one matching open position → LLM evaluates whether to
        scale_up / scale_down / close. Result → `adjustment_suggested` row.
     3. No matching open positions → no-op (this isn't the strategist; we
        don't open new trades from here).

Auto vs HITL: configurable via `monitor_auto_close_*` and `monitor_auto_scale_*`
keys in risk_config. Defaults: SL/TP/timeout auto, everything else HITL.

The agent runs in `intelligence_sandbox` alongside the strategist (needs the
LLM + Redis + ledger). Heartbeat appears as `agent_status:position_monitor`;
cost is tracked under `cost:daily:*:agent:position_monitor` automatically.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from plata.agents.base import BaseAgent
from plata.core.bus import Streams, consume, get_redis, publish
from plata.core.db import TradeLedger, session_scope
from plata.core.llm import LLMClient
from plata.core.schemas import CloseReason, Side, TradeClosure, TradeMode
from plata.core.observability import get_logger

_log = get_logger("position_monitor")


def _interpolate_milestone(milestones: list[dict], hours_from_entry: float) -> float:
    """Linearly interpolate the expected cumulative % move at the given
    elapsed hours, using the strategist's milestone list (each is
    {eta_minutes, expected_pct_move, ...}).

    Before the first milestone → scale from 0.
    Between two milestones    → linear interp on (eta_h, expected_pct).
    After the last milestone  → flat-extrapolate (clamp to last value)."""
    if not milestones:
        return 0.0
    pts: list[tuple[float, float]] = []
    for m in milestones:
        try:
            eta_h = float(m.get("eta_minutes") or 0) / 60.0
            pct = float(m.get("expected_pct_move") or 0) * 100.0  # decimal → %
            pts.append((eta_h, pct))
        except (TypeError, ValueError):
            continue
    if not pts:
        return 0.0
    pts.sort(key=lambda x: x[0])
    if hours_from_entry <= pts[0][0]:
        # Linear from (0, 0) to (pts[0][0], pts[0][1])
        if pts[0][0] <= 0:
            return pts[0][1]
        return pts[0][1] * (hours_from_entry / pts[0][0])
    for i in range(len(pts) - 1):
        if pts[i][0] <= hours_from_entry <= pts[i + 1][0]:
            x0, y0 = pts[i]; x1, y1 = pts[i + 1]
            if x1 == x0:
                return y1
            return y0 + (y1 - y0) * (hours_from_entry - x0) / (x1 - x0)
    return pts[-1][1]  # past the last milestone — flat extrapolation


HEALTH_KEY = "position:health"  # hash per trade_ulid
LLM_LAST_EVAL_KEY = "position:last_llm_eval"  # hash trade_ulid → iso ts

ADJUSTMENT_DECISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["action", "reasoning", "conviction"],
    "properties": {
        "action": {"type": "string", "enum": ["hold", "close", "scale_up", "scale_down"]},
        "reasoning": {"type": "string", "minLength": 20, "maxLength": 1200},
        "conviction": {"type": "number", "minimum": 0, "maximum": 1},
        "scale_pct": {"type": "number", "minimum": 0.1, "maximum": 3.0},
    },
}

OFFTRACK_PROMPT = """You are a position monitor watching an OPEN trade.

The strategist opened this trade with a predicted trajectory. The trade is now
drifting from that prediction. Decide whether to HOLD, CLOSE the position, or
SCALE_DOWN (reduce size) based on the actual price action + the original
reasoning.

DO NOT recommend SCALE_UP — that's only for new-event reactions, not drift.

Output JSON: action ("hold" | "close" | "scale_down"), reasoning, conviction
0..1, and optionally scale_pct (used for scale_down only — e.g. 0.5 = halve
the position)."""

EVENT_PROMPT = """You are a position monitor reacting to a NEW EVENT that
affects an OPEN position.

Given the open position + the new event, decide: HOLD, CLOSE, SCALE_UP (add
notional), or SCALE_DOWN (reduce notional). Be conservative — recommend HOLD
unless the new event clearly changes the trade thesis.

Output JSON: action, reasoning, conviction 0..1, and scale_pct (for scale_up:
fraction of original notional to ADD; for scale_down: fraction to REMAIN —
e.g. 0.5 = halve)."""


class PositionMonitor(BaseAgent):
    name = "position_monitor"
    # No single input_stream — we drive ourselves. We DO consume ENRICHED_EVENTS
    # in loop B but as a separate consumer group inside setup(), not the
    # framework's default consume loop.

    def __init__(self) -> None:
        super().__init__()
        self._llm = LLMClient(self.name)

    async def handle(self, payload: dict[str, Any]) -> None:
        # Not used — we don't consume a single stream via the framework loop.
        # The two loops are kicked off in setup().
        return

    async def setup(self) -> None:
        # Kick off the two concurrent loops. They both honour self._halted.
        asyncio.create_task(self._periodic_loop(), name=f"{self.name}-periodic")
        asyncio.create_task(self._event_loop(), name=f"{self.name}-events")

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------

    async def _cfg(self) -> dict[str, str]:
        return await get_redis().hgetall("risk_config")

    def _f(self, cfg: dict[str, str], key: str, default: float) -> float:
        try:
            return float(cfg.get(key) or default)
        except (TypeError, ValueError):
            return default

    def _b(self, cfg: dict[str, str], key: str, default: bool) -> bool:
        v = (cfg.get(key) or str(default)).strip().lower()
        return v in ("1", "true", "yes", "on")

    def _auto_by_conviction(self, cfg: dict[str, str], decision: dict) -> bool:
        """Conviction-based auto-approval shortcut. Independent of the
        per-action toggles — if the monitor LLM is at least N confident
        (default 0.6), bypass HITL entirely. Set the threshold to 1.0 on
        /settings/?tab=risk to disable."""
        try:
            threshold = float(cfg.get("monitor_auto_approve_conviction_threshold") or 0.6)
        except (TypeError, ValueError):
            threshold = 0.6
        if threshold >= 1.0:
            return False
        try:
            conv = float((decision or {}).get("conviction") or 0)
        except (TypeError, ValueError):
            conv = 0.0
        return conv >= threshold

    # ------------------------------------------------------------------
    # Loop A — periodic per-trade health check
    # ------------------------------------------------------------------

    async def _periodic_loop(self) -> None:
        _log.info("position_monitor_periodic_starting")
        while True:
            if self._halted.is_set():
                await asyncio.sleep(5)
                continue
            cfg = await self._cfg()
            interval = max(10, int(self._f(cfg, "monitor_check_interval_sec", 60)))
            try:
                await self._check_all_open(cfg)
            except Exception as exc:  # noqa: BLE001
                _log.exception("monitor_periodic_error", error=str(exc)[:200])
            await asyncio.sleep(interval)

    async def _check_all_open(self, cfg: dict[str, str]) -> None:
        async with session_scope() as session:
            rows = (await session.execute(
                select(TradeLedger).where(TradeLedger.exit_price.is_(None))
            )).scalars().all()
        if not rows:
            return
        redis = get_redis()
        now = datetime.now(timezone.utc)
        for trade in rows:
            try:
                await self._check_one(trade, cfg, redis, now)
            except Exception as exc:  # noqa: BLE001
                _log.warning("monitor_check_one_failed",
                              trade=trade.trade_ulid, error=str(exc)[:160])

    async def _check_one(self, trade: TradeLedger, cfg: dict[str, str],
                          redis, now: datetime) -> None:
        # Current price.
        sym_latest = await redis.hgetall(f"symbol:latest:{trade.symbol}")
        cur_price: float | None = None
        try:
            cur_price = float(sym_latest.get("price") or 0) or None
        except (TypeError, ValueError):
            cur_price = None

        entry = float(trade.entry_price or 0)
        qty = float(trade.qty or 0)
        side_sign = 1.0 if (trade.side or "").lower() == "long" else -1.0

        # --- 0) Per-trade auto-close rules (set by user on trade detail) ----
        # These run BEFORE SL/TP because they're more user-specific and live
        # on `raw_bybit_response.auto_close_rules`. Each is a dollar / percent
        # / time threshold; first one that matches publishes a manual closure.
        raw_resp = trade.raw_bybit_response or {}
        rules = raw_resp.get("auto_close_rules") or {}
        if cur_price and rules and qty > 0 and entry > 0:
            unr = side_sign * (cur_price - entry) * qty
            unr_pct = side_sign * (cur_price - entry) / entry * 100.0
            triggered_rule: str | None = None
            # max_loss_usd — "close when unrealized PnL ≤ −$X"
            mlu = rules.get("max_loss_usd")
            if mlu and unr <= -float(mlu):
                triggered_rule = f"max_loss_usd:{mlu}"
            # max_loss_pct — "close when PnL% ≤ −X%"
            if not triggered_rule and rules.get("max_loss_pct"):
                mlp = float(rules["max_loss_pct"])
                if unr_pct <= -mlp:
                    triggered_rule = f"max_loss_pct:{mlp}"
            # close_after_days — "close N days after the rule was set"
            if not triggered_rule and rules.get("close_after_days"):
                set_at_iso = raw_resp.get("auto_close_rules_set_at")
                if set_at_iso:
                    try:
                        set_at = datetime.fromisoformat(set_at_iso)
                        days = float(rules["close_after_days"])
                        if (now - set_at).total_seconds() >= days * 86400:
                            triggered_rule = f"close_after_days:{days}"
                    except Exception:  # noqa: BLE001
                        pass
            # trailing_peak_pct — drawdown from the best PnL we've seen.
            # Cheap implementation: track best unr in a Redis hash.
            if not triggered_rule and rules.get("trailing_peak_pct"):
                try:
                    tk = f"trade:peak_unr:{trade.trade_ulid}"
                    peak_raw = await redis.get(tk)
                    peak = float(peak_raw) if peak_raw else unr
                    if unr > peak:
                        peak = unr
                        await redis.set(tk, peak, ex=14 * 86400)
                    drop_pct = (peak - unr) / max(abs(peak), 1e-9) * 100.0 if peak > 0 else 0.0
                    tpp = float(rules["trailing_peak_pct"])
                    if peak > 0 and drop_pct >= tpp:
                        triggered_rule = f"trailing_peak_pct:{tpp}"
                except Exception:  # noqa: BLE001
                    pass
            # rolling_loss_pct over rolling_loss_days — PnL drop over a window.
            if (not triggered_rule and rules.get("rolling_loss_pct")
                  and rules.get("rolling_loss_days")):
                try:
                    rlp = float(rules["rolling_loss_pct"])
                    rld = float(rules["rolling_loss_days"])
                    # Pull recent samples; trade:samples:<ulid> is newest-first
                    # cap-limited list (see trade_sampler).
                    raw_samples = await redis.lrange(
                        f"trade:samples:{trade.trade_ulid}", 0, 2000)
                    cutoff = now - timedelta(days=rld)
                    past_pct = None
                    # Walk backwards (oldest end) until we find a sample at-or-before cutoff.
                    for line in raw_samples[::-1]:
                        try:
                            obj = json.loads(line)
                            ts = datetime.fromisoformat(obj.get("ts"))
                            if ts >= cutoff:
                                past_pct = float(obj.get("pct") or 0)
                                break
                        except Exception:  # noqa: BLE001
                            continue
                    if past_pct is not None and (past_pct - unr_pct) >= rlp:
                        triggered_rule = f"rolling_loss:{rlp}%/{rld}d"
                except Exception:  # noqa: BLE001
                    pass
            if triggered_rule:
                self.log.info("auto_close_rule_triggered",
                              trade=trade.trade_ulid, rule=triggered_rule)
                await self._publish_closure(trade, cur_price, CloseReason.MANUAL)
                return

        # --- 1) SL / TP hit ---------------------------------------------------
        if cur_price and self._b(cfg, "monitor_auto_close_sl_tp", True):
            sl = float(trade.sl_price) if trade.sl_price is not None else None
            tp = float(trade.tp_price) if trade.tp_price is not None else None
            close_reason: CloseReason | None = None
            if side_sign > 0:
                if sl is not None and cur_price <= sl:
                    close_reason = CloseReason.STOP_LOSS
                elif tp is not None and cur_price >= tp:
                    close_reason = CloseReason.TAKE_PROFIT
            else:
                if sl is not None and cur_price >= sl:
                    close_reason = CloseReason.STOP_LOSS
                elif tp is not None and cur_price <= tp:
                    close_reason = CloseReason.TAKE_PROFIT
            if close_reason is not None:
                await self._publish_closure(trade, cur_price, close_reason)
                return

        # --- 2) Timeout -------------------------------------------------------
        if self._b(cfg, "monitor_auto_close_timeout", True) and trade.opened_at:
            max_hold = int(self._f(cfg, "monitor_max_hold_min", 10080))
            age_min = (now - trade.opened_at).total_seconds() / 60.0
            if age_min > max_hold and cur_price:
                await self._publish_closure(trade, cur_price, CloseReason.TIMEOUT)
                return

        # --- 3) Deviation math (no LLM) --------------------------------------
        # Look up the proposal's milestones via Redis stream snapshot.
        milestones = await self._load_milestones(trade.proposal_id)
        if not milestones or not cur_price or not entry:
            # Untracked: SL/TP/timeout still happen; no drift judgement.
            await redis.hset(f"{HEALTH_KEY}:{trade.trade_ulid}", mapping={
                "status": "untracked",
                "reason": "no_milestones" if not milestones else "no_price",
                "evaluated_at": now.isoformat(),
            })
            await redis.expire(f"{HEALTH_KEY}:{trade.trade_ulid}", 24 * 60 * 60)
            return

        hours_from_entry = (now - trade.opened_at).total_seconds() / 3600.0
        actual_pct = side_sign * (cur_price - entry) / entry * 100.0
        expected_pct = _interpolate_milestone(milestones, hours_from_entry)
        deviation_pct = abs(actual_pct - expected_pct)
        aligned = (actual_pct >= 0) == (expected_pct >= 0)

        drift_thresh = self._f(cfg, "monitor_drift_threshold_pct", 25.0)
        off_thresh = self._f(cfg, "monitor_off_track_threshold_pct", 50.0)
        if not aligned or deviation_pct >= off_thresh:
            status = "off_track"
        elif deviation_pct >= drift_thresh:
            status = "drifting"
        else:
            status = "on_track"
        await redis.hset(f"{HEALTH_KEY}:{trade.trade_ulid}", mapping={
            "status": status,
            "actual_pct": round(actual_pct, 4),
            "expected_pct": round(expected_pct, 4),
            "deviation_pct": round(deviation_pct, 4),
            "aligned": "true" if aligned else "false",
            "evaluated_at": now.isoformat(),
            "current_price": cur_price,
        })
        await redis.expire(f"{HEALTH_KEY}:{trade.trade_ulid}", 24 * 60 * 60)

        # --- 4) Off-track LLM evaluation (throttled) -------------------------
        if status != "off_track":
            return
        cooldown_min = int(self._f(cfg, "monitor_llm_cooldown_min", 30))
        last_eval = await redis.hget(LLM_LAST_EVAL_KEY, trade.trade_ulid)
        if last_eval:
            try:
                if (now - datetime.fromisoformat(last_eval)).total_seconds() < cooldown_min * 60:
                    return
            except Exception:  # noqa: BLE001
                pass
        await redis.hset(LLM_LAST_EVAL_KEY, trade.trade_ulid, now.isoformat())
        await self._offtrack_llm(trade, cfg, milestones,
                                  cur_price, actual_pct, expected_pct, deviation_pct)

    async def _publish_closure(self, trade: TradeLedger, price: float,
                                reason: CloseReason) -> None:
        sign = Decimal("1") if (trade.side or "").lower() == "long" else Decimal("-1")
        qty = Decimal(str(trade.qty or 0))
        entry = Decimal(str(trade.entry_price or 0))
        exit_d = Decimal(str(price))
        gross = sign * (exit_d - entry) * qty
        fees = Decimal(str(trade.fees or 0))
        closure = TradeClosure(
            trade_ulid=trade.trade_ulid,
            proposal_ulid=trade.proposal_id or "",
            symbol=trade.symbol,
            venue=trade.venue,
            mode=TradeMode(trade.mode) if trade.mode else TradeMode.PAPER,
            side=Side(trade.side),
            qty=qty, entry_price=entry, exit_price=exit_d,
            fees=fees, gross_pnl=gross, net_pnl=gross - fees,
            close_reason=reason,
            opened_at=trade.opened_at,
            closed_at=datetime.now(timezone.utc),
        )
        await publish(Streams.TRADE_CLOSURES, closure)
        _log.info("position_monitor_closed",
                   trade=trade.trade_ulid, reason=str(reason), price=price)

    async def _load_milestones(self, proposal_ulid: str | None) -> list[dict]:
        if not proposal_ulid:
            return []
        # Pull from the Postgres proposals table (fast, indexed) — falls back
        # to the Redis stream if the row hasn't been mirrored yet.
        try:
            from plata.core.proposals import get as proposal_get
            p = await proposal_get(proposal_ulid)
            if p and p.milestones:
                return list(p.milestones)
        except Exception:  # noqa: BLE001
            pass
        try:
            entries = await get_redis().xrevrange(Streams.TRADING_PROPOSALS, count=500)
            for _id, fields in entries:
                try:
                    payload = json.loads(fields.get("data", "{}"))
                    if payload.get("ulid") == proposal_ulid:
                        return list(payload.get("milestones") or [])
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            pass
        return []

    async def _offtrack_llm(self, trade: TradeLedger, cfg: dict[str, str],
                             milestones: list[dict], cur_price: float,
                             actual_pct: float, expected_pct: float,
                             deviation_pct: float) -> None:
        # De-dup: if there's already an open adjustment_suggested for this trade,
        # don't create another one. Previously the LLM cooldown only throttled the
        # LLM call, not the proposal row creation — so off-track BTC positions
        # produced dozens of "offtrack:close" rows on /proposals/ as they sat open.
        try:
            from plata.core.db import Proposal as _Prop, session_scope as _ss
            from sqlalchemy import desc as _desc, select as _select
            async with _ss() as session:
                existing = (await session.execute(
                    _select(_Prop)
                    .where(_Prop.state == "adjustment_suggested")
                    .order_by(_desc(_Prop.created_at))
                    .limit(50)
                )).scalars().all()
                for p in existing:
                    extras = p.extras or {}
                    if extras.get("adjustment_target_trade_ulid") == trade.trade_ulid:
                        self.log.debug("offtrack_skip_existing_open_adjustment",
                                       trade_ulid=trade.trade_ulid,
                                       existing_proposal=p.ulid)
                        return
        except Exception:  # noqa: BLE001
            pass

        user_msg = (
            f"OPEN TRADE: {trade.symbol} {trade.side} qty={trade.qty} @ {trade.entry_price}\n"
            f"Current: {cur_price}  ({actual_pct:+.2f}% from entry)\n"
            f"Predicted at this point: {expected_pct:+.2f}%\n"
            f"Deviation: {deviation_pct:.2f}%\n\n"
            f"Original milestones: {json.dumps(milestones)[:1200]}\n"
        )
        try:
            decision = await self._llm.structured(
                messages=[
                    {"role": "system", "content": OFFTRACK_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                schema=ADJUSTMENT_DECISION_SCHEMA,
                schema_name="position_adjustment",
                metadata={"trade_ulid": trade.trade_ulid, "kind": "offtrack"},
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning("offtrack_llm_failed",
                          trade=trade.trade_ulid, error=str(exc)[:160])
            return
        action = (decision or {}).get("action") or "hold"
        if action == "hold":
            return
        # Auto vs HITL
        auto = self._b(cfg, "monitor_auto_close_offtrack", False) or self._auto_by_conviction(cfg, decision)
        await self._record_adjustment(
            trade=trade,
            decision=decision,
            kind="offtrack",
            triggering_event_ulid=None,
            extras_extra={
                "cur_price": cur_price,
                "actual_pct": actual_pct,
                "expected_pct": expected_pct,
                "deviation_pct": deviation_pct,
            },
            auto_execute=auto,
        )

    # ------------------------------------------------------------------
    # Loop B — react to new events on held symbols
    # ------------------------------------------------------------------

    async def _event_loop(self) -> None:
        _log.info("position_monitor_event_loop_starting")
        while True:
            if self._halted.is_set():
                await asyncio.sleep(5)
                continue
            try:
                async for msg in consume(
                    Streams.ENRICHED_EVENTS,
                    group=f"{self.name}-grp",
                    consumer=self.consumer_name,
                ):
                    if self._halted.is_set():
                        break
                    try:
                        await self._handle_event(msg.payload)
                    except Exception as exc:  # noqa: BLE001
                        _log.warning("event_eval_failed",
                                      ulid=msg.payload.get("ulid"),
                                      error=str(exc)[:160])
                    finally:
                        from plata.core.bus import ack
                        await ack(msg.stream, f"{self.name}-grp", msg.redis_id)
            except Exception as exc:  # noqa: BLE001
                _log.exception("event_loop_error", error=str(exc)[:200])
                await asyncio.sleep(5)

    async def _handle_event(self, payload: dict[str, Any]) -> None:
        cfg = await self._cfg()
        threshold = self._f(cfg, "monitor_event_sentiment_min", 0.7)
        mag = float(payload.get("sentiment_magnitude") or 0)
        if mag < threshold:
            return
        # Symbol affected: derive from entities (ticker, asset, company) + category.
        affected: set[str] = set()
        for e in (payload.get("entities") or []):
            t = (e.get("type") or "").lower()
            if t in ("ticker", "asset", "company"):
                affected.add(str(e.get("id") or e.get("name") or "").upper())
        # Find matching open trades.
        async with session_scope() as session:
            rows = (await session.execute(
                select(TradeLedger).where(TradeLedger.exit_price.is_(None))
            )).scalars().all()
        targets = [t for t in rows if t.symbol.upper() in affected
                    or any(a and (a in t.symbol.upper() or t.symbol.upper() in a)
                            for a in affected)]
        if not targets:
            return
        for trade in targets:
            await self._event_llm(trade, payload, cfg)

    async def _event_llm(self, trade: TradeLedger, event: dict[str, Any],
                          cfg: dict[str, str]) -> None:
        cur = await get_redis().hgetall(f"symbol:latest:{trade.symbol}")
        cur_price = float(cur.get("price") or 0) or None
        entry = float(trade.entry_price or 0)
        sign = 1.0 if (trade.side or "").lower() == "long" else -1.0
        actual_pct = sign * ((cur_price or entry) - entry) / entry * 100.0 if entry else 0.0
        user_msg = (
            f"OPEN POSITION: {trade.symbol} {trade.side} qty={trade.qty} @ {trade.entry_price}\n"
            f"Current price: {cur_price}  ({actual_pct:+.2f}%)\n\n"
            f"NEW EVENT:\n"
            f"  summary: {event.get('summary')}\n"
            f"  category: {event.get('category')}\n"
            f"  sentiment: {event.get('sentiment')} (magnitude {event.get('sentiment_magnitude')})\n"
            f"  entities: {[e.get('id') or e.get('name') for e in (event.get('entities') or [])]}"
        )
        try:
            decision = await self._llm.structured(
                messages=[
                    {"role": "system", "content": EVENT_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                schema=ADJUSTMENT_DECISION_SCHEMA,
                schema_name="position_adjustment",
                metadata={"trade_ulid": trade.trade_ulid, "kind": "event"},
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning("event_llm_failed",
                          trade=trade.trade_ulid, error=str(exc)[:160])
            return
        action = (decision or {}).get("action") or "hold"
        if action == "hold":
            return
        if action == "scale_up":
            auto = self._b(cfg, "monitor_auto_scale_up", False)
        else:
            auto = self._b(cfg, "monitor_auto_scale_down", False)
        auto = auto or self._auto_by_conviction(cfg, decision)
        await self._record_adjustment(
            trade=trade,
            decision=decision,
            kind="event",
            triggering_event_ulid=event.get("ulid"),
            extras_extra={
                "cur_price": cur_price,
                "actual_pct": actual_pct,
                "event_category": event.get("category"),
                "event_sentiment_magnitude": event.get("sentiment_magnitude"),
                "event_summary": (event.get("summary") or "")[:400],
            },
            auto_execute=auto,
        )

    # ------------------------------------------------------------------
    # Recording the suggestion as a Proposal row + (optional) auto-exec
    # ------------------------------------------------------------------

    async def _record_adjustment(self, *, trade: TradeLedger, decision: dict,
                                  kind: str, triggering_event_ulid: str | None,
                                  extras_extra: dict, auto_execute: bool) -> None:
        from sqlalchemy.dialects.postgresql import insert as _pg_insert
        from plata.core.db import Proposal
        from plata.core.ulid import new_ulid
        ulid_str = new_ulid()
        action = decision.get("action") or "hold"
        scale_pct = decision.get("scale_pct")
        reasoning = decision.get("reasoning") or ""
        conv = float(decision.get("conviction") or 0)
        extras = {
            "adjustment_kind": kind,                # "offtrack" | "event"
            "adjustment_action": action,            # close | scale_up | scale_down
            "adjustment_scale_pct": scale_pct,
            "adjustment_target_trade_ulid": trade.trade_ulid,
            "adjustment_target_symbol": trade.symbol,
            "adjustment_target_side": trade.side,
            "adjustment_target_qty": str(trade.qty or 0),
            "adjustment_target_entry": str(trade.entry_price or 0),
            "auto_executed": auto_execute,
            **extras_extra,
        }
        try:
            async with session_scope() as session:
                stmt = _pg_insert(Proposal).values(
                    proposal_ulid=ulid_str[:26],
                    triggering_event_ulid=triggering_event_ulid,
                    symbol=trade.symbol,
                    side=trade.side,
                    conviction=conv,
                    reasoning=reasoning[:1500],
                    state="adjustment_executed" if auto_execute else "adjustment_suggested",
                    state_reason=f"{kind}:{action}",
                    last_actor=self.name,
                    extras=extras,
                ).on_conflict_do_nothing(index_elements=["proposal_ulid"])
                await session.execute(stmt)
        except Exception as exc:  # noqa: BLE001
            _log.warning("record_adjustment_failed",
                          trade=trade.trade_ulid, error=str(exc)[:160])
            return
        # SSE so the proposals page lights up.
        try:
            from plata.core.bus import publish_channel
            await publish_channel("dashboard:events", {
                "kind": "proposal_pending",
                "ulid": ulid_str,
                "symbol": trade.symbol,
                "side": trade.side,
                "adjustment": True,
                "action": action,
            })
        except Exception:  # noqa: BLE001
            pass
        # Auto-execute now if the toggle is on.
        if auto_execute:
            try:
                await self._apply_adjustment(trade, action, scale_pct or 1.0)
            except Exception as exc:  # noqa: BLE001
                _log.warning("auto_apply_adjustment_failed",
                              trade=trade.trade_ulid, error=str(exc)[:160])

    async def _apply_adjustment(self, trade: TradeLedger,
                                  action: str, scale_pct: float) -> None:
        """Execute a monitor-suggested adjustment. Called either from
        auto-execute path or from the /proposals/<ulid>/decide approval
        flow."""
        # CLOSE → publish TradeClosure at current price.
        if action == "close":
            redis = get_redis()
            sym = await redis.hgetall(f"symbol:latest:{trade.symbol}")
            try:
                price = float(sym.get("price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price > 0:
                await self._publish_closure(trade, price, CloseReason.MANUAL)
            return
        # SCALE_UP → emit a new TradeProposal so the regular pipeline opens
        # an additional trade on the same symbol/side. Bypass risk only at
        # the user's explicit approval (handled in the proposals route).
        # SCALE_DOWN is more invasive (partial close at venue API); for paper
        # mode we approximate by closing this trade and opening a smaller one.
        # Implemented as a HITL-only flow handled by the proposals route.
        # For auto-execute path, we no-op scale_* here (HITL by default).
        _log.info("apply_adjustment_scale_no_op_in_auto", action=action)
