"""SelfImprover — continuous LLM-driven self-improvement agent.

Layer 3 of the self-improvement plan. Wakes every N hours (default 6),
bundles together everything the system has learned recently — recent
trades and their outcomes, the current risk_config, the calibration
table's surprises, recent error log peaks, per-bucket utilization, and
crucially the EFFECT of the last few tweaks the operator has applied —
and asks an LLM to suggest ONE additional structured config change.

Distinct from the existing `Reviewer._propose_tweak` (which fires every
N closures and looks ONLY at the single worst-performing bucket): the
SelfImprover sees the whole portfolio, the calibration deltas, and the
historical effectiveness of past tweaks. The Reviewer is a
trade-by-trade reactive tuner; the SelfImprover is a periodic
strategic tuner. They write to the same Tuning page (same
`audit_log.action = "proposed_config_tweak"`) so the operator has a
single approval queue.

Reflexion-style memory:
  - When the operator clicks Apply on a tweak, the Tuning route flips
    `payload.status` to "applied". On the NEXT self-improver run, we
    look up the realized PnL in the 24h windows BEFORE and AFTER each
    recently-applied tweak and include that in the LLM context as
    "what worked / what didn't". The LLM is told not to re-propose
    something that demonstrably made things worse.

Cost guard:
  - Skip the run if today's `cost:daily:<date>:agent:self_improver` is
    already above `self_improver_daily_budget_usd` (default $0.50).
    Sonnet calls with this much context cost roughly $0.05–$0.10 each;
    the budget caps the agent at ~5–10 cycles per day.

Disable from /settings/?tab=advanced by setting
`self_improver_enabled = false`.
"""
from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc as _desc, select as _select

from plata.core.bus import get_redis
from plata.core.db import AuditLog, ErrorLog, TradeLedger, session_scope
from plata.core.llm import LLMClient
from plata.core.observability import get_logger

_log = get_logger("self_improver")

_INITIAL_DELAY_SEC = 5 * 60         # let the rest of the stack warm up
_DEFAULT_INTERVAL_SEC = 6 * 60 * 60 # every 6h
_DEFAULT_DAILY_BUDGET_USD = 0.50
_RECENT_TRADES_LIMIT = 50
_RECENT_TWEAKS_LIMIT = 8
_BUNDLE_REDIS_KEY = "self_improver:last_bundle"
_BUNDLE_TTL_SEC = 14 * 24 * 60 * 60


# Allowed tweak keys — same shape as Reviewer's, plus a few more that
# the wider context bundle actually has the evidence to argue about.
# Restricted on purpose: we don't want the LLM proposing arbitrary
# new config keys nobody's wired up.
ALLOWED_KEYS: tuple[str, ...] = (
    "guard_min_conviction",
    "guard_symbol_cooldown_min",
    "guard_max_per_category_day",
    "guard_one_per_symbol_side",
    "guard_dedup_event_ulid",
    "risk_per_trade_pct",
    "max_open_positions",
    "max_correlated_positions",
    "strategist_sentiment_threshold",
    "strategist_analog_k",
    "min_conviction_to_publish",
    "council_min_conviction",
    "horizon_few_hours_daily_count",
    "horizon_few_days_daily_count",
    "horizon_few_weeks_daily_count",
    "horizon_long_term_daily_count",
    "horizon_few_hours_budget_pct",
    "horizon_few_days_budget_pct",
    "horizon_few_weeks_budget_pct",
    "horizon_long_term_budget_pct",
    "calibrator_min_samples",
    "calibrator_interval_min",
)


TWEAK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["should_tweak", "diagnosis", "confidence"],
    "properties": {
        # The LLM is encouraged to say "should_tweak=false, things look OK"
        # rather than fabricate a low-confidence change.
        "should_tweak": {"type": "boolean"},
        "diagnosis":    {"type": "string", "minLength": 20, "maxLength": 600},
        "confidence":   {"type": "number", "minimum": 0, "maximum": 1},
        "key":          {"type": "string", "enum": list(ALLOWED_KEYS)},
        "new_value":    {"type": "string", "maxLength": 64},
        "rationale":    {"type": "string", "maxLength": 600},
        "expected_effect": {"type": "string", "maxLength": 240},
    },
}

SYSTEM_PROMPT = """You are the self-improvement agent for a multi-agent news-driven trading system.

You see the live system state (config + recent trades + calibration deltas
+ effects of past tweaks) and decide whether ONE small, conservative
configuration change would improve forward-looking risk-adjusted return.

Hard rules:
- Propose AT MOST ONE change per turn. Smaller is better. If unsure, return should_tweak=false.
- Only pick keys from the provided list — the dashboard knows how to apply only those.
- Conservative steps: raise/lower a fraction by ≤0.05, halve/double an int, change a $ by ≤25%.
- READ the "past_tweak_effects" carefully. If a recent tweak made things WORSE, do NOT propose
  reverting it instantly — at least one full cycle should pass first. If a tweak made things
  BETTER, consider extending that direction. If unmeasured, prefer waiting.
- If the system looks healthy (positive PnL, calibration deltas small, no error spikes),
  return should_tweak=false with a one-line diagnosis. Pestering with constant tweaks is harmful.
- expected_effect should be a falsifiable one-liner ("expect 24h win-rate to rise from 40% to 50%")
  so future runs can score this tweak.

Return JSON only."""


async def _llm_cost_today() -> float:
    today_key = f"cost:daily:{date.today().isoformat()}:agent:self_improver"
    try:
        return float(await get_redis().get(today_key) or 0.0)
    except Exception:  # noqa: BLE001
        return 0.0


async def _calibration_snapshot() -> dict[str, Any]:
    redis = get_redis()
    out: dict[str, Any] = {"cells": [], "meta": {}}
    try:
        meta_raw = await redis.hgetall("calibration:meta") or {}
        out["meta"] = meta_raw
        cells_raw = await redis.hgetall("calibration:conviction_table") or {}
    except Exception:  # noqa: BLE001
        return out
    cells: list[dict[str, Any]] = []
    for field, payload in cells_raw.items():
        try:
            d = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        d["field"] = field
        midpoint = float(d.get("midpoint") or 0.5)
        wr = float(d.get("wr") or 0.5)
        d["delta"] = round(wr - midpoint, 4)
        cells.append(d)
    # Surface the most surprising cells first — by |delta|.
    cells.sort(key=lambda c: abs(c.get("delta", 0.0)), reverse=True)
    out["cells"] = cells[:20]
    return out


async def _bucket_utilization() -> dict[str, Any]:
    """How full is each horizon bucket today? Reveals "we have quota
    but no proposals" vs "we're slamming into the cap"."""
    redis = get_redis()
    today = date.today().isoformat()
    cfg = await redis.hgetall("risk_config") or {}
    DEFAULT_COUNTS = {"few_hours": 35, "few_days": 15,
                      "few_weeks": 10, "long_term": 5}
    out: dict[str, Any] = {}
    for bucket in DEFAULT_COUNTS:
        try:
            max_per_day = int(cfg.get(f"horizon_{bucket}_daily_count")
                              or DEFAULT_COUNTS[bucket])
            used = int(await redis.get(f"horizon:count:{today}:{bucket}") or 0)
            out[bucket] = {"used": used, "max": max_per_day,
                           "fill_pct": round(100 * used / max(1, max_per_day), 1)}
        except Exception:  # noqa: BLE001
            continue
    return out


async def _recent_trades() -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=14)
    async with session_scope() as session:
        rows = (await session.execute(
            _select(TradeLedger)
            .where(TradeLedger.closed_at.isnot(None),
                   TradeLedger.closed_at >= cutoff)
            .order_by(_desc(TradeLedger.closed_at))
            .limit(_RECENT_TRADES_LIMIT)
        )).scalars().all()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "trade_ulid":  r.trade_ulid,
            "symbol":      r.symbol,
            "side":        getattr(r, "side", None),
            "entry_price": float(r.entry_price or 0) if r.entry_price else None,
            "exit_price":  float(r.exit_price  or 0) if r.exit_price  else None,
            "net_pnl":     float(r.net_pnl     or 0) if r.net_pnl     else 0.0,
            "fees":        float(r.fees        or 0) if r.fees        else 0.0,
            "close_reason": getattr(r, "close_reason", None),
            "closed_at":   r.closed_at.isoformat() if r.closed_at else None,
        })
    return out


def _summarise_trades(trades: list[dict]) -> dict[str, Any]:
    if not trades:
        return {"n": 0, "wins": 0, "losses": 0, "net_pnl": 0.0, "win_rate": None}
    wins = sum(1 for t in trades if t["net_pnl"] > 0)
    losses = sum(1 for t in trades if t["net_pnl"] < 0)
    pnl = sum(t["net_pnl"] for t in trades)
    return {
        "n":         len(trades),
        "wins":      wins,
        "losses":    losses,
        "net_pnl":   round(pnl, 2),
        "win_rate":  round(wins / len(trades), 3),
    }


async def _recent_errors() -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    async with session_scope() as session:
        rows = (await session.execute(
            _select(ErrorLog)
            .where(ErrorLog.ts >= cutoff,
                   ErrorLog.severity.in_(["ERROR", "CRITICAL"]))
            .order_by(_desc(ErrorLog.ts))
            .limit(15)
        )).scalars().all()
    # Collapse by (agent, error_type) so a thousand identical NoEmbedding
    # rows show up as one line with a count.
    counts: dict[tuple[str, str], int] = {}
    sample: dict[tuple[str, str], str] = {}
    for r in rows:
        k = (r.agent or "?", r.error_type or "?")
        counts[k] = counts.get(k, 0) + 1
        sample.setdefault(k, (r.message or "")[:200])
    return [
        {"agent": a, "error_type": e, "count": c, "sample": sample[(a, e)]}
        for (a, e), c in sorted(counts.items(), key=lambda kv: -kv[1])
    ]


async def _past_tweak_effects() -> list[dict[str, Any]]:
    """Pull the last applied tweaks and compute realized-PnL deltas
    over equal pre/post windows. This is the Reflexion memory: the LLM
    sees which previous changes helped or hurt and is told not to
    repeat the losers."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    async with session_scope() as session:
        audits = (await session.execute(
            _select(AuditLog)
            .where(AuditLog.action == "proposed_config_tweak",
                   AuditLog.ts >= cutoff)
            .order_by(_desc(AuditLog.ts))
            .limit(60)
        )).scalars().all()
    applied: list[AuditLog] = [
        a for a in audits if (a.payload or {}).get("status") == "applied"
    ][:_RECENT_TWEAKS_LIMIT]

    out: list[dict[str, Any]] = []
    for a in applied:
        applied_at = a.ts
        window = timedelta(hours=24)
        async with session_scope() as session:
            pre = (await session.execute(
                _select(TradeLedger)
                .where(TradeLedger.closed_at.isnot(None),
                       TradeLedger.closed_at >= applied_at - window,
                       TradeLedger.closed_at <  applied_at)
            )).scalars().all()
            post = (await session.execute(
                _select(TradeLedger)
                .where(TradeLedger.closed_at.isnot(None),
                       TradeLedger.closed_at >= applied_at,
                       TradeLedger.closed_at <  applied_at + window)
            )).scalars().all()
        # Skip post-windows that haven't elapsed yet — the data isn't in.
        elapsed = datetime.now(timezone.utc) - applied_at
        post_complete = elapsed >= window
        pre_pnl  = sum(float(r.net_pnl or 0) for r in pre)
        post_pnl = sum(float(r.net_pnl or 0) for r in post)
        out.append({
            "applied_at":   applied_at.isoformat(),
            "key":          (a.payload or {}).get("key"),
            "old":          (a.payload or {}).get("old"),
            "new":          (a.payload or {}).get("new"),
            "actor":        a.actor,
            "rationale":    (a.payload or {}).get("rationale"),
            "pre_window":   {"n": len(pre),  "net_pnl": round(pre_pnl, 2)},
            "post_window":  {"n": len(post), "net_pnl": round(post_pnl, 2)},
            "delta_net_pnl": round(post_pnl - pre_pnl, 2) if post_complete else None,
            "post_complete": post_complete,
        })
    return out


async def _build_context_bundle() -> dict[str, Any]:
    """Snapshot of everything the LLM should see in one call."""
    redis = get_redis()
    cfg = await redis.hgetall("risk_config") or {}
    # Strip out fields irrelevant to risk/strategy decisions to keep the
    # context tight (the hash has dozens of monitor_* / horizon_* sub-keys).
    config_view = {
        k: v for k, v in cfg.items()
        if k in ALLOWED_KEYS
        or k.startswith("guard_")
        or k.startswith("risk_per_trade")
        or k.startswith("max_")
        or k.startswith("strategist_")
        or k.startswith("horizon_")
        or k.startswith("council_")
        or k.startswith("calibrator_")
    }
    trades = await _recent_trades()
    bundle = {
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "config":             config_view,
        "trades_summary":     _summarise_trades(trades),
        "recent_trades":      trades[:20],         # keep token cost bounded
        "calibration":        await _calibration_snapshot(),
        "bucket_utilization": await _bucket_utilization(),
        "recent_errors":      await _recent_errors(),
        "past_tweak_effects": await _past_tweak_effects(),
        "allowed_keys":       list(ALLOWED_KEYS),
    }
    return bundle


async def _persist_bundle(bundle: dict[str, Any]) -> None:
    """Mirror the latest bundle to Redis so the dashboard can render
    what the agent saw on its last run — total transparency."""
    try:
        redis = get_redis()
        await redis.set(_BUNDLE_REDIS_KEY, json.dumps(bundle, default=str))
        await redis.expire(_BUNDLE_REDIS_KEY, _BUNDLE_TTL_SEC)
    except Exception:  # noqa: BLE001
        pass


async def _queue_tweak(decision: dict[str, Any], bundle: dict[str, Any]) -> int | None:
    """Append the proposed tweak to the same Tuning queue the Reviewer
    uses. Returns the new AuditLog id (or None on failure)."""
    redis = get_redis()
    cfg = await redis.hgetall("risk_config") or {}
    key = decision.get("key")
    new_val = decision.get("new_value")
    if not key or new_val is None:
        return None
    payload = {
        "key":         key,
        "old":         cfg.get(key),
        "new":         new_val,
        "rationale":   decision.get("rationale") or "",
        "diagnosis":   decision.get("diagnosis") or "",
        "confidence":  decision.get("confidence"),
        "expected_effect": decision.get("expected_effect") or "",
        # The whole bundle is too heavy to stash on every audit row, so
        # only summarise the evidence the LLM was looking at.
        "evidence": {
            "trades":             bundle.get("trades_summary"),
            "bucket_utilization": bundle.get("bucket_utilization"),
            "calibration_meta":   bundle.get("calibration", {}).get("meta"),
            "top_calibration_deltas":
                bundle.get("calibration", {}).get("cells", [])[:5],
            "top_errors":         bundle.get("recent_errors", [])[:3],
            "past_tweak_effects": bundle.get("past_tweak_effects", [])[:5],
        },
        "status": "pending",
    }
    async with session_scope() as session:
        row = AuditLog(
            actor="self_improver",
            action="proposed_config_tweak",
            target=key,
            payload=payload,
        )
        session.add(row)
        await session.flush()
        return row.id


async def run_once() -> dict[str, Any] | None:
    """One pass: build bundle, call LLM, queue tweak. Returns the LLM
    decision dict (or None if skipped / failed)."""
    redis = get_redis()
    cfg = await redis.hgetall("risk_config") or {}
    enabled = (cfg.get("self_improver_enabled") or "true").lower() in (
        "true", "1", "yes", "on",
    )
    if not enabled:
        _log.info("self_improver_disabled_skipping")
        return None
    budget = float(cfg.get("self_improver_daily_budget_usd")
                    or _DEFAULT_DAILY_BUDGET_USD)
    spent = await _llm_cost_today()
    if spent >= budget:
        _log.info("self_improver_budget_exhausted",
                   spent_usd=spent, budget_usd=budget)
        return None

    bundle = await _build_context_bundle()
    await _persist_bundle(bundle)

    # Refuse to call the LLM if there's literally nothing to learn from.
    if (bundle["trades_summary"]["n"] == 0
            and not bundle["calibration"]["cells"]
            and not bundle["recent_errors"]):
        _log.info("self_improver_nothing_to_review")
        return None

    llm = LLMClient("self_improver")
    user_msg = (
        "SYSTEM STATE BUNDLE (JSON):\n"
        + json.dumps(bundle, default=str, indent=2)[:24000]
        + "\n\nDecide whether ONE tweak from `allowed_keys` is warranted now. "
          "If past_tweak_effects shows a recent applied tweak made things worse, "
          "leave that key alone — let one cycle pass. Return JSON only."
    )
    try:
        decision = await llm.structured(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            schema=TWEAK_SCHEMA,
            schema_name="self_improver_tweak",
            metadata={"agent": "self_improver"},
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("self_improver_llm_failed", error=str(exc)[:200])
        return None

    if not decision.get("should_tweak"):
        _log.info("self_improver_no_action",
                   diagnosis=(decision.get("diagnosis") or "")[:160],
                   confidence=decision.get("confidence"))
        return decision

    audit_id = await _queue_tweak(decision, bundle)
    _log.info("self_improver_tweak_queued",
               audit_id=audit_id, key=decision.get("key"),
               new_value=decision.get("new_value"),
               confidence=decision.get("confidence"))
    return decision


async def run() -> None:
    """Background loop. Sleep → run_once → sleep, forever.
    Honors `self_improver_enabled` and `self_improver_interval_hours`
    from the live risk_config hash."""
    await asyncio.sleep(_INITIAL_DELAY_SEC)
    while True:
        interval_sec = _DEFAULT_INTERVAL_SEC
        try:
            cfg = await get_redis().hgetall("risk_config") or {}
            interval_hours = float(cfg.get("self_improver_interval_hours") or 6)
            interval_sec = max(30 * 60, int(interval_hours * 3600))
            await run_once()
        except Exception as exc:  # noqa: BLE001
            _log.warning("self_improver_loop_error", error=str(exc)[:200])
        await asyncio.sleep(interval_sec)
