"""Conviction Calibrator — closes the Reviewer → Strategist feedback loop.

Layer 1 of the self-improvement plan. Deterministic, no LLM calls.

The Reviewer agent already writes per-bucket trade stats to Redis hashes
keyed `reviewer:stats:{symbol}:{category}:{conviction_bucket}` (fields:
`trades`, `wins`, `losses`, `sum_pnl`). Nothing currently reads those
stats back into the trading path, so a "0.7 conviction" trade has no
empirical anchor to what 0.7 actually means in P(win).

This agent:

  1. Periodically (every `calibrator_interval_min`, default 30) scans
     all `reviewer:stats:*` hashes.
  2. Aggregates them into two views:
       - `(category, conviction_bucket)`  — coarser, more samples
       - `(symbol,   conviction_bucket)`  — finer, fewer samples
  3. For each cell with `trades >= calibrator_min_samples` (default 3),
     computes a Beta(1,1)-smoothed win-rate:  ``(wins + 1) / (trades + 2)``
     so a 3-trade bucket with 2 wins comes out at 0.60, not 0.67 — small
     samples are pulled toward 0.5 in proportion to how thin they are.
  4. Writes the table to the Redis hash `calibration:conviction_table`
     (fields shaped `cat:{CATEGORY}:{BUCKET}` and `sym:{SYMBOL}:{BUCKET}`,
     values JSON like {"wr": 0.62, "trades": 9, "midpoint": 0.75}).
  5. Writes a meta blob to `calibration:meta` with the timestamp,
     cell-counts and the largest |wr - midpoint| delta (so the dashboard
     can show "calibration says your 0.85-bucket trades actually win 47%").

The Strategist consults the table after the LLM returns a proposal:
if a matching cell exists with enough samples, the raw LLM conviction
is REPLACED with the cell's empirical win-rate. Both values are stashed
in proposal.extras so the trade-detail page can render the calibration
delta and the operator can audit the system's self-correction.

Disable from /settings/?tab=advanced by setting
`calibrator_enabled = false`.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from plata.core.bus import get_redis
from plata.core.observability import get_logger

_log = get_logger("calibrator")

_INITIAL_DELAY_SEC = 60         # wait for the rest of the stack to boot
_DEFAULT_INTERVAL_SEC = 30 * 60
_DEFAULT_MIN_SAMPLES = 3

# Same labels the Reviewer already uses — keep them in sync.
_BUCKET_MIDPOINTS: dict[str, float] = {
    "<0.6":    0.55,
    "0.6-0.7": 0.65,
    "0.7-0.8": 0.75,
    "0.8-0.9": 0.85,
    "0.9-1.0": 0.95,
}


def conviction_bucket(c: float | None) -> str:
    """Inverse of `_BUCKET_MIDPOINTS`. Returns the bucket label for a
    conviction value in [0, 1]. Mirrored from Reviewer._conviction_bucket
    so the calibrator's output keys join cleanly with the Reviewer's
    inputs."""
    try:
        v = float(c or 0)
    except (TypeError, ValueError):
        return "<0.6"
    if v < 0.6:
        return "<0.6"
    if v < 0.7:
        return "0.6-0.7"
    if v < 0.8:
        return "0.7-0.8"
    if v < 0.9:
        return "0.8-0.9"
    return "0.9-1.0"


def beta_smoothed_winrate(wins: int, trades: int) -> float:
    """Beta(1, 1) posterior mean — the classic Laplace rule of succession.

    Maps small samples toward 0.5 in proportion to data thinness:
      0/0    → 0.500   (no data, take the prior)
      1/1    → 0.667   (one win still pulls strongly toward 0.5)
      2/3    → 0.600   (vs naive 0.667)
      8/10   → 0.750   (vs naive 0.80)
      80/100 → 0.794   (close to the naive 0.80 — large-sample limit)

    This is what we want for trade calibration: a single lucky win on a
    given conviction-bucket shouldn't move the calibrated WR all the way
    to 1.0; a hundred trades should pin it close to the true rate.
    """
    if trades < 0 or wins < 0 or wins > trades:
        return 0.5
    return (wins + 1) / (trades + 2)


async def build_table(min_samples: int = _DEFAULT_MIN_SAMPLES) -> dict:
    """Scan `reviewer:stats:*`, aggregate two ways, return a single
    flat table ready to write into the `calibration:conviction_table`
    Redis hash.

    Returns ``{"fields": {...}, "meta": {...}}`` rather than writing —
    the caller decides whether this run is worth persisting (e.g. skip
    the write when nothing changed).
    """
    redis = get_redis()
    by_cat:  dict[tuple[str, str], dict] = {}
    by_sym:  dict[tuple[str, str], dict] = {}
    total_trades = 0

    async for k in redis.scan_iter(match="reviewer:stats:*", count=200):
        # Key shape: reviewer:stats:{symbol}:{category}:{bucket}
        parts = k.split(":")
        if len(parts) < 5:
            continue
        symbol, category, bucket = parts[2], parts[3], ":".join(parts[4:])
        if bucket not in _BUCKET_MIDPOINTS:
            continue
        data = await redis.hgetall(k)
        try:
            trades = int(data.get("trades") or 0)
            wins   = int(data.get("wins")   or 0)
        except (TypeError, ValueError):
            continue
        if trades <= 0:
            continue
        total_trades += trades
        cat_key = (category, bucket)
        sym_key = (symbol,   bucket)
        for store, key in ((by_cat, cat_key), (by_sym, sym_key)):
            slot = store.setdefault(key, {"trades": 0, "wins": 0})
            slot["trades"] += trades
            slot["wins"]   += wins

    fields: dict[str, str] = {}
    largest_delta = 0.0
    cells_written = 0
    for (category, bucket), agg in by_cat.items():
        if agg["trades"] < min_samples:
            continue
        wr = beta_smoothed_winrate(agg["wins"], agg["trades"])
        midpoint = _BUCKET_MIDPOINTS[bucket]
        fields[f"cat:{category}:{bucket}"] = json.dumps({
            "wr": round(wr, 4),
            "trades": agg["trades"],
            "wins": agg["wins"],
            "midpoint": midpoint,
        })
        cells_written += 1
        largest_delta = max(largest_delta, abs(wr - midpoint))
    for (symbol, bucket), agg in by_sym.items():
        if agg["trades"] < min_samples:
            continue
        wr = beta_smoothed_winrate(agg["wins"], agg["trades"])
        midpoint = _BUCKET_MIDPOINTS[bucket]
        fields[f"sym:{symbol}:{bucket}"] = json.dumps({
            "wr": round(wr, 4),
            "trades": agg["trades"],
            "wins": agg["wins"],
            "midpoint": midpoint,
        })
        cells_written += 1
        largest_delta = max(largest_delta, abs(wr - midpoint))

    meta = {
        "last_run_at":        datetime.now(timezone.utc).isoformat(),
        "cells_written":      cells_written,
        "trades_observed":    total_trades,
        "category_cells":     sum(1 for k in fields if k.startswith("cat:")),
        "symbol_cells":       sum(1 for k in fields if k.startswith("sym:")),
        "largest_wr_delta":   round(largest_delta, 4),
        "min_samples":        min_samples,
    }
    return {"fields": fields, "meta": meta}


async def lookup(
    category: str | None,
    symbol: str | None,
    conviction: float | None,
) -> dict | None:
    """Strategist helper. Returns the most-specific calibration cell that
    matches: prefer ``(symbol, bucket)`` over ``(category, bucket)``
    when both exist, because the symbol view tells us this asset
    specifically — not the whole category.

    Returns ``{"wr", "trades", "midpoint", "source"}`` or None.
    """
    bucket = conviction_bucket(conviction)
    redis = get_redis()
    try:
        cfg = await redis.hgetall("risk_config") or {}
        min_samples = int(cfg.get("calibrator_min_samples")
                          or _DEFAULT_MIN_SAMPLES)
    except Exception:  # noqa: BLE001
        min_samples = _DEFAULT_MIN_SAMPLES

    candidates: list[tuple[str, str]] = []
    if symbol:
        candidates.append(("symbol",   f"sym:{symbol}:{bucket}"))
    if category:
        candidates.append(("category", f"cat:{category}:{bucket}"))
    for source, field in candidates:
        raw = await redis.hget("calibration:conviction_table", field)
        if not raw:
            continue
        try:
            cell = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if int(cell.get("trades") or 0) < min_samples:
            continue
        return {
            "wr":       float(cell.get("wr") or 0.5),
            "trades":   int(cell.get("trades") or 0),
            "midpoint": float(cell.get("midpoint") or 0.5),
            "source":   source,
        }
    return None


async def _persist(table: dict) -> None:
    redis = get_redis()
    # Overwrite atomically: delete then HSET. The whole table is rebuilt
    # each run, so partial leftovers from a smaller previous run would
    # otherwise pollute the lookup (stale `cat:WAR:0.7-0.8` long after
    # WAR trades aged out of the Reviewer's stats).
    pipe = redis.pipeline()
    pipe.delete("calibration:conviction_table")
    if table["fields"]:
        pipe.hset("calibration:conviction_table", mapping=table["fields"])
    pipe.delete("calibration:meta")
    pipe.hset("calibration:meta", mapping={
        k: (json.dumps(v) if not isinstance(v, (str, int, float)) else str(v))
        for k, v in table["meta"].items()
    })
    await pipe.execute()


async def run_once() -> dict:
    """One pass: build, persist, log. Returns the meta blob."""
    redis = get_redis()
    try:
        cfg = await redis.hgetall("risk_config") or {}
        min_samples = int(cfg.get("calibrator_min_samples")
                          or _DEFAULT_MIN_SAMPLES)
    except Exception:  # noqa: BLE001
        min_samples = _DEFAULT_MIN_SAMPLES
    table = await build_table(min_samples=min_samples)
    await _persist(table)
    _log.info("calibrator_run",
              cells=table["meta"]["cells_written"],
              trades=table["meta"]["trades_observed"],
              largest_delta=table["meta"]["largest_wr_delta"])
    return table["meta"]


async def run() -> None:
    """Background loop. Sleep → run_once → sleep, forever.

    Honors `calibrator_enabled` (default true) and `calibrator_interval_min`
    (default 30) from the live risk_config hash so the operator can
    pause / change the cadence from /settings/ without redeploying.
    """
    await asyncio.sleep(_INITIAL_DELAY_SEC)
    while True:
        interval_sec = _DEFAULT_INTERVAL_SEC
        try:
            cfg = await get_redis().hgetall("risk_config") or {}
            enabled = (cfg.get("calibrator_enabled") or "true").lower() in (
                "true", "1", "yes", "on",
            )
            interval_min = int(cfg.get("calibrator_interval_min") or 30)
            interval_sec = max(60, interval_min * 60)
            if enabled:
                await run_once()
            else:
                _log.info("calibrator_disabled_skipping")
        except Exception as exc:  # noqa: BLE001
            _log.warning("calibrator_loop_error", error=str(exc)[:200])
        await asyncio.sleep(interval_sec)
