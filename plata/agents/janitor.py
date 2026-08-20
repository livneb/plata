"""Janitor agent — periodic data-retention sweeps across Redis + Postgres.

Why this exists: every storage layer in Plata was append-only in practice.
Streams were never XTRIM'd (heartbeats alone add ~100k entries/day), graph
event nodes carry 1024-float embeddings and were kept forever, and Postgres
tables like signal_archive / error_log / audit_log grew without bound. That
bloat is what slows dashboard pages and swells the Redis RDB.

Design (mirrors the sysop agent's conventions):
  • Deterministic, no LLM. Every sweep is bounded per run so a huge backlog
    is drained over several cycles instead of one long stop-the-world pass.
  • Config lives in the Redis hash `janitor_config` (ints; 0 = keep forever /
    disabled). Anything not set falls back to DEFAULTS below.
  • Every run writes a JSON summary to `janitor:last_run` and appends to the
    `janitor:history` list (last 30 runs) so the dashboard can show what was
    reclaimed. A manual sweep can be requested by setting `janitor:run_now=1`
    (see /controls/janitor/run_now).

What is deliberately NOT touched:
  • trade_ledger, backtest_runs/trades, users, api_credentials — system of
    record / small.
  • proposals that became real trades (trade_ulid set) — the learning set.
  • lesson:* graph nodes — the distilled learning; tiny and high-value.
  • entity:* graph nodes — bounded by the entity universe, sentiment EWMA
    history matters.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, select, text

from plata.core.bus import Streams, get_redis, trim_stream
from plata.core.observability import get_logger

_log = get_logger("janitor")

STARTUP_DELAY_SEC = 90
RUN_NOW_POLL_SEC = 30

# All first-class streams plus their DLQs get trimmed each run.
_TRIMMED_STREAMS: tuple[tuple[str, str], ...] = (
    # (stream, config key for maxlen)
    (Streams.HEARTBEATS, "stream_heartbeats_maxlen"),
    (Streams.RAW_SIGNALS, "stream_signals_maxlen"),
    (Streams.ENRICHED_EVENTS, "stream_signals_maxlen"),
    (Streams.TRADING_PROPOSALS, "stream_trading_maxlen"),
    (Streams.RISK_DECISIONS, "stream_trading_maxlen"),
    (Streams.APPROVED_TRADES, "stream_trading_maxlen"),
    (Streams.EXECUTED_TRADES, "stream_trading_maxlen"),
    (Streams.TRADE_CLOSURES, "stream_trading_maxlen"),
    (Streams.HISTORIAN_RESEARCH_REQUESTS, "stream_trading_maxlen"),
)

DEFAULTS: dict[str, int] = {
    # -- scheduling ---------------------------------------------------------
    "interval_hours": 6,
    # -- Redis streams (maxlen, approximate trim) ---------------------------
    "stream_heartbeats_maxlen": 5_000,     # ~4h of 12 agents @ 10s
    "stream_signals_maxlen": 20_000,       # raw_signals + enriched_events
    "stream_trading_maxlen": 10_000,       # proposals/decisions/trades/closures
    "stream_dlq_maxlen": 2_000,            # per-DLQ; inspect on /dlq/ before it rolls
    # -- Knowledge graph (Redis Stack) ---------------------------------------
    # Events WITHOUT measured price impact are pure news noise after a while.
    "graph_event_retention_days": 120,
    # Events WITH price_impact are the historical-analog training set — keep
    # much longer.
    "graph_event_impact_retention_days": 365,
    "graph_event_delete_cap_per_run": 5_000,
    # Edge evidence lists grow unbounded (one entry per co-mention).
    "graph_edge_evidence_cap": 50,
    # High cap so a normal keyspace completes a FULL edge pass in one run —
    # only a complete pass may set graph:edgeidx_ready (see _sweep_graph_edges).
    "graph_edge_scan_cap_per_run": 500_000,
    # -- Postgres retention (days; 0 = keep forever) -------------------------
    "signal_archive_days": 180,
    "signal_archive_duplicate_days": 30,   # is_duplicate=true rows die sooner
    "error_log_resolved_days": 30,
    "error_log_days": 90,
    "audit_log_days": 365,
    "agent_activity_log_days": 30,
    "sysop_findings_days": 60,             # only non-`new` states
    "event_price_window_days": 365,
    "llm_cost_days": 400,
    # `dropped` rows are pure diagnostics — the strategist writes one for
    # EVERY event it rejects, so they dominate the table (tens of thousands).
    # Keep exactly the last week so recent decisions can still be studied.
    "proposals_dropped_days": 7,
    # Proposals that never became a trade (rejected / hitl_rejected /
    # timed out / stale). Ones WITH a trade_ulid are kept forever — they're
    # the learning set tied to trade_ledger.
    "proposals_days": 7,
    "config_settings_keep_versions": 20,   # per key
    # -- Postgres batching ----------------------------------------------------
    "pg_delete_batch": 5_000,
    "pg_delete_max_batches": 40,
}


def merge_config(overrides: dict[str, Any] | None) -> dict[str, int]:
    """DEFAULTS + Redis-hash overrides. Non-numeric / negative values are
    ignored so a fat-fingered HSET can't wedge the sweeps."""
    cfg = dict(DEFAULTS)
    for key, raw in (overrides or {}).items():
        if key not in DEFAULTS:
            continue
        try:
            val = int(float(raw))
        except (TypeError, ValueError):
            continue
        if val < 0:
            continue
        cfg[key] = val
    return cfg


async def get_config() -> dict[str, int]:
    redis = get_redis()
    try:
        overrides = await redis.hgetall("janitor_config")
    except Exception:  # noqa: BLE001
        overrides = {}
    return merge_config(overrides)


def cutoff_utc(days: int, *, now: datetime | None = None) -> datetime:
    return (now or datetime.now(timezone.utc)) - timedelta(days=days)


# ---------------------------------------------------------------------------
# Redis sweeps
# ---------------------------------------------------------------------------

async def _sweep_streams(cfg: dict[str, int]) -> dict[str, int]:
    """XTRIM every known stream + its DLQ. Returns {stream: trimmed_count}."""
    redis = get_redis()
    trimmed: dict[str, int] = {}
    for stream, cfg_key in _TRIMMED_STREAMS:
        maxlen = cfg[cfg_key]
        if maxlen <= 0:
            continue
        try:
            n = await trim_stream(stream, maxlen=maxlen)
            if n:
                trimmed[stream] = n
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_stream_trim_failed", stream=stream, error=str(exc)[:120])
        dlq = Streams.dlq_for(stream)
        dlq_maxlen = cfg["stream_dlq_maxlen"]
        if dlq_maxlen <= 0:
            continue
        try:
            if await redis.exists(dlq):
                n = await trim_stream(dlq, maxlen=dlq_maxlen)
                if n:
                    trimmed[dlq] = n
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_dlq_trim_failed", stream=dlq, error=str(exc)[:120])
    return trimmed


async def _sweep_graph_events(cfg: dict[str, int]) -> dict[str, int]:
    """Delete aged event:* nodes.

    Two tiers:
      • no measured price impact → graph_event_retention_days
      • has price_impact         → graph_event_impact_retention_days
    Lessons + entities are never touched. Deletion also drops the node from
    the RediSearch index, shrinking the HNSW graph the strategist queries.
    """
    from redis.commands.search.query import Query

    from plata.core.graph import EVENT_INDEX

    retention = cfg["graph_event_retention_days"]
    if retention <= 0:
        return {"deleted": 0, "kept_with_impact": 0}
    impact_retention = max(cfg["graph_event_impact_retention_days"], retention)
    cap = cfg["graph_event_delete_cap_per_run"]

    redis = get_redis()
    now = datetime.now(timezone.utc)
    cutoff_epoch = int(cutoff_utc(retention, now=now).timestamp())
    impact_cutoff_epoch = int(cutoff_utc(impact_retention, now=now).timestamp())

    deleted = 0
    kept: set[str] = set()
    # Page from offset 0 every time: deletions shift the result window, and
    # docs we keep (recent impact) are skipped via the growing offset.
    offset = 0
    page = 200
    while deleted < cap:
        q = (
            Query(f"@ts_epoch:[0 {cutoff_epoch}]")
            .sort_by("ts_epoch", asc=True)
            .return_fields("ulid", "ts_epoch")
            .dialect(2)
            .paging(offset, page)
        )
        try:
            result = await redis.ft(EVENT_INDEX).search(q)
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_graph_search_failed", error=str(exc)[:120])
            break
        if not result.docs:
            break
        page_deleted = 0
        for doc in result.docs:
            if deleted >= cap:
                break
            key = doc.id
            if key in kept:
                continue
            try:
                ts_epoch = int(float(getattr(doc, "ts_epoch", 0) or 0))
                has_impact = False
                impact = await redis.json().get(key, "$.price_impact")
                # JSONPath returns a list of matches; non-empty dict = has data.
                if impact and any(bool(m) for m in impact):
                    has_impact = True
                if has_impact and ts_epoch > impact_cutoff_epoch:
                    kept.add(key)
                    continue
                # Delete the event's outgoing edges too (via its edgeidx SET)
                # so they don't linger as keyspace-bloating orphans.
                from plata.core.graph import edge_index_key
                idx_key = edge_index_key(key)
                edge_keys = await redis.smembers(idx_key) or set()
                if edge_keys:
                    await redis.delete(*edge_keys)
                await redis.delete(idx_key, key)
                deleted += 1
                page_deleted += 1
            except Exception as exc:  # noqa: BLE001
                # Conservative: on error we keep the doc (and stop retrying it
                # this run) rather than risk deleting something we misread.
                kept.add(key)
                _log.warning("janitor_graph_delete_failed", key=key, error=str(exc)[:120])
        if page_deleted == 0:
            # Everything in this window was kept — advance past it.
            offset += page
    return {"deleted": deleted, "kept_with_impact": len(kept)}


async def _sweep_graph_edges(cfg: dict[str, int]) -> dict[str, int]:
    """Full edge maintenance pass. For every edge:* key (batched, pipelined):
      • orphan cleanup — an edge whose src event no longer exists is deleted
        (event deletions used to leave these behind forever, bloating the
        keyspace and slowing every SCAN in the system);
      • edgeidx backfill — SADD the edge into edgeidx:{src} so readers can
        stop scanning (see plata.core.graph.edge_index_key);
      • evidence cap — trim evidence_event_ids to the most recent `cap`.
    Sets graph:edgeidx_ready after a COMPLETE pass, unlocking the fast
    SMEMBERS path in graph.neighbors() / edge_keys_for_sources()."""
    from plata.core.graph import EDGE_INDEX_READY_KEY, edge_index_key

    cap = cfg["graph_edge_evidence_cap"]
    scan_cap = cfg["graph_edge_scan_cap_per_run"]
    redis = get_redis()
    scanned = 0
    capped = 0
    orphans_deleted = 0
    complete = True

    all_keys: list[str] = []
    async for key in redis.scan_iter(match="edge:*", count=5000):
        all_keys.append(key)
        if len(all_keys) >= scan_cap:
            complete = False
            break

    batch_size = 500
    for i in range(0, len(all_keys), batch_size):
        batch = all_keys[i:i + batch_size]
        scanned += len(batch)
        try:
            # Tiny pipelined reads: src + evidence length per edge.
            pipe = redis.pipeline()
            for k in batch:
                pipe.json().get(k, "$.src")
                pipe.json().arrlen(k, "$.evidence_event_ids")
            reads = await pipe.execute(raise_on_error=False)
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_edge_batch_read_failed", error=str(exc)[:120])
            complete = False
            continue

        srcs: dict[str, str] = {}
        needs_cap: list[str] = []
        for j, k in enumerate(batch):
            src_raw = reads[j * 2]
            len_raw = reads[j * 2 + 1]
            if isinstance(src_raw, list) and src_raw and isinstance(src_raw[0], str):
                srcs[k] = src_raw[0]
            if cap > 0 and isinstance(len_raw, list) and len_raw and \
                    isinstance(len_raw[0], int) and len_raw[0] > cap:
                needs_cap.append(k)

        # Orphan check for event-sourced edges, one pipelined EXISTS round.
        event_edges = [(k, s) for k, s in srcs.items() if s.startswith("event:")]
        orphans: list[tuple[str, str]] = []
        if event_edges:
            pipe = redis.pipeline()
            for _k, s in event_edges:
                pipe.exists(s)
            exist_flags = await pipe.execute(raise_on_error=False)
            # Only a clean EXISTS == 0 marks an orphan — an errored EXISTS
            # must never delete the edge.
            orphans = [pair for pair, ex in zip(event_edges, exist_flags) if ex == 0]

        orphan_keys = {k for k, _ in orphans}
        pipe = redis.pipeline()
        for k, s in orphans:
            pipe.delete(k)
            pipe.srem(edge_index_key(s), k)
        # Backfill the index for every surviving edge.
        for k, s in srcs.items():
            if k not in orphan_keys:
                pipe.sadd(edge_index_key(s), k)
        try:
            await pipe.execute(raise_on_error=False)
            orphans_deleted += len(orphans)
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_edge_batch_write_failed", error=str(exc)[:120])
            complete = False

        for k in needs_cap:
            if k in orphan_keys:
                continue
            try:
                doc = await redis.json().get(k)
                if not doc:
                    continue
                evidence = doc.get("evidence_event_ids") or []
                doc["evidence_event_ids"] = evidence[-cap:]
                await redis.json().set(k, "$", doc)
                capped += 1
            except Exception:  # noqa: BLE001
                continue

    if complete:
        try:
            await redis.set(EDGE_INDEX_READY_KEY, "1")
        except Exception:  # noqa: BLE001
            pass
    return {"scanned": scanned, "capped": capped,
            "orphans_deleted": orphans_deleted, "complete": int(complete)}


# ---------------------------------------------------------------------------
# Postgres sweeps
# ---------------------------------------------------------------------------

async def _batched_delete(model, id_col, condition, cfg: dict[str, int]) -> int:
    """DELETE in bounded batches so we never hold a long lock or bloat one
    transaction. Returns total rows deleted."""
    from plata.core.db import session_scope

    batch = max(cfg["pg_delete_batch"], 100)
    max_batches = max(cfg["pg_delete_max_batches"], 1)
    total = 0
    for _ in range(max_batches):
        async with session_scope() as session:
            subq = select(id_col).where(condition).limit(batch)
            result = await session.execute(
                delete(model).where(id_col.in_(subq.scalar_subquery()))
            )
            deleted = result.rowcount or 0
        total += deleted
        if deleted < batch:
            break
    return total


async def _sweep_postgres(cfg: dict[str, int]) -> dict[str, int]:
    from plata.core.db import (
        AgentActivityLog,
        AuditLog,
        ErrorLog,
        EventPriceWindow,
        LLMCost,
        Proposal,
        SignalArchive,
        SysopFinding,
        session_scope,
    )

    out: dict[str, int] = {}
    now = datetime.now(timezone.utc)

    async def run_sweep(name: str, coro) -> None:
        try:
            out[name] = await coro
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_pg_sweep_failed", sweep=name, error=str(exc)[:160])
            out[name] = -1  # sentinel: sweep errored

    if cfg["signal_archive_duplicate_days"] > 0:
        await run_sweep("signal_archive_duplicates", _batched_delete(
            SignalArchive, SignalArchive.id,
            (SignalArchive.is_duplicate.is_(True))
            & (SignalArchive.fetched_at < cutoff_utc(cfg["signal_archive_duplicate_days"], now=now)),
            cfg,
        ))
    if cfg["signal_archive_days"] > 0:
        await run_sweep("signal_archive", _batched_delete(
            SignalArchive, SignalArchive.id,
            SignalArchive.fetched_at < cutoff_utc(cfg["signal_archive_days"], now=now),
            cfg,
        ))
    if cfg["error_log_resolved_days"] > 0:
        await run_sweep("error_log_resolved", _batched_delete(
            ErrorLog, ErrorLog.id,
            (ErrorLog.resolved.is_(True))
            & (ErrorLog.ts < cutoff_utc(cfg["error_log_resolved_days"], now=now)),
            cfg,
        ))
    if cfg["error_log_days"] > 0:
        await run_sweep("error_log", _batched_delete(
            ErrorLog, ErrorLog.id,
            ErrorLog.ts < cutoff_utc(cfg["error_log_days"], now=now),
            cfg,
        ))
    if cfg["audit_log_days"] > 0:
        await run_sweep("audit_log", _batched_delete(
            AuditLog, AuditLog.id,
            AuditLog.ts < cutoff_utc(cfg["audit_log_days"], now=now),
            cfg,
        ))
    if cfg["agent_activity_log_days"] > 0:
        await run_sweep("agent_activity_log", _batched_delete(
            AgentActivityLog, AgentActivityLog.id,
            AgentActivityLog.ts < cutoff_utc(cfg["agent_activity_log_days"], now=now),
            cfg,
        ))
    if cfg["sysop_findings_days"] > 0:
        await run_sweep("sysop_findings", _batched_delete(
            SysopFinding, SysopFinding.id,
            (SysopFinding.state != "new")
            & (SysopFinding.updated_at < cutoff_utc(cfg["sysop_findings_days"], now=now)),
            cfg,
        ))
    if cfg["event_price_window_days"] > 0:
        await run_sweep("event_price_windows", _batched_delete(
            EventPriceWindow, EventPriceWindow.id,
            EventPriceWindow.event_ts < cutoff_utc(cfg["event_price_window_days"], now=now),
            cfg,
        ))
    if cfg["llm_cost_days"] > 0:
        await run_sweep("llm_cost", _batched_delete(
            LLMCost, LLMCost.id,
            LLMCost.ts < cutoff_utc(cfg["llm_cost_days"], now=now),
            cfg,
        ))
    if cfg["proposals_dropped_days"] > 0:
        # Diagnostic drop records (state=dropped) — one per rejected event.
        await run_sweep("proposals_dropped", _batched_delete(
            Proposal, Proposal.proposal_ulid,
            (Proposal.state == "dropped")
            & (Proposal.created_at < cutoff_utc(cfg["proposals_dropped_days"], now=now)),
            cfg,
        ))
    if cfg["proposals_days"] > 0:
        # Anything that never became a trade. Proposals WITH a trade_ulid
        # are never deleted here — they document why real trades happened.
        await run_sweep("proposals", _batched_delete(
            Proposal, Proposal.proposal_ulid,
            (Proposal.trade_ulid.is_(None))
            & (Proposal.created_at < cutoff_utc(cfg["proposals_days"], now=now)),
            cfg,
        ))
    keep_versions = cfg["config_settings_keep_versions"]
    if keep_versions > 0:
        async def _config_versions() -> int:
            async with session_scope() as session:
                result = await session.execute(text(
                    "DELETE FROM config_settings WHERE id IN ("
                    "  SELECT id FROM ("
                    "    SELECT id, ROW_NUMBER() OVER ("
                    "      PARTITION BY key ORDER BY id DESC) AS rn"
                    "    FROM config_settings) ranked"
                    "  WHERE rn > :keep)"
                ), {"keep": keep_versions})
                return result.rowcount or 0
        await run_sweep("config_settings_versions", _config_versions())
    return out


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def summarize(summary: dict[str, Any]) -> str:
    """One activity-feed line: only the non-zero work."""
    parts: list[str] = []
    trimmed = summary.get("streams") or {}
    if trimmed:
        parts.append(f"trimmed {sum(trimmed.values()):,} stream entries")
    graph = summary.get("graph_events") or {}
    if graph.get("deleted"):
        parts.append(f"aged out {graph['deleted']:,} graph events")
    edges = summary.get("graph_edges") or {}
    if edges.get("orphans_deleted"):
        parts.append(f"removed {edges['orphans_deleted']:,} orphan edges")
    if edges.get("capped"):
        parts.append(f"capped {edges['capped']:,} edge evidence lists")
    pg = summary.get("postgres") or {}
    pg_total = sum(v for v in pg.values() if isinstance(v, int) and v > 0)
    if pg_total:
        parts.append(f"deleted {pg_total:,} Postgres rows")
    if not parts:
        return "Retention sweep: nothing to clean"
    return "Retention sweep: " + ", ".join(parts)


async def run_once() -> dict[str, Any]:
    started = time.monotonic()
    cfg = await get_config()
    summary: dict[str, Any] = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }
    summary["streams"] = await _sweep_streams(cfg)
    summary["graph_events"] = await _sweep_graph_events(cfg)
    summary["graph_edges"] = await _sweep_graph_edges(cfg)
    summary["postgres"] = await _sweep_postgres(cfg)
    summary["duration_ms"] = int((time.monotonic() - started) * 1000)

    redis = get_redis()
    try:
        body = json.dumps(summary, default=str)
        await redis.set("janitor:last_run", body)
        await redis.set("janitor:last_run_at", summary["ran_at"])
        await redis.lpush("janitor:history", body)
        await redis.ltrim("janitor:history", 0, 29)
    except Exception as exc:  # noqa: BLE001
        _log.warning("janitor_summary_store_failed", error=str(exc)[:120])
    try:
        from plata.agents.base import log_action
        await log_action("janitor", summarize(summary))
    except Exception:  # noqa: BLE001
        pass
    _log.info("janitor_run_complete", duration_ms=summary["duration_ms"],
              summary=summarize(summary))
    return summary


async def run() -> None:
    """Main loop. Sweeps every `interval_hours` (default 6), or immediately
    when `janitor:run_now` is set (by /controls/janitor/run_now)."""
    await asyncio.sleep(STARTUP_DELAY_SEC)
    redis = get_redis()
    while True:
        due = False
        try:
            if await redis.get("janitor:run_now"):
                await redis.delete("janitor:run_now")
                due = True
            else:
                cfg = await get_config()
                interval = max(cfg["interval_hours"], 1) * 3600
                last_at_raw = await redis.get("janitor:last_run_at")
                if not last_at_raw:
                    due = True
                else:
                    try:
                        last_at = datetime.fromisoformat(last_at_raw)
                        due = (datetime.now(timezone.utc) - last_at).total_seconds() >= interval
                    except ValueError:
                        due = True
        except Exception as exc:  # noqa: BLE001
            _log.warning("janitor_schedule_check_failed", error=str(exc)[:120])
        if due:
            try:
                await run_once()
            except Exception as exc:  # noqa: BLE001
                _log.warning("janitor_run_failed", error=str(exc)[:160])
        await asyncio.sleep(RUN_NOW_POLL_SEC)
