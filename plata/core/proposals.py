"""Proposal persistence helpers.

Every TradeProposal the strategist publishes is mirrored into Postgres
(`proposals` table) so the dashboard can show its full lifecycle — from
publication through risk decision, HITL, and execution / rejection.

State transitions:
    published       — strategist published it
    rejected        — risk_manager rejected (reason in state_reason)
    pending_hitl    — needs human approval
    hitl_approved   — human said yes; passed back to risk for sizing
    hitl_rejected   — human said no
    hitl_timeout    — 60 min elapsed without a decision
    approved        — risk_manager sized + approved (about to execute)
    executed        — executor opened the trade (trade_ulid set)
    failed_execution — venue order failed
    manual_override — user cloned & re-submitted (bypasses risk gates)
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from plata.core.db import Proposal, session_scope
from plata.core.observability import get_logger

_log = get_logger("proposals_store")
# Surface the FIRST failure loudly (so we don't silently drop everything if
# the table is missing); after that, downgrade to debug to avoid log spam.
_warned_drop = False
_warned_published = False
_table_create_attempted = False  # avoid hammering ensure_aux_tables() on every failure


async def _persist_failure_to_redis(error: str, context: dict) -> None:
    """Write the last persistence error to Redis so the proposals page
    diagnostic banner can surface it inline — instead of asking the user
    to read logs."""
    try:
        from datetime import datetime, timezone
        from plata.core.bus import get_redis
        redis = get_redis()
        payload = {
            "error": error[:500],
            "ts": datetime.now(timezone.utc).isoformat(),
            **{k: str(v)[:200] for k, v in context.items() if v is not None},
        }
        await redis.hset("proposals:last_persist_error", mapping=payload)
        await redis.expire("proposals:last_persist_error", 7 * 24 * 60 * 60)
        # Bump a counter so the banner can show "47 drops failed to persist".
        await redis.incr("proposals:persist_failures_total")
    except Exception:  # noqa: BLE001 — never break the caller
        pass


async def _maybe_self_heal_tables(exc: Exception) -> bool:
    """If the persistence error looks like "relation does not exist",
    try to create the table ourselves and signal that a retry is worth it.
    Idempotent — won't loop, since we only attempt once per process."""
    global _table_create_attempted
    msg = str(exc).lower()
    looks_like_missing_table = "does not exist" in msg or "undefinedtable" in msg
    if not looks_like_missing_table or _table_create_attempted:
        return False
    _table_create_attempted = True
    try:
        from plata.core.db import ensure_aux_tables
        await ensure_aux_tables()
        _log.warning("proposals_table_self_healed_after_first_insert_failure")
        return True
    except Exception as inner:  # noqa: BLE001
        _log.error("proposals_table_self_heal_failed", error=str(inner)[:300])
        return False


async def record_published(proposal: Any) -> None:
    """Called by the strategist immediately after publishing to Redis.
    Self-heals the table on the first failure."""
    values = {
        "proposal_ulid": proposal.ulid,
        "triggering_event_ulid": getattr(proposal, "triggering_event_ulid", None),
        "symbol": (proposal.symbol or "")[:32],
        "side": str(proposal.side),
        "conviction": float(proposal.conviction) if proposal.conviction is not None else None,
        "suggested_sl_pct": proposal.suggested_sl_pct,
        "suggested_tp_pct": proposal.suggested_tp_pct,
        "reasoning": proposal.reasoning,
        "milestones": [m.model_dump(mode="json") for m in (proposal.milestones or [])],
        "analogs": [a.model_dump(mode="json") for a in (proposal.similar_events or [])],
        "state": "published",
        "last_actor": "strategist",
    }
    err = await _record_drop_attempt(values)
    if err is None:
        return
    if await _maybe_self_heal_tables(err):
        err = await _record_drop_attempt(values)
        if err is None:
            _log.warning("record_published_succeeded_after_self_heal", ulid=proposal.ulid)
            return
    await _persist_failure_to_redis(str(err), {
        "ulid": getattr(proposal, "ulid", None), "fn": "record_published",
    })
    global _warned_published
    if not _warned_published:
        _log.error("record_published_first_failure",
                   ulid=getattr(proposal, "ulid", None), error=str(err)[:400])
        _warned_published = True


async def update_state(
    proposal_ulid: str,
    *,
    state: str,
    reason: str | None = None,
    actor: str | None = None,
    trade_ulid: str | None = None,
    extras: dict | None = None,
) -> None:
    """Move a proposal to a new state. Idempotent."""
    try:
        async with session_scope() as session:
            row = (await session.execute(
                select(Proposal).where(Proposal.proposal_ulid == proposal_ulid)
            )).scalar_one_or_none()
            if row is None:
                # Strategist might not have recorded it yet (race) — skip,
                # the next call from later in the pipeline will re-create.
                _log.debug("update_state_no_row", ulid=proposal_ulid, state=state)
                return
            row.state = state
            if reason is not None:
                row.state_reason = reason[:255]
            if actor is not None:
                row.last_actor = actor[:128]
            if trade_ulid is not None:
                row.trade_ulid = trade_ulid
            if extras is not None:
                merged = dict(row.extras or {})
                merged.update(extras)
                row.extras = merged
    except Exception as exc:  # noqa: BLE001
        _log.warning("update_state_failed", ulid=proposal_ulid, state=state, error=str(exc)[:160])


async def list_recent(
    *,
    state: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[Proposal]:
    """List proposals newest-first, optionally filtered + paginated."""
    try:
        async with session_scope() as session:
            q = select(Proposal).order_by(Proposal.created_at.desc())
            if state:
                q = q.where(Proposal.state == state)
            if symbol:
                q = q.where(Proposal.symbol == symbol)
            q = q.offset(max(0, offset)).limit(limit)
            return (await session.execute(q)).scalars().all()
    except Exception as exc:  # noqa: BLE001
        _log.warning("list_recent_failed", error=str(exc)[:160])
        return []


async def count_recent(
    *,
    state: str | None = None,
    symbol: str | None = None,
) -> int:
    """Total row count matching the same filters as list_recent — used for
    pagination's `total`/`pages` math."""
    from sqlalchemy import func as _func
    try:
        async with session_scope() as session:
            q = select(_func.count()).select_from(Proposal)
            if state:
                q = q.where(Proposal.state == state)
            if symbol:
                q = q.where(Proposal.symbol == symbol)
            return int((await session.execute(q)).scalar() or 0)
    except Exception as exc:  # noqa: BLE001
        _log.warning("count_recent_failed", error=str(exc)[:160])
        return 0


async def _record_drop_attempt(values: dict) -> Exception | None:
    """Single insert attempt. Returns the exception on failure, None on success."""
    try:
        async with session_scope() as session:
            stmt = insert(Proposal).values(**values).on_conflict_do_nothing(
                index_elements=["proposal_ulid"]
            )
            await session.execute(stmt)
        # First successful write after a streak of failures should clear the
        # sysop banner — otherwise the "1h ago" stale message lingers for
        # 7 days. Cheap del; tolerate Redis errors silently.
        try:
            from plata.core.bus import get_redis
            await get_redis().delete("proposals:last_persist_error")
        except Exception:  # noqa: BLE001
            pass
        return None
    except Exception as exc:  # noqa: BLE001
        return exc


async def record_drop(
    *,
    event_ulid: str,
    reason_code: str,
    reason_human: str,
    symbol: str | None = None,
    side: str | None = None,
    conviction: float | None = None,
    reasoning: str | None = None,
    analogs: list[dict] | None = None,
    extras: dict | None = None,
) -> None:
    """Persist a 'dropped' Proposal row capturing why the strategist
    rejected this event. These rows are normal lifecycle entries — every
    event that doesn't become a published trade becomes a `dropped` row
    with the reason. Self-heals the table on the first failure (in case a
    fresh service started before tables were created)."""
    if not event_ulid:
        return
    values = {
        # Use event_ulid as the PK so each event has at most one drop row.
        # Real published proposals get their own random ULID, so no clash.
        "proposal_ulid": event_ulid[:26],
        "triggering_event_ulid": event_ulid,
        "symbol": (symbol or "—")[:32],
        "side": (side or "long")[:8],
        "conviction": conviction,
        "reasoning": (reasoning or reason_human)[:1500],
        "state": "dropped",
        "state_reason": f"{reason_code}: {reason_human}"[:255],
        "last_actor": "strategist",
        "analogs": analogs or [],
        "extras": extras or {"drop_reason_code": reason_code},
    }
    err = await _record_drop_attempt(values)
    if err is None:
        return
    # Self-heal: if the table is missing, create it then retry once.
    if await _maybe_self_heal_tables(err):
        err = await _record_drop_attempt(values)
        if err is None:
            _log.warning("record_drop_succeeded_after_self_heal", event=event_ulid)
            return
    # Still failing — record the cause for the UI banner; logs stay quiet.
    await _persist_failure_to_redis(str(err), {
        "event_ulid": event_ulid, "code": reason_code, "fn": "record_drop",
    })
    global _warned_drop
    if not _warned_drop:
        _log.error("record_drop_first_failure", event=event_ulid,
                   code=reason_code, error=str(err)[:400])
        _warned_drop = True


async def get(proposal_ulid: str) -> Proposal | None:
    try:
        async with session_scope() as session:
            return (await session.execute(
                select(Proposal).where(Proposal.proposal_ulid == proposal_ulid)
            )).scalar_one_or_none()
    except Exception as exc:  # noqa: BLE001
        _log.warning("get_failed", ulid=proposal_ulid, error=str(exc)[:160])
        return None
