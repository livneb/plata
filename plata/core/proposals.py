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


async def record_published(proposal: Any) -> None:
    """Called by the strategist immediately after publishing to Redis."""
    try:
        async with session_scope() as session:
            stmt = insert(Proposal).values(
                proposal_ulid=proposal.ulid,
                triggering_event_ulid=getattr(proposal, "triggering_event_ulid", None),
                symbol=proposal.symbol,
                side=str(proposal.side),
                conviction=float(proposal.conviction) if proposal.conviction is not None else None,
                suggested_sl_pct=proposal.suggested_sl_pct,
                suggested_tp_pct=proposal.suggested_tp_pct,
                reasoning=proposal.reasoning,
                milestones=[m.model_dump(mode="json") for m in (proposal.milestones or [])],
                analogs=[a.model_dump(mode="json") for a in (proposal.similar_events or [])],
                state="published",
                last_actor="strategist",
            ).on_conflict_do_nothing(index_elements=["proposal_ulid"])
            await session.execute(stmt)
    except Exception as exc:  # noqa: BLE001
        _log.warning("record_published_failed", ulid=getattr(proposal, "ulid", None), error=str(exc)[:160])


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
) -> list[Proposal]:
    """List proposals newest-first, optionally filtered."""
    try:
        async with session_scope() as session:
            q = select(Proposal).order_by(Proposal.created_at.desc()).limit(limit)
            if state:
                q = q.where(Proposal.state == state)
            if symbol:
                q = q.where(Proposal.symbol == symbol)
            return (await session.execute(q)).scalars().all()
    except Exception as exc:  # noqa: BLE001
        _log.warning("list_recent_failed", error=str(exc)[:160])
        return []


async def get(proposal_ulid: str) -> Proposal | None:
    try:
        async with session_scope() as session:
            return (await session.execute(
                select(Proposal).where(Proposal.proposal_ulid == proposal_ulid)
            )).scalar_one_or_none()
    except Exception as exc:  # noqa: BLE001
        _log.warning("get_failed", ulid=proposal_ulid, error=str(exc)[:160])
        return None
