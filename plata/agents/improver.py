"""Nightly self-improvement digest.

Surveys the system state (errors, agent health, news pipeline, recent
trades) and writes a single ErrorLog row (severity=INFO) summarising
what looks broken and one or two concrete suggestions. The row appears
on `/errors/` like any other alert, so the user sees it without having
to look for it.

This is intentionally NOT an auto-fixer. It surfaces problems and
suggests fixes; the user (or the LLM via HITL) decides. v1 is a
deterministic survey; a future version can hand the survey to an LLM
and have it propose specific config tweaks via the existing tuning page.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from plata.core.bus import get_redis
from plata.core.db import ErrorLog, LLMCost, Proposal, TradeLedger, session_scope
from plata.core.observability import get_logger
from sqlalchemy import desc, func, select

_log = get_logger("improver")

# Run once at boot then every 24h.
_INITIAL_DELAY_SEC = 5 * 60
_INTERVAL_SEC = 24 * 60 * 60


async def _survey() -> tuple[list[str], list[str]]:
    """Return (findings, suggestions). Each is a list of short strings."""
    findings: list[str] = []
    suggestions: list[str] = []
    now = datetime.now(timezone.utc)
    redis = get_redis()

    # --- Agent health
    stale = []
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        data = await redis.hgetall(k)
        hb = data.get("last_heartbeat")
        if not hb:
            continue
        try:
            age = (now - datetime.fromisoformat(hb)).total_seconds()
        except Exception:  # noqa: BLE001
            continue
        if age > 300:
            stale.append((k.split(":")[-1], int(age // 60)))
    if stale:
        names = ", ".join(f"{n} ({m}m)" for n, m in stale[:5])
        findings.append(f"{len(stale)} agent(s) have stale heartbeats: {names}.")
        suggestions.append("Restart the affected Railway containers (intelligence_sandbox / execution_vault / ingestion_hub).")

    # --- News pipeline output
    pub_total = 0
    async for sk in redis.scan_iter(match="scraper:source:*", count=100):
        if sk.endswith(":log") or sk.endswith(":probe"):
            continue
        h = await redis.hgetall(sk)
        try:
            pub_total += int(h.get("lifetime_published") or 0)
        except (TypeError, ValueError):
            pass
    if pub_total == 0:
        findings.append("No scraper source has ever published a signal. The strategist has nothing to act on.")
        suggestions.append("Open /news/ and run each source with ▶ Run now; check the Diagnosis column for the HTTP status / error.")

    # --- Proposals in last 24h
    async with session_scope() as session:
        cutoff = now - timedelta(hours=24)
        prop_cnt = (await session.execute(
            select(func.count(Proposal.id)).where(Proposal.created_at >= cutoff)
        )).scalar() or 0
        traded_cnt = (await session.execute(
            select(func.count(Proposal.id))
            .where(Proposal.created_at >= cutoff, Proposal.state.in_(["executed", "adjustment_executed"]))
        )).scalar() or 0
        closed_cnt = (await session.execute(
            select(func.count(TradeLedger.id)).where(TradeLedger.closed_at >= cutoff)
        )).scalar() or 0
        pnl_sum = (await session.execute(
            select(func.coalesce(func.sum(TradeLedger.net_pnl), 0))
            .where(TradeLedger.closed_at >= cutoff)
        )).scalar() or 0
        spend_sum = (await session.execute(
            select(func.coalesce(func.sum(LLMCost.cost_usd), 0))
            .where(LLMCost.ts >= cutoff)
        )).scalar() or 0
    if prop_cnt == 0:
        findings.append("Zero proposals were created in the last 24h.")
        suggestions.append("Likely the news pipeline is silent (see above) or the sentiment_magnitude threshold is too high — try lowering `min_sentiment_magnitude` to 0.3 on /settings/?tab=risk.")
    elif traded_cnt == 0:
        findings.append(f"{prop_cnt} proposals in 24h but none traded.")
        suggestions.append("Open /proposals/ → filter 'Why not traded' to see the rejection breakdown (likely conviction floor, exposure cap, or dedup).")
    if closed_cnt and float(pnl_sum) < 0:
        findings.append(f"24h realized PnL is ${float(pnl_sum):.2f} on {closed_cnt} closed trades.")
        suggestions.append("Review losing trades on /trades/?status=closed — the Reviewer's bucket stats can suggest a `guard_*` tweak (see /tuning/).")

    # --- LLM spend trend
    if float(spend_sum) > 0:
        findings.append(f"24h LLM spend: ${float(spend_sum):.4f}.")
        if float(spend_sum) > 5.0:
            suggestions.append("Spend > $5/day — consider switching `llm_mode` to `auto` on /settings/?tab=models so 402s auto-fall-back to free models.")

    # --- Recent errors
    async with session_scope() as session:
        cutoff = now - timedelta(hours=24)
        err_cnt = (await session.execute(
            select(func.count(ErrorLog.id))
            .where(ErrorLog.ts >= cutoff, ErrorLog.severity.in_(["ERROR", "CRITICAL"]))
        )).scalar() or 0
    if err_cnt > 5:
        findings.append(f"{err_cnt} ERROR/CRITICAL log rows in the last 24h.")
        suggestions.append("Open /errors/ and resolve the top ones; many will be the same root cause.")

    if not findings:
        findings.append("Pipeline looks healthy: agents are heartbeating, sources published, proposals were created, and PnL is non-negative.")
    return findings, suggestions


async def run() -> None:
    """Background task: sleep, survey, write digest. Repeat daily."""
    await asyncio.sleep(_INITIAL_DELAY_SEC)
    while True:
        try:
            findings, suggestions = await _survey()
            lines = ["📋 Nightly self-improvement digest"]
            lines.append("")
            lines.append("Findings:")
            for f in findings:
                lines.append(f"  • {f}")
            if suggestions:
                lines.append("")
                lines.append("Suggested fixes:")
                for s in suggestions:
                    lines.append(f"  → {s}")
            body = "\n".join(lines)
            async with session_scope() as session:
                session.add(ErrorLog(
                    container="dashboard",
                    agent="improver",
                    severity="WARN" if any("stale" in f.lower() or "zero" in f.lower()
                                            or "no scraper" in f.lower()
                                            for f in findings) else "INFO",
                    error_type="DailyDigest",
                    message=body[:8000],
                    context={"findings": findings, "suggestions": suggestions},
                ))
            _log.info("improver_digest_written",
                      findings=len(findings), suggestions=len(suggestions))
        except Exception as exc:  # noqa: BLE001
            _log.warning("improver_digest_failed", error=str(exc)[:160])
        await asyncio.sleep(_INTERVAL_SEC)
