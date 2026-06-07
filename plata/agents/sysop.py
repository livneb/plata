"""Sysop agent — continuously monitors the system, detects known issues, and
either auto-fixes (with prior user approval) or surfaces an escalation with
copy-paste evidence the user can hand to a human / Claude.

Design principles:
  • Detection is deterministic pattern-matching (no LLM in the hot path).
  • Each finding has a stable `fingerprint` so re-detecting the same issue
    UPDATES the existing row instead of creating noise.
  • Fix actions are a curated whitelist (see FIX_REGISTRY). The user must
    explicitly approve each finding's fix on /sysop/ before it runs.
  • Findings carry enough raw evidence (probe data, log excerpts, agent
    statuses) that a human reading them can act without further digging.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Awaitable

from sqlalchemy import desc, func, select

from plata.core.bus import get_redis
from plata.core.db import ErrorLog, SysopFinding, session_scope
from plata.core.observability import get_logger

_log = get_logger("sysop")

INTERVAL_SEC = 5 * 60   # full sweep every 5 min
STARTUP_DELAY_SEC = 30


# -----------------------------------------------------------------------------
# Fix-action registry — the curated whitelist of safe automatic fixes.
# Each callable returns a short result string for the audit trail.
# Adding a new fix here = expanding what the sysop CAN do; it still won't run
# without the user's approve click on /sysop/.
# -----------------------------------------------------------------------------

async def _fix_force_resume_all(_args: dict) -> str:
    from plata.core.bus import Channels, publish_channel
    await publish_channel(Channels.SYSTEM_RESUME, {"reason": "sysop_auto_fix"})
    redis = get_redis()
    await redis.set("system:state", "RUNNING")
    cleared_sources = 0
    cleared_agents = 0
    async for k in redis.scan_iter(match="scraper:source:*", count=100):
        if k.endswith(":log") or k.endswith(":probe"):
            continue
        data = await redis.hgetall(k)
        if (data.get("status") or "").lower() == "halted" \
                and (data.get("halted_by") or "system") == "system":
            await redis.hset(k, mapping={"status": "idle", "halted_by": ""})
            cleared_sources += 1
    async for k in redis.scan_iter(match="agent_status:*", count=100):
        data = await redis.hgetall(k)
        if (data.get("halted") or "").lower() == "true":
            await redis.hset(k, "halted", "False")
            cleared_agents += 1
    return f"Resume broadcast; cleared {cleared_sources} sources, {cleared_agents} agent halt flags."


async def _fix_clear_venue_block(args: dict) -> str:
    redis = get_redis()
    venue = (args.get("venue") or "").strip()
    if not venue:
        return "no venue arg"
    await redis.delete(f"venue:blocked:{venue}")
    return f"Cleared venue:blocked:{venue}."


async def _fix_set_llm_mode_auto(_args: dict) -> str:
    redis = get_redis()
    await redis.hset("llm_config", "mode", "auto")
    return "Set llm_config.mode = auto."


async def _fix_clear_orchestrator_dead(args: dict) -> str:
    redis = get_redis()
    agent = (args.get("agent") or "").strip()
    if agent:
        await redis.hdel("orchestrator:dead_logged", agent)
        return f"Cleared dead_logged for {agent}."
    await redis.delete("orchestrator:dead_logged")
    return "Cleared all dead_logged markers."


async def _fix_resume_source(args: dict) -> str:
    redis = get_redis()
    name = (args.get("source") or "").strip()
    if not name:
        return "no source arg"
    await redis.hset(f"scraper:source:{name}",
                      mapping={"status": "idle", "halted_by": "", "run_now": "1"})
    return f"Resumed source {name} and queued an immediate run."


async def _fix_lower_sentiment_threshold(args: dict) -> str:
    """Lower min_sentiment_magnitude. Floor-protected: never goes below 0.1
    even if the args say to. Halves the current value when called via
    auto-apply with no explicit `new`."""
    redis = get_redis()
    cur_raw = await redis.hget("risk_config", "min_sentiment_magnitude")
    try:
        cur = float(cur_raw) if cur_raw else 0.5
    except (TypeError, ValueError):
        cur = 0.5
    if "new" in args:
        try:
            new = float(args["new"])
        except (TypeError, ValueError):
            new = cur / 2
    else:
        new = round(cur / 2, 3)
    new = max(0.1, min(new, 0.9))
    await redis.hset("risk_config", "min_sentiment_magnitude", str(new))
    return f"Set min_sentiment_magnitude {cur} → {new} (floor 0.1)."


async def _fix_restart_strategist_consume(_args: dict) -> str:
    """Mark the strategist's pending consume-group entries for re-delivery.
    Use when the strategist appears stuck on a backlog: claims pending items
    older than 5min back to itself, letting it process fresh ones. Safe — the
    same item just gets retried."""
    redis = get_redis()
    try:
        # Use the standard stream name
        from plata.core.bus import Streams as _S
        await redis.xautoclaim(
            _S.ENRICHED_EVENTS, "strategist", "strategist-recover",
            min_idle_time=5 * 60 * 1000, start_id="0-0", count=100,
        )
        return "Re-claimed strategist's stuck pending entries (≥5min idle)."
    except Exception as exc:  # noqa: BLE001
        return f"xautoclaim failed: {exc}"


FIX_REGISTRY: dict[str, Callable[[dict], Awaitable[str]]] = {
    "force_resume_all": _fix_force_resume_all,
    "clear_venue_block": _fix_clear_venue_block,
    "set_llm_mode_auto": _fix_set_llm_mode_auto,
    "clear_orchestrator_dead": _fix_clear_orchestrator_dead,
    "resume_source": _fix_resume_source,
    "lower_sentiment_threshold": _fix_lower_sentiment_threshold,
    "restart_strategist_consume": _fix_restart_strategist_consume,
}


# Fix actions that are SAFE to auto-apply without a human approval click.
# Rules of thumb to be in here:
#   - Cannot lose data
#   - Cannot place a trade
#   - Cannot incur cost
#   - Reverses cleanly OR has a built-in floor (so repeated calls plateau)
#   - The user can always undo it via the normal UI
AUTO_APPLY_SAFE: set[str] = {
    "lower_sentiment_threshold",   # halves the threshold, floor 0.1
    "clear_orchestrator_dead",     # only clears markers, no side effects
    "resume_source",               # safe — runner just polls
    "restart_strategist_consume",  # xautoclaim is idempotent
    "set_llm_mode_auto",           # only switches mode; user can flip back
}


# -----------------------------------------------------------------------------
# Detection patterns. Each returns 0+ candidate findings.
# A finding is a dict: {pattern, severity, title, evidence, proposed_fix,
#                       fix_action, fix_action_args, fingerprint}
# Fingerprint must be stable per logical issue so we update instead of dup.
# -----------------------------------------------------------------------------

async def _detect_stale_agents() -> list[dict[str, Any]]:
    out = []
    redis = get_redis()
    now = datetime.now(timezone.utc)
    stale: list[tuple[str, int, dict]] = []
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
            name = k.split(":")[-1]
            stale.append((name, int(age // 60), data))
    if not stale:
        return out
    container_map = {}
    for n, m, d in stale:
        c = d.get("container") or "?"
        container_map.setdefault(c, []).append((n, m))
    for container, members in container_map.items():
        evidence = {
            "stale_agents": [{"name": n, "minutes_stale": m} for n, m in members],
            "container": container,
        }
        critical = any(n in {"executor", "risk_manager"} for n, _ in members)
        out.append({
            "pattern": "stale_agents_per_container",
            "severity": "critical" if critical else "warn",
            "title": f"{len(members)} agent(s) stale in container `{container}`",
            "evidence": evidence,
            "proposed_fix": (
                f"Restart the `{container}` container on Railway. Resume from "
                "the dashboard cannot bring back a dead process — only a "
                "container restart will."
            ),
            "fix_action": None,  # manual — only Railway can restart
            "fix_action_args": {},
            "fingerprint": _fp("stale_container", container),
        })
    return out


async def _detect_news_silent() -> list[dict[str, Any]]:
    out = []
    redis = get_redis()
    now = datetime.now(timezone.utc)
    latest = None
    per_source: list[dict] = []
    async for k in redis.scan_iter(match="scraper:source:*", count=100):
        if k.endswith(":log") or k.endswith(":probe"):
            continue
        data = await redis.hgetall(k)
        name = k.split(":")[-1]
        lp = data.get("last_poll_at")
        published = int(data.get("lifetime_published") or 0)
        per_source.append({
            "name": name,
            "last_poll_at": lp,
            "lifetime_published": published,
            "last_error": data.get("last_error", ""),
        })
        if lp:
            try:
                t = datetime.fromisoformat(lp)
                if latest is None or t > latest:
                    latest = t
            except Exception:  # noqa: BLE001
                pass
    if not per_source:
        return out
    silent_for_sec = (now - latest).total_seconds() if latest else 99999
    if silent_for_sec > 30 * 60:
        out.append({
            "pattern": "news_pipeline_silent",
            "severity": "critical",
            "title": f"No scraper has polled in {int(silent_for_sec // 60)} min",
            "evidence": {"per_source": per_source,
                          "latest_poll_at": str(latest) if latest else "never"},
            "proposed_fix": (
                "Most likely the ingestion_hub container is down. Restart it "
                "on Railway. If the container is fine, check each source's "
                "Diagnosis on /news/."
            ),
            "fix_action": None,
            "fix_action_args": {},
            "fingerprint": _fp("news_silent"),
        })
    # Sources with 0 published despite having polled many times
    zero_pub = [s for s in per_source if s["lifetime_published"] == 0]
    for s in zero_pub:
        # Pull the latest probe for evidence
        probe = await redis.hgetall(f"scraper:source:{s['name']}:probe") or {}
        s["last_probe"] = probe
        out.append({
            "pattern": "source_zero_published",
            "severity": "warn",
            "title": f"Source `{s['name']}` has never published a signal",
            "evidence": {"source": s["name"], "probe": probe,
                          "lifetime_published": 0,
                          "last_error": s.get("last_error", "")},
            "proposed_fix": _suggest_for_source(s["name"], probe, s.get("last_error", "")),
            "fix_action": "resume_source",
            "fix_action_args": {"source": s["name"]},
            "fingerprint": _fp("zero_pub", s["name"]),
        })
    return out


def _suggest_for_source(name: str, probe: dict, last_error: str) -> str:
    et = probe.get("error_type", "")
    em = probe.get("error_message", "")
    hs = probe.get("http_status", "")
    if et == "MissingCredentials":
        return ("Set the source's API credentials at /settings/?tab=api, then "
                "click ▶ Run now on /news/.")
    if et == "NoFeedsConfigured":
        return "Add at least one feed under 'RSS feeds' on /news/, save, then ▶ Run now."
    if et == "MissingApiKey":
        return ("Add an API key at /settings/?tab=api (cryptopanic free tier "
                "still needs an auth_token from cryptopanic.com).")
    if hs.startswith("4") or hs.startswith("5"):
        return f"Upstream returned HTTP {hs}: {em or 'check probe sample'}. Review source config."
    if last_error:
        return f"Source's last poll errored: {last_error}. Inspect /news/source/{name}/log."
    return ("Source polls but returns nothing. For GDELT: query may be too "
            "narrow. For Reddit: subreddit list may be empty. Inspect probe.")


async def _detect_venue_block() -> list[dict[str, Any]]:
    out = []
    redis = get_redis()
    for venue in ("bybit", "alpaca"):
        blocked = await redis.hgetall(f"venue:blocked:{venue}")
        if not blocked:
            continue
        out.append({
            "pattern": "venue_blocked",
            "severity": "warn",
            "title": f"{venue.capitalize()} returned a regulatory block",
            "evidence": {"venue": venue, "details": blocked},
            "proposed_fix": (
                f"Live trades on {venue} are silently paper-filled. To restore "
                "live: contact venue support / change IP region. To clear the "
                "block flag and retry: approve the fix below."
            ),
            "fix_action": "clear_venue_block",
            "fix_action_args": {"venue": venue},
            "fingerprint": _fp("venue_blocked", venue),
        })
    return out


async def _detect_openrouter_402() -> list[dict[str, Any]]:
    out = []
    redis = get_redis()
    flag = await redis.hgetall("api_limit:openrouter")
    if not flag:
        return out
    mode = (await redis.hget("llm_config", "mode")) or "paid"
    if mode != "auto":
        out.append({
            "pattern": "openrouter_402_paid_mode",
            "severity": "critical",
            "title": "OpenRouter returned a credit/billing error and llm_mode is not `auto`",
            "evidence": {"flag": flag, "current_mode": mode},
            "proposed_fix": (
                "Switch llm_config.mode to `auto` so the next 402 falls back "
                "to free models automatically. Approve below to set it now."
            ),
            "fix_action": "set_llm_mode_auto",
            "fix_action_args": {},
            "fingerprint": _fp("or402_paid"),
        })
    return out


async def _detect_no_proposals() -> list[dict[str, Any]]:
    out = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    async with session_scope() as session:
        from plata.core.db import Proposal
        latest = (await session.execute(
            select(Proposal).order_by(desc(Proposal.created_at)).limit(1)
        )).scalar_one_or_none()
        recent_count = (await session.execute(
            select(func.count(Proposal.id)).where(Proposal.created_at >= cutoff)
        )).scalar() or 0
    if recent_count > 0:
        return out
    age_min = 99999
    if latest is not None:
        age_min = int((now - latest.created_at).total_seconds() / 60)
    threshold = await get_redis().hget("risk_config", "min_sentiment_magnitude")
    out.append({
        "pattern": "no_recent_proposals",
        "severity": "warn",
        "title": f"No new proposals in the last {min(age_min, 120)} min",
        "evidence": {"last_proposal_age_min": age_min,
                      "min_sentiment_magnitude": threshold or "0.5"},
        "proposed_fix": (
            "Likely causes: (1) news pipeline silent — see other findings; "
            "(2) sentiment_magnitude threshold too high. Approve below to "
            "lower min_sentiment_magnitude to 0.3 (was " + str(threshold or "0.5") + ")."
        ),
        "fix_action": "lower_sentiment_threshold",
        "fix_action_args": {"new": 0.3},
        "fingerprint": _fp("no_proposals"),
    })
    return out


async def _detect_repeated_errors() -> list[dict[str, Any]]:
    out = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    async with session_scope() as session:
        rows = (await session.execute(
            select(ErrorLog.error_type, ErrorLog.agent, func.count(ErrorLog.id).label("c"))
            .where(ErrorLog.ts >= cutoff, ErrorLog.severity.in_(["ERROR", "CRITICAL"]))
            .group_by(ErrorLog.error_type, ErrorLog.agent)
            .order_by(desc("c"))
            .limit(5)
        )).all()
        # Pull the latest example of each
        examples: dict[tuple[str, str], dict] = {}
        for etype, agent, _c in rows:
            ex = (await session.execute(
                select(ErrorLog)
                .where(ErrorLog.error_type == etype, ErrorLog.agent == agent)
                .order_by(desc(ErrorLog.ts)).limit(1)
            )).scalar_one_or_none()
            if ex is not None:
                examples[(etype, agent)] = {
                    "message": ex.message[:600],
                    "ts": ex.ts.isoformat(),
                    "traceback_tail": (ex.traceback or "").splitlines()[-3:] if ex.traceback else [],
                }
    for etype, agent, c in rows:
        if c < 3:
            continue
        ex = examples.get((etype, agent)) or {}
        out.append({
            "pattern": "repeated_error",
            "severity": "warn",
            "title": f"`{etype}` from `{agent}` recurred {c}× in the last hour",
            "evidence": {"error_type": etype, "agent": agent,
                          "count_1h": c, "latest_example": ex},
            "proposed_fix": (
                "Recurring error suggests a real bug or a misconfig. Copy the "
                "evidence below into chat for a diagnosis, or check /errors/."
            ),
            "fix_action": None,
            "fix_action_args": {},
            "fingerprint": _fp("repeated_err", etype, agent),
        })
    return out


# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------

def _fp(*parts: str) -> str:
    return hashlib.sha1(("|".join(parts)).encode("utf-8")).hexdigest()[:24]


async def _detect_supervisor_crashloop() -> list[dict[str, Any]]:
    """Surface agents the in-container supervisor has been restarting."""
    out = []
    redis = get_redis()
    async for k in redis.scan_iter(match="agent_supervisor:*", count=100):
        data = await redis.hgetall(k)
        try:
            count = int(data.get("restart_count") or 0)
        except (ValueError, TypeError):
            count = 0
        if count <= 0:
            continue
        agent = k.split(":", 1)[-1]
        out.append({
            "pattern": "supervisor_crashloop",
            "severity": "warn" if count < 5 else "critical",
            "title": f"`{agent}` has been auto-restarted {count}× by the supervisor",
            "evidence": {
                "agent": agent,
                "restart_count": count,
                "last_crash_at": data.get("last_crash_at"),
                "last_crash_error": data.get("last_crash_error"),
            },
            "proposed_fix": (
                "The container's in-process supervisor caught a crash and "
                "auto-restarted the agent (v2.24.155+). It's running again, but "
                "the underlying bug should be fixed so the loop stops. Copy the "
                "`last_crash_error` from evidence into chat for a diagnosis."
            ),
            "fix_action": None,
            "fix_action_args": {},
            "fingerprint": _fp("crashloop", agent),
        })
    return out


async def _detect_signal_to_proposal_gap() -> list[dict[str, Any]]:
    """Detect: scrapers have published signals in the last hour, BUT no new
    proposals came out. Means the chain is broken between graph_ingestion /
    strategist and Proposal.

    AUTO-FIX: halve the sentiment threshold (floor 0.1) AND re-claim
    strategist's stuck pending entries. Both are in AUTO_APPLY_SAFE so they
    run without confirmation.
    """
    out = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    # Count signals published in the last hour across all sources
    redis = get_redis()
    pub_in_hour = 0
    async for sk in redis.scan_iter(match="scraper:source:*", count=100):
        if sk.endswith(":log") or sk.endswith(":probe"):
            continue
        h = await redis.hgetall(sk)
        # We track lifetime_published; use last_published as recent proxy
        try:
            pub_in_hour += int(h.get("last_published") or 0)
        except (TypeError, ValueError):
            pass
    if pub_in_hour <= 0:
        return out  # let _detect_news_silent handle it
    # Count proposals in the same hour
    from plata.core.db import Proposal
    async with session_scope() as session:
        prop_in_hour = (await session.execute(
            select(func.count(Proposal.id)).where(Proposal.created_at >= cutoff)
        )).scalar() or 0
    if prop_in_hour > 0:
        return out  # healthy
    # Gap: signals flowing, no proposals. Read current threshold for evidence.
    thresh_raw = await redis.hget("risk_config", "min_sentiment_magnitude")
    try:
        cur_thresh = float(thresh_raw) if thresh_raw else 0.5
    except (TypeError, ValueError):
        cur_thresh = 0.5
    out.append({
        "pattern": "signal_to_proposal_gap",
        "severity": "warn",
        "title": (
            f"Signals flowing ({pub_in_hour} last poll) but 0 proposals in 1h — "
            "auto-lowering sentiment threshold"
        ),
        "evidence": {
            "signals_last_poll": pub_in_hour,
            "proposals_last_hour": prop_in_hour,
            "current_sentiment_threshold": cur_thresh,
            "note": "Most likely: strategist is rejecting everything as below_threshold. Auto-fix halves the threshold (floor 0.1). If still 0 proposals next hour, the strategist itself may be stuck.",
        },
        "proposed_fix": (
            f"Auto-applying: halve min_sentiment_magnitude (currently "
            f"{cur_thresh}) to surface borderline events. Floor protected at "
            "0.1 so it won't drift away."
        ),
        "fix_action": "lower_sentiment_threshold",
        "fix_action_args": {},  # use halving behaviour
        "fingerprint": _fp("sig_to_prop_gap"),
    })
    return out


DETECTORS = [
    _detect_stale_agents,
    _detect_news_silent,
    _detect_venue_block,
    _detect_openrouter_402,
    _detect_no_proposals,
    _detect_repeated_errors,
    _detect_supervisor_crashloop,
    _detect_signal_to_proposal_gap,
]


async def _upsert(finding: dict) -> None:
    """Upsert by fingerprint: refresh title/evidence/severity if `new`.
    If the fix_action is in AUTO_APPLY_SAFE, run it immediately and stash
    the result on the row so the user can see what happened — no approval
    click needed for safe, reversible, no-cost actions.
    """
    new_finding_id: int | None = None
    async with session_scope() as session:
        existing = (await session.execute(
            select(SysopFinding)
            .where(SysopFinding.fingerprint == finding["fingerprint"])
            .order_by(desc(SysopFinding.created_at)).limit(1)
        )).scalar_one_or_none()
        if existing is not None and existing.state in ("new",):
            existing.title = finding["title"][:255]
            existing.severity = finding["severity"]
            existing.evidence = finding["evidence"]
            existing.proposed_fix = finding["proposed_fix"]
            existing.fix_action = finding["fix_action"]
            existing.fix_action_args = finding.get("fix_action_args", {})
            return
        if existing is not None:
            recent = (datetime.now(timezone.utc) - existing.updated_at).total_seconds() < 3600
            if recent:
                return
        row = SysopFinding(
            pattern=finding["pattern"],
            severity=finding["severity"],
            title=finding["title"][:255],
            evidence=finding["evidence"],
            proposed_fix=finding["proposed_fix"],
            fix_action=finding["fix_action"],
            fix_action_args=finding.get("fix_action_args", {}),
            state="new",
            fingerprint=finding["fingerprint"],
        )
        session.add(row)
        await session.flush()
        new_finding_id = row.id
        _log.info("sysop_finding_created", pattern=finding["pattern"],
                  severity=finding["severity"], title=finding["title"][:100])
    # Auto-apply OUTSIDE the upsert transaction so the fix's own DB writes
    # don't conflict with the row insert.
    if new_finding_id and finding.get("fix_action") in AUTO_APPLY_SAFE:
        try:
            result = await apply_fix(new_finding_id, "sysop:auto")
            _log.info("sysop_auto_applied",
                      finding_id=new_finding_id,
                      action=finding.get("fix_action"),
                      result=result[:200])
        except Exception as exc:  # noqa: BLE001
            _log.warning("sysop_auto_apply_failed",
                          finding_id=new_finding_id, error=str(exc)[:160])


async def apply_fix(finding_id: int, actor: str) -> str:
    """Run the approved fix for a finding. Returns a result string."""
    async with session_scope() as session:
        f = (await session.execute(
            select(SysopFinding).where(SysopFinding.id == finding_id)
        )).scalar_one_or_none()
        if f is None:
            raise ValueError("finding not found")
        if f.state != "new":
            raise ValueError(f"already in state {f.state}")
        if not f.fix_action or f.fix_action not in FIX_REGISTRY:
            raise ValueError("no automatic fix registered for this finding")
        fix = FIX_REGISTRY[f.fix_action]
        result = await fix(f.fix_action_args or {})
        f.state = "auto_applied"
        f.actor = actor
        # Stash result back into evidence so the user can audit
        f.evidence = {**(f.evidence or {}), "fix_result": result,
                       "applied_at": datetime.now(timezone.utc).isoformat()}
    return result


async def mark_state(finding_id: int, new_state: str, actor: str) -> None:
    async with session_scope() as session:
        f = (await session.execute(
            select(SysopFinding).where(SysopFinding.id == finding_id)
        )).scalar_one_or_none()
        if f is None:
            return
        f.state = new_state
        f.actor = actor


async def run() -> None:
    await asyncio.sleep(STARTUP_DELAY_SEC)
    while True:
        try:
            for det in DETECTORS:
                try:
                    findings = await det()
                except Exception as exc:  # noqa: BLE001
                    _log.warning("sysop_detector_failed",
                                  detector=det.__name__, error=str(exc)[:160])
                    continue
                for f in findings:
                    try:
                        await _upsert(f)
                    except Exception as exc:  # noqa: BLE001
                        _log.warning("sysop_upsert_failed", error=str(exc)[:160])
        except Exception as exc:  # noqa: BLE001
            _log.warning("sysop_loop_failed", error=str(exc)[:160])
        await asyncio.sleep(INTERVAL_SEC)


def format_for_clipboard(f: SysopFinding) -> str:
    """Render a finding as a markdown block ready to paste into chat."""
    lines = [
        f"# Plata sysop finding · {f.severity.upper()}",
        f"**Pattern:** `{f.pattern}`",
        f"**Title:** {f.title}",
        f"**Created:** {f.created_at.isoformat()}",
        f"**Fingerprint:** `{f.fingerprint}`",
        "",
        "## Proposed fix",
        f.proposed_fix,
        "",
        "## Evidence",
        "```json",
        json.dumps(f.evidence or {}, indent=2, default=str),
        "```",
    ]
    if f.fix_action:
        lines += ["",
                   f"## Automatic fix registered",
                   f"Action: `{f.fix_action}`  ·  Args: `{json.dumps(f.fix_action_args or {})}`",
                   "(Approve on /sysop/ to apply.)"]
    return "\n".join(lines)
