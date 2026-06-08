"""FastAPI dashboard — Flowbite Admin UI."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from plata.config.settings import get_settings
from plata.dashboard import BASE_DIR, templates
from plata.dashboard.auth import (
    auth_middleware,
    current_user_email,
    ensure_admin_bootstrapped,
)
from plata.dashboard.routes import (
    activity,
    agents,
    auth,
    dlq,
    errors,
    graph,
    historian,
    history,
    news as news_route,
    positions,
    proposals,
    push as push_route,
    sse as sse_route,
    translate as translate_route,
    risk_config,
    settings as settings_route,
    sysop as sysop_route,
    trades,
    tuning as tuning_route,
    workflow,
)


# Default for templates that don't pass `current_user` explicitly (kept harmless).
templates.env.globals.setdefault("current_user", None)


def _current_user_or_none(request: Request) -> str | None:
    try:
        return current_user_email(request)
    except Exception:  # noqa: BLE001
        return None


def _parse_changelog(text: str) -> list[dict]:
    """Split CHANGELOG.md by `## <version>` headings into entries."""
    entries: list[dict] = []
    current: dict | None = None
    for line in text.splitlines():
        if line.startswith("## "):
            if current is not None:
                entries.append(current)
            header = line[3:].strip()
            current = {"header": header, "body": ""}
        elif current is not None:
            current["body"] += line + "\n"
    if current is not None:
        entries.append(current)
    return entries


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    try:
        await ensure_admin_bootstrapped()
    except Exception as exc:  # noqa: BLE001
        import logging
        logging.getLogger("dashboard").warning("admin_bootstrap_skipped: %s", exc)
    # Auto-resume any historian seed that was killed by the previous restart.
    try:
        from plata.dashboard.routes.historian import _resume_if_interrupted
        await _resume_if_interrupted()
    except Exception as exc:  # noqa: BLE001
        import logging
        logging.getLogger("dashboard").warning("historian_resume_skipped: %s", exc)
    # Make sure all auto-created tables exist (api_credentials, proposals,
    # agent_activity_log). Idempotent — safe across redeploys.
    try:
        from plata.core.db import ensure_aux_tables
        await ensure_aux_tables()
    except Exception as exc:  # noqa: BLE001
        _log.warning("aux_tables_create_failed", error=str(exc)[:160])
    # One-shot backfill: copy any live Redis cost:daily:*:agent:* keys into the
    # new llm_cost table so that the migration moment doesn't lose recent data.
    # Each (date, agent) becomes ONE row with synthetic noon-UTC timestamp.
    # Idempotent: marks each key as backfilled with a sentinel `:backfilled` key.
    try:
        from datetime import datetime as _dt, time as _t, timezone as _tz
        from decimal import Decimal as _Dec
        from plata.core.bus import get_redis as _gr
        from plata.core.db import LLMCost as _LLMCost, session_scope as _ss
        from sqlalchemy import select as _sel, func as _f
        r = _gr()
        moved = 0
        async with _ss() as session:
            async for ck in r.scan_iter(match="cost:daily:*:agent:*", count=500):
                if await r.exists(f"{ck}:backfilled"):
                    continue
                parts = ck.split(":")
                if len(parts) != 5:
                    continue
                date_iso = parts[2]; agent = parts[4]
                try:
                    d = _dt.fromisoformat(date_iso).date()
                except Exception:  # noqa: BLE001
                    continue
                # Skip if a row already exists for this (agent, date) — protects
                # against re-running on a dashboard restart.
                date_col = _f.date(_LLMCost.ts)
                exists = (await session.execute(
                    _sel(_LLMCost.id).where(_LLMCost.agent == agent, date_col == d).limit(1)
                )).scalar_one_or_none()
                if exists:
                    await r.set(f"{ck}:backfilled", "1", ex=60 * 60 * 24 * 365)
                    continue
                raw = await r.get(ck)
                try:
                    val = float(raw or 0)
                except (TypeError, ValueError):
                    continue
                if val <= 0:
                    continue
                ts = _dt.combine(d, _t(12, 0, 0), tzinfo=_tz.utc)
                session.add(_LLMCost(
                    ts=ts, agent=agent, model=None,
                    prompt_tokens=None, completion_tokens=None,
                    cost_usd=_Dec(str(val)),
                ))
                await r.set(f"{ck}:backfilled", "1", ex=60 * 60 * 24 * 365)
                moved += 1
        if moved:
            _log.info("llm_cost_backfill_complete", moved=moved)
    except Exception as exc:  # noqa: BLE001
        _log.warning("llm_cost_backfill_failed", error=str(exc)[:160])
    try:
        from plata.config import credentials as _creds
        await _creds.ensure_table()
        for p in ("openrouter", "voyage", "bybit_key", "bybit_secret",
                   "alpaca_key", "alpaca_secret", "telegram",
                   "langfuse_public", "langfuse_secret"):
            await _creds.get(p)
    except Exception as exc:  # noqa: BLE001
        import logging
        logging.getLogger("dashboard").warning("credentials_warmup_skipped: %s", exc)
    # Periodic sweeper: delete agent_activity_log rows older than 30 days.
    import asyncio as _asyncio
    async def _activity_sweeper() -> None:
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        from sqlalchemy import delete as _delete
        from plata.core.db import AgentActivityLog, session_scope
        while True:
            try:
                cutoff = _dt.now(_tz.utc) - _td(days=30)
                async with session_scope() as session:
                    await session.execute(
                        _delete(AgentActivityLog).where(AgentActivityLog.ts < cutoff)
                    )
            except Exception as exc:  # noqa: BLE001
                _log.warning("activity_sweep_failed", error=str(exc)[:160])
            await _asyncio.sleep(6 * 60 * 60)  # every 6h
    _sweeper_task = _asyncio.create_task(_activity_sweeper(), name="activity-sweeper")
    _app.state._activity_sweeper = _sweeper_task

    # Health watchdog: detect "silently stuck" major functions and surface
    # them as WARN entries in error_log so they show up on /errors/.
    # Goes after agent-style failures (no exception raised, just a function
    # that should be running and isn't):
    #   • System is RUNNING but every scraper source is halted → "no signals
    #     can enter the pipeline".
    #   • A critical agent's heartbeat is stale > 3 min while system RUNNING.
    # Each condition fires at most once per cooldown window (10 min) to keep
    # the errors page readable.
    async def _health_watchdog() -> None:
        from datetime import datetime as _dt, timezone as _tz
        from plata.core.bus import get_redis
        from plata.core.error_reporter import get_error_reporter
        redis = get_redis()
        reporter = get_error_reporter()
        cooldown_key = "health_watchdog:last_warned"
        cooldown_sec = 10 * 60
        CRITICAL_AGENTS = ("enricher", "strategist", "executor", "risk_manager", "position_monitor")
        while True:
            try:
                state = await redis.get("system:state") or "RUNNING"
                now_utc = _dt.now(_tz.utc)
                async def _maybe_warn(code: str, error_type: str, msg: str, ctx: dict) -> None:
                    last = await redis.hget(cooldown_key, code)
                    if last:
                        try:
                            if (now_utc - _dt.fromisoformat(last)).total_seconds() < cooldown_sec:
                                return
                        except Exception:  # noqa: BLE001
                            pass
                    await reporter.capture(
                        agent="health_watchdog", severity="WARN",
                        error_type=error_type, message=msg, context=ctx,
                    )
                    await redis.hset(cooldown_key, code, now_utc.isoformat())

                # 1. Scrapers all halted while system says RUNNING.
                #    For halts marked `halted_by=system` (left over from a past
                #    system halt that never cleaned up): AUTO-RESUME them.
                #    For user-halted sources: WARN — those need a human.
                if state == "RUNNING":
                    seen_any = False
                    active_count = 0
                    auto_resumed: list[str] = []
                    still_halted_by_user: list[str] = []
                    async for k in redis.scan_iter(match="scraper:source:*", count=100):
                        if k.endswith(":log"):
                            continue
                        seen_any = True
                        data = await redis.hgetall(k)
                        name = k.rsplit(":", 1)[-1]
                        status = (data.get("status") or "").lower()
                        if status == "halted":
                            if (data.get("halted_by") or "system") == "system":
                                # Self-healing: the resume action must have missed
                                # this one (e.g. service was down then). Fix it now.
                                await redis.hset(k, mapping={"status": "idle", "halted_by": ""})
                                auto_resumed.append(name)
                                active_count += 1
                            else:
                                still_halted_by_user.append(name)
                        else:
                            active_count += 1
                    if auto_resumed:
                        await reporter.capture(
                            agent="health_watchdog", severity="WARN",
                            error_type="ScrapersAutoResumed",
                            message=(
                                f"Auto-resumed {len(auto_resumed)} scraper source(s) "
                                f"that were halted_by=system while system is RUNNING: "
                                f"{', '.join(auto_resumed)}. Root cause is a past "
                                f"system halt whose resume didn't clean up these flags."
                            ),
                            context={"sources": ",".join(auto_resumed[:10])},
                        )
                    if seen_any and active_count == 0:
                        await _maybe_warn(
                            "all_scrapers_halted",
                            "AllScrapersHalted",
                            f"System is RUNNING but every scraper source is halted "
                            f"by the user ({len(still_halted_by_user)} sources). "
                            f"No new signals will reach the pipeline until at least "
                            f"one source is resumed. Resume from /workflow/.",
                            {"halted_sources": ",".join(still_halted_by_user[:10])},
                        )

                # 2. Critical agents with stale heartbeats while RUNNING.
                if state == "RUNNING":
                    for agent_name in CRITICAL_AGENTS:
                        hb = await redis.hgetall(f"agent_status:{agent_name}")
                        if not hb:
                            await _maybe_warn(
                                f"no_heartbeat:{agent_name}",
                                "AgentMissing",
                                f"No heartbeat ever recorded for `{agent_name}` — "
                                f"the service may not have booted at all.",
                                {"agent": agent_name},
                            )
                            continue
                        last_hb = hb.get("last_heartbeat")
                        if not last_hb:
                            continue
                        try:
                            age = (now_utc - _dt.fromisoformat(last_hb)).total_seconds()
                        except Exception:  # noqa: BLE001
                            continue
                        if age > 180:
                            halted_part = ""
                            if (hb.get("halted") or "").lower() == "true":
                                halted_part = " (process was also flagged halted before going stale — likely died after a halt)"
                            await _maybe_warn(
                                f"stale_heartbeat:{agent_name}",
                                "AgentStaleHeartbeat",
                                f"`{agent_name}` heartbeat is {int(age)}s old "
                                f"(threshold 180s){halted_part}. "
                                f"The container is probably dead — restart it on Railway. "
                                f"Resume from the dashboard cannot bring back a dead process.",
                                {"agent": agent_name, "age_sec": str(int(age)),
                                 "was_halted": (hb.get("halted") or "false")},
                            )

                # 3. Venue regulatory blocks (Bybit / Alpaca returned 10024 etc.).
                #    Executor falls back to paper for those trades but the user
                #    should know live trading on that venue is unavailable.
                for venue in ("bybit", "alpaca"):
                    blocked = await redis.hgetall(f"venue:blocked:{venue}")
                    if blocked:
                        await _maybe_warn(
                            f"venue_blocked:{venue}",
                            "VenueRegulatoryBlock",
                            f"{venue.capitalize()} returned a regulatory / KYC "
                            f"block ({blocked.get('code', '?')}). New live "
                            f"trades on this venue are silently falling back "
                            f"to paper fills. To restore live: contact the "
                            f"venue's support or switch IP region.",
                            {"venue": venue, "msg": (blocked.get("message") or "")[:200]},
                        )

                # 4. PIPELINE SILENCE alerts — the user-facing pain.
                # Each check runs every minute; cooldown_sec dedup keeps the
                # alert from spamming. Pushes to /errors/ AND the bell badge
                # (severity=WARN so it shows up in red).
                if state == "RUNNING":
                    # 4a. No signal published by ANY source in 30 min while RUNNING.
                    last_pub_ts = None
                    async for sk in redis.scan_iter(match="scraper:source:*", count=100):
                        if sk.endswith(":log") or sk.endswith(":probe"):
                            continue
                        h = await redis.hgetall(sk)
                        lp = h.get("last_poll_at")
                        if not lp:
                            continue
                        try:
                            t = _dt.fromisoformat(lp)
                            if (last_pub_ts is None) or t > last_pub_ts:
                                last_pub_ts = t
                        except Exception:  # noqa: BLE001
                            pass
                    # No poll AT ALL recently → upstream pipeline is dark.
                    if last_pub_ts is None or (now_utc - last_pub_ts).total_seconds() > 30 * 60:
                        await _maybe_warn(
                            "news_pipeline_silent",
                            "NewsPipelineSilent",
                            "No scraper source has polled in the last 30 min. "
                            "Open `/news/` and check the Diagnosis column for each "
                            "row — the probe will show the HTTP status / error. "
                            "Most common causes: (a) ingestion_hub container is "
                            "dead, (b) all sources halted, (c) source creds missing.",
                            {"last_poll_ts": str(last_pub_ts) if last_pub_ts else "never"},
                        )
                    # 4b. No new proposal in last 2h while RUNNING and pipeline is alive.
                    try:
                        from plata.core.db import Proposal as _Prop, session_scope as _ss
                        from sqlalchemy import desc as _desc, select as _select
                        async with _ss() as session:
                            latest = (await session.execute(
                                _select(_Prop)
                                .order_by(_desc(_Prop.created_at))
                                .limit(1)
                            )).scalar_one_or_none()
                        if latest is None:
                            age_min = 99999
                        else:
                            age_min = (now_utc - latest.created_at).total_seconds() / 60.0
                        if age_min > 120:
                            await _maybe_warn(
                                "no_proposal_in_2h",
                                "NoProposalsEmitted",
                                f"No new trade proposal in the last {int(age_min)} min. "
                                "Strategist is running but either: (a) every event is "
                                "below sentiment_magnitude threshold, (b) every event "
                                "is dedup'd or hits the cooldown guard, (c) news pipeline "
                                "is silent (see other alerts). Check `/proposals/` filter "
                                "by 'Why not traded' to see the breakdown.",
                                {"last_proposal_age_min": str(int(age_min))},
                            )
                    except Exception as exc:  # noqa: BLE001
                        _log.debug("no_proposal_check_failed", error=str(exc)[:120])
            except Exception as exc:  # noqa: BLE001
                _log.warning("health_watchdog_failed", error=str(exc)[:160])
            await _asyncio.sleep(60)
    _watchdog_task = _asyncio.create_task(_health_watchdog(), name="health-watchdog")
    _app.state._health_watchdog = _watchdog_task

    # Improver: nightly self-survey that writes a digest row to /errors/.
    try:
        from plata.agents import improver as _improver
        _improver_task = _asyncio.create_task(_improver.run(), name="improver")
        _app.state._improver = _improver_task
    except Exception as exc:  # noqa: BLE001
        _log.warning("improver_start_failed", error=str(exc)[:160])

    # Sysop: continuous monitor → SysopFinding rows on /sysop/.
    try:
        from plata.agents import sysop as _sysop
        _sysop_task = _asyncio.create_task(_sysop.run(), name="sysop")
        _app.state._sysop = _sysop_task
    except Exception as exc:  # noqa: BLE001
        _log.warning("sysop_start_failed", error=str(exc)[:160])

    # OpenRouter free-model catalog refresh: once at boot then every 24h.
    # Keeps `llm:free_catalog` Redis set current so when OpenRouter retires
    # or adds free models, Plata picks the change up the next day.
    async def _free_catalog_refresher() -> None:
        from plata.core.bus import get_redis as _gr
        from plata.core.llm import refresh_free_catalog
        await _asyncio.sleep(5)  # let the dashboard finish booting
        # ONE-SHOT v2.24.164 cleanup: the v2.24.158 classification wrongly
        # marked "no endpoints found" as PERMANENT 24h-dead. As a result
        # the curated FREE_FALLBACKS (llama / deepseek / gemini / qwen /
        # hermes) all ended up cached as dead, so chain walks always
        # skipped them and used obscure live-catalog models that 429'd.
        # Clear the set so the new (correct) classification can re-evaluate.
        try:
            await _gr().delete("llm:dead_free_models")
            _log.info("llm_dead_free_set_cleared_for_reclassification")
        except Exception:  # noqa: BLE001
            pass
        # ONE-SHOT v2.24.168: the proposals:last_persist_error banner had a
        # 7-day TTL. v2.24.167 dropped it to 30 min, but old entries linger.
        # Wipe once on boot so the user sees the banner disappear immediately
        # after the fix deploys.
        try:
            await _gr().delete("proposals:last_persist_error")
        except Exception:  # noqa: BLE001
            pass
        while True:
            try:
                await refresh_free_catalog()
            except Exception as exc:  # noqa: BLE001
                _log.warning("free_catalog_loop_error", error=str(exc)[:160])
            await _asyncio.sleep(24 * 60 * 60)
    try:
        _cat_task = _asyncio.create_task(_free_catalog_refresher(), name="free-catalog-refresh")
        _app.state._free_catalog_refresher = _cat_task
    except Exception as exc:  # noqa: BLE001
        _log.warning("free_catalog_task_start_failed", error=str(exc)[:160])

    # Server-side push relay: subscribe to dashboard:events and, for
    # actionable kinds, deliver a web push to every saved subscription
    # (iOS PWA / Chrome). Browsers wake even when the tab is closed.
    async def _push_relay() -> None:
        from plata.core.bus import subscribe as _subscribe
        from plata.dashboard.push import send_to_user
        ACTIONABLE = {"proposal_pending", "adjustment_suggested", "system_state"}
        try:
            async for _channel, payload in _subscribe("dashboard:events"):
                try:
                    if not isinstance(payload, dict):
                        continue
                    kind = payload.get("kind")
                    if kind not in ACTIONABLE:
                        continue
                    if kind == "system_state" and payload.get("state") != "HALTED":
                        continue
                    # Compose title/body/url per kind.
                    if kind == "proposal_pending":
                        title = "Plata · New proposal"
                        body = f"{payload.get('symbol','?')} {(payload.get('side') or '').upper()} — awaiting your approval"
                        url = "/proposals/?state=pending_hitl"
                    elif kind == "adjustment_suggested":
                        title = "Plata · Position adjustment"
                        body = f"Monitor suggests {payload.get('action','adjustment')} on {payload.get('symbol','?')}"
                        url = f"/proposals/?symbol={payload.get('symbol','')}#detail-{payload.get('ulid','')}"
                    else:  # system_state == HALTED
                        title = "Plata · System HALTED"
                        body = "Trading is paused — tap to manage"
                        url = "/agents/"
                    # Fan out to every user with a saved subscription.
                    async for sub_key in get_redis().scan_iter(match="push:sub:*", count=50):
                        user_email = sub_key.split(":", 2)[-1]
                        try:
                            await send_to_user(user_email, title=title, body=body, url=url, tag=kind)
                        except Exception as exc:  # noqa: BLE001
                            _log.warning("push_relay_send_failed", user=user_email, error=str(exc)[:160])
                except Exception as exc:  # noqa: BLE001
                    _log.warning("push_relay_dispatch_failed", error=str(exc)[:160])
        except _asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _log.warning("push_relay_loop_died", error=str(exc)[:160])
    _push_task = _asyncio.create_task(_push_relay(), name="push-relay")
    _app.state._push_relay = _push_task
    yield
    _sweeper_task.cancel()
    _watchdog_task.cancel()
    _push_task.cancel()


def create_app() -> FastAPI:
    app = FastAPI(title="Plata Dashboard", docs_url=None, redoc_url=None, lifespan=_lifespan)

    # Global handler: any uncaught exception in any dashboard route is logged
    # to /errors/ (Postgres `error_log`) instead of disappearing into stdout.
    @app.exception_handler(Exception)
    async def _global_exception_logger(req: Request, exc: Exception):
        from starlette.responses import JSONResponse
        from starlette.exceptions import HTTPException as StarletteHTTPException
        # Let FastAPI's normal flow handle HTTPException / redirects.
        if isinstance(exc, StarletteHTTPException):
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        try:
            from plata.core.error_reporter import get_error_reporter
            await get_error_reporter().capture_exception(
                exc, agent="dashboard", severity="ERROR",
                context={
                    "path": req.url.path,
                    "method": req.method,
                    "user": _current_user_or_none(req) or "anonymous",
                },
            )
        except Exception:  # noqa: BLE001
            import logging
            logging.getLogger("dashboard").exception("error_reporter_unavailable")
        return JSONResponse({"detail": "internal server error"}, status_code=500)
    static_dir = BASE_DIR / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.add_middleware(BaseHTTPMiddleware, dispatch=auth_middleware)

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/api/version")
    async def api_version():
        return {"version": get_settings().app_version}

    @app.get("/api/dashboard/sparklines")
    async def api_sparklines():
        """Return small time-bucketed series for the dashboard tile sparklines.

        - signals_24h: hourly count for the last 24 h.
        - pnl_30d: daily net PnL for the last 30 d.
        """
        from datetime import date, datetime, timedelta, timezone
        from sqlalchemy import cast, func, select, Date
        from plata.core.db import SignalArchive, TradeLedger, session_scope
        try:
            async with session_scope() as session:
                since = datetime.now(timezone.utc) - timedelta(hours=24)
                rows = (await session.execute(
                    select(
                        func.date_trunc('hour', SignalArchive.fetched_at).label('h'),
                        func.count().label('n'),
                    )
                    .where(SignalArchive.fetched_at >= since)
                    .group_by('h').order_by('h')
                )).all()
                signals = [(r.h.isoformat() if r.h else "", int(r.n or 0)) for r in rows]

                since30 = datetime.now(timezone.utc) - timedelta(days=30)
                rows2 = (await session.execute(
                    select(
                        cast(TradeLedger.closed_at, Date).label('d'),
                        func.coalesce(func.sum(TradeLedger.net_pnl), 0).label('p'),
                    )
                    .where(TradeLedger.closed_at >= since30)
                    .group_by('d').order_by('d')
                )).all()
                pnl = [(r.d.isoformat() if r.d else "", float(r.p or 0)) for r in rows2]
            return {"signals_24h": signals, "pnl_30d": pnl}
        except Exception:  # noqa: BLE001
            return {"signals_24h": [], "pnl_30d": []}

    @app.get("/api/changelog")
    async def api_changelog():
        from pathlib import Path
        for candidate in (Path("CHANGELOG.md"), BASE_DIR.parents[1] / "CHANGELOG.md"):
            if candidate.is_file():
                return {"raw": candidate.read_text(), "entries": _parse_changelog(candidate.read_text())}
        return {"raw": "", "entries": []}

    @app.post("/api/kill")
    async def api_kill():
        from plata.core.bus import Channels, get_redis, publish_channel
        await publish_channel(Channels.SYSTEM_HALT, {"reason": "manual_killswitch"})
        await get_redis().set("system:state", "HALTED")
        await publish_channel("dashboard:events", {"kind": "system_state", "state": "HALTED"})
        return {"ok": True, "state": "HALTED"}

    @app.post("/api/resume")
    async def api_resume():
        from plata.core.bus import Channels, get_redis, publish_channel
        await publish_channel(Channels.SYSTEM_RESUME, {"reason": "manual_resume"})
        redis = get_redis()
        await redis.set("system:state", "RUNNING")
        # Sticky-halt cleanup: scrapers set status=halted on every source while
        # the system is halted (runner.py:47); the per-source flags don't get
        # cleared on resume by themselves. Clear any source whose `halted_by`
        # is "system" so the scraper loop picks it back up on its next tick.
        # User-cancelled sources keep their flag (they have halted_by=user).
        cleared: list[str] = []
        try:
            async for k in redis.scan_iter(match="scraper:source:*", count=100):
                # Skip the per-source log lists (lpush keys end in :log).
                if k.endswith(":log"):
                    continue
                data = await redis.hgetall(k)
                if (data.get("status") or "").lower() != "halted":
                    continue
                if (data.get("halted_by") or "system") == "system":
                    await redis.hset(k, mapping={"status": "idle", "halted_by": ""})
                    cleared.append(k.rsplit(":", 1)[-1])
        except Exception:  # noqa: BLE001
            pass
        # Clear the per-agent halted flag so the /agents/ card pill no longer
        # shows HALTED for a process that may already be dead — STALE (the
        # heartbeat-age check) is the more honest signal. The pub/sub message
        # above wakes any live agent; dead ones get marked correctly here.
        agents_cleared: list[str] = []
        try:
            async for k in redis.scan_iter(match="agent_status:*", count=100):
                data = await redis.hgetall(k)
                if (data.get("halted") or "").lower() == "true":
                    await redis.hset(k, "halted", "False")
                    agents_cleared.append(k.split(":", 1)[-1])
        except Exception:  # noqa: BLE001
            pass
        await publish_channel("dashboard:events", {"kind": "system_state", "state": "RUNNING"})
        return {"ok": True, "state": "RUNNING",
                "sources_cleared": cleared, "agents_cleared": agents_cleared}

    @app.post("/api/agents/{name}/resume")
    async def api_agent_resume(name: str):
        from plata.core.bus import Channels, publish_channel
        await publish_channel(Channels.SYSTEM_RESUME, {"agent": name, "reason": "manual_resume"})
        return {"ok": True, "agent": name}

    @app.post("/api/agents/{name}/halt")
    async def api_agent_halt(name: str):
        from plata.core.bus import Channels, publish_channel
        await publish_channel(Channels.SYSTEM_HALT, {"agent": name, "reason": "manual_killswitch"})
        return {"ok": True, "agent": name}

    @app.get("/api/agents/halted")
    async def api_agents_halted():
        """Returns the list of agents currently halted (per their own status hashes)
        plus the global system:state for the topbar banner.

        Stale agents (heartbeat older than 2 min) are excluded — they're dead
        processes, not halted, and the banner is for actionable halts only.
        """
        from datetime import datetime as _dt, timezone as _tz
        from plata.core.bus import get_redis
        redis = get_redis()
        halted: list[str] = []
        now_utc = _dt.now(_tz.utc)
        async for k in redis.scan_iter(match="agent_status:*", count=100):
            data = await redis.hgetall(k)
            if (data.get("halted") or "").lower() != "true":
                continue
            # Exclude stale processes — they're dead, not pausable.
            hb = data.get("last_heartbeat")
            if hb:
                try:
                    if (now_utc - _dt.fromisoformat(hb)).total_seconds() > 120:
                        continue
                except Exception:  # noqa: BLE001
                    continue
            halted.append(k.split(":")[-1])
        system_state = await redis.get("system:state") or "RUNNING"
        return {"count": len(halted), "names": sorted(halted), "system_state": system_state}

    @app.get("/api/header_stats")
    async def api_header_stats():
        """Top-bar KPIs: today's realized PnL, open positions + unrealized PnL,
        pending HITL count, today's LLM spend. Polled every ~10s by the topbar."""
        from datetime import date, datetime, timedelta, timezone
        from plata.core.bus import get_redis
        from plata.core.db import TradeLedger, session_scope
        from sqlalchemy import func, select
        redis = get_redis()
        stats: dict = {
            "daily_pnl": 0.0, "open_count": 0, "unrealized_pnl": 0.0,
            "pending_hitl": 0, "llm_spend_today": 0.0, "llm_budget": 0.0,
            "paper_mode": True,
            # Reveal-on-eye-click fields:
            "open_notional_usd": 0.0,
            "realized_7d": 0.0, "realized_30d": 0.0, "realized_all": 0.0,
            "pct_today": 0.0, "pct_7d": 0.0, "pct_30d": 0.0, "pct_all": 0.0,
            "baseline_equity_usd": 10000.0,
        }
        try:
            v = await redis.hget("risk_config", "paper_trading_mode")
            stats["paper_mode"] = (v or "true").lower() in ("true", "1", "yes", "on")
        except Exception:  # noqa: BLE001
            pass
        try:
            today_utc = datetime.combine(date.today(), datetime.min.time(), tzinfo=timezone.utc)
            async with session_scope() as session:
                stats["daily_pnl"] = float((await session.execute(
                    select(func.coalesce(func.sum(TradeLedger.net_pnl), 0))
                    .where(TradeLedger.exit_price.is_not(None))
                    .where(TradeLedger.opened_at >= today_utc)
                )).scalar() or 0)
                open_rows = (await session.execute(
                    select(TradeLedger).where(TradeLedger.exit_price.is_(None))
                )).scalars().all()
                stats["open_count"] = len(open_rows)
                # Unrealized PnL: sum (latest_price - entry) * qty * side_sign
                u = 0.0
                for r in open_rows:
                    # Prefer the per-symbol watch cache (refreshed every 5min
                    # for every open-position symbol); fall back to the
                    # per-trade sample if the symbol watcher hasn't run yet.
                    sym_latest = await redis.hgetall(f"symbol:latest:{r.symbol}")
                    price = float(sym_latest.get("price") or 0)
                    if price <= 0:
                        latest = await redis.hgetall(f"trade:latest:{r.trade_ulid}")
                        price = float(latest.get("price") or 0)
                    entry = float(r.entry_price or 0)
                    qty = float(r.qty or 0)
                    if price <= 0 or entry <= 0 or qty <= 0:
                        continue
                    sign = 1.0 if (r.side or "").lower() == "long" else -1.0
                    u += sign * (price - entry) * qty
                stats["unrealized_pnl"] = round(u, 2)
                # Open notional (qty × entry, USD) summed across open positions.
                stats["open_notional_usd"] = round(
                    sum(float(r.qty or 0) * float(r.entry_price or 0) for r in open_rows),
                    2,
                )
                # Realized PnL windows: 7d / 30d / all time.
                week_ago = datetime.now(timezone.utc) - timedelta(days=7)
                month_ago = datetime.now(timezone.utc) - timedelta(days=30)
                stats["realized_7d"] = float((await session.execute(
                    select(func.coalesce(func.sum(TradeLedger.net_pnl), 0))
                    .where(TradeLedger.exit_price.is_not(None))
                    .where(TradeLedger.closed_at >= week_ago)
                )).scalar() or 0)
                stats["realized_30d"] = float((await session.execute(
                    select(func.coalesce(func.sum(TradeLedger.net_pnl), 0))
                    .where(TradeLedger.exit_price.is_not(None))
                    .where(TradeLedger.closed_at >= month_ago)
                )).scalar() or 0)
                stats["realized_all"] = float((await session.execute(
                    select(func.coalesce(func.sum(TradeLedger.net_pnl), 0))
                    .where(TradeLedger.exit_price.is_not(None))
                )).scalar() or 0)
                # Equity baseline for %-change calc — configurable via risk_config.
                # Paper accounts default to $10k; users override on /settings/?tab=risk.
                try:
                    b = await redis.hget("risk_config", "account_baseline_equity_usd")
                    if b:
                        stats["baseline_equity_usd"] = float(b)
                except Exception:  # noqa: BLE001
                    pass
                base = stats["baseline_equity_usd"] or 1.0
                stats["pct_today"] = round(100.0 * stats["daily_pnl"] / base, 3)
                stats["pct_7d"]    = round(100.0 * (stats["realized_7d"] + stats["unrealized_pnl"]) / base, 3)
                stats["pct_30d"]   = round(100.0 * (stats["realized_30d"] + stats["unrealized_pnl"]) / base, 3)
                stats["pct_all"]   = round(100.0 * (stats["realized_all"] + stats["unrealized_pnl"]) / base, 3)
        except Exception:  # noqa: BLE001
            pass
        try:
            n = 0
            async for k in redis.scan_iter(match="proposal:pending:*", count=100):
                n += 1
            stats["pending_hitl"] = n
        except Exception:  # noqa: BLE001
            pass
        try:
            today_key = "llm:spend:" + date.today().isoformat()
            v = await redis.get(today_key)
            if v:
                stats["llm_spend_today"] = float(v)
            budget = await redis.get("llm:daily_budget_usd")
            if budget:
                stats["llm_budget"] = float(budget)
        except Exception:  # noqa: BLE001
            pass
        # Next scraper poll — the soonest deterministic moment a new signal
        # could enter the pipeline and (eventually) become a proposal.
        # Always returns SOMETHING so the topbar chip can render an informative
        # state instead of silently hiding.
        try:
            soonest: tuple[float, str] | None = None
            seen_any = False
            all_halted = True
            now_ts = datetime.now(timezone.utc).timestamp()
            async for k in redis.scan_iter(match="scraper:source:*", count=100):
                if k.endswith(":log"):
                    continue
                seen_any = True
                data = await redis.hgetall(k)
                if (data.get("status") or "").lower() == "halted":
                    continue
                all_halted = False
                interval = float(data.get("interval_sec") or 0)
                # `last_poll_at` is only set AFTER the first poll completes;
                # on a fresh deploy fall back to `started_at` so the user sees
                # a countdown right away.
                last = data.get("last_poll_at") or data.get("started_at")
                if not last or interval <= 0:
                    continue
                try:
                    last_ts = datetime.fromisoformat(last).timestamp()
                except Exception:  # noqa: BLE001
                    continue
                eta = last_ts + interval
                name = k.rsplit(":", 1)[-1]
                if soonest is None or eta < soonest[0]:
                    soonest = (eta, name)
            if soonest:
                stats["next_poll_eta_sec"] = max(0, int(soonest[0] - now_ts))
                stats["next_poll_source"] = soonest[1]
                stats["next_poll_state"] = "ok"
            elif not seen_any:
                stats["next_poll_state"] = "no_sources"
            elif all_halted:
                stats["next_poll_state"] = "all_halted"
            else:
                stats["next_poll_state"] = "warming_up"
        except Exception:  # noqa: BLE001
            stats["next_poll_state"] = "unknown"
        # Last strategist activity — gives the user a sense of whether the
        # decision loop is alive even when no proposals are landing.
        try:
            hb = await redis.hgetall("agent_status:strategist")
            last_hb = hb.get("last_heartbeat")
            if last_hb:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(last_hb)).total_seconds()
                stats["strategist_last_hb_sec"] = int(max(0, age))
        except Exception:  # noqa: BLE001
            pass
        return stats

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        from datetime import date, datetime, timezone
        from sqlalchemy import func, select
        from plata.core.bus import get_redis
        from plata.core.db import ErrorLog, SignalArchive, TradeLedger, session_scope
        from plata.hitl.approval_store import list_pending

        ctx: dict = {"active": "dashboard", "current_user": _current_user_or_none(request)}
        try:
            redis = get_redis()
            ctx["system_state"] = await redis.get("system:state") or "UNKNOWN"
            today_key = f"cost:daily:{date.today().isoformat()}"
            ctx["llm_spend_today"] = float(await redis.get(today_key) or 0)
        except Exception:
            ctx["system_state"] = "UNKNOWN"
            ctx["llm_spend_today"] = 0.0
        try:
            ctx["pending_hitl"] = len(await list_pending())
        except Exception:
            ctx["pending_hitl"] = 0
        try:
            async with session_scope() as session:
                ctx["open_positions"] = (await session.execute(
                    select(func.count()).select_from(TradeLedger).where(TradeLedger.exit_price.is_(None))
                )).scalar() or 0
                ctx["open_ulid"] = None
                if ctx["open_positions"] == 1:
                    ctx["open_ulid"] = (await session.execute(
                        select(TradeLedger.trade_ulid).where(TradeLedger.exit_price.is_(None)).limit(1)
                    )).scalar_one_or_none()
                today_utc = datetime.combine(date.today(), datetime.min.time(), tzinfo=timezone.utc)
                ctx["daily_pnl"] = float((await session.execute(
                    select(func.coalesce(func.sum(TradeLedger.net_pnl), 0))
                    .where(TradeLedger.exit_price.is_not(None))
                    .where(TradeLedger.opened_at >= today_utc)
                )).scalar() or 0)
                ctx["signals_24h"] = (await session.execute(
                    select(func.count()).select_from(SignalArchive)
                    .where(SignalArchive.fetched_at >= datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0))
                )).scalar() or 0
                recent_errors = (await session.execute(
                    select(ErrorLog).order_by(ErrorLog.ts.desc()).limit(8)
                )).scalars().all()
                recent_signals = (await session.execute(
                    select(SignalArchive).order_by(SignalArchive.fetched_at.desc()).limit(8)
                )).scalars().all()
                recent_trades = (await session.execute(
                    select(TradeLedger).order_by(TradeLedger.opened_at.desc()).limit(8)
                )).scalars().all()
        except Exception:
            ctx.update(open_positions=0, daily_pnl=0.0, signals_24h=0,
                       recent_errors=[], recent_signals=[], recent_trades=[])
        else:
            ctx["recent_errors"] = recent_errors
            ctx["recent_signals"] = recent_signals
            ctx["recent_trades"] = recent_trades
        return templates.TemplateResponse(request, "pages/dashboard.html", ctx)

    app.include_router(auth.router)
    app.include_router(proposals.router)
    app.include_router(trades.router)
    app.include_router(positions.router)
    app.include_router(risk_config.router)
    app.include_router(errors.router)
    app.include_router(agents.router)
    app.include_router(activity.router)
    app.include_router(workflow.router)
    app.include_router(history.router)
    app.include_router(graph.router)
    app.include_router(historian.router)
    app.include_router(news_route.router)
    app.include_router(dlq.router)
    app.include_router(settings_route.router)
    app.include_router(tuning_route.router)
    app.include_router(sysop_route.router)
    app.include_router(push_route.router)
    app.include_router(sse_route.router)
    app.include_router(translate_route.router)
    return app


app = create_app()
