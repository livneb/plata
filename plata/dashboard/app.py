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
    proposals,
    push as push_route,
    sse as sse_route,
    translate as translate_route,
    risk_config,
    settings as settings_route,
    trades,
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
    yield


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
        await get_redis().set("system:state", "RUNNING")
        await publish_channel("dashboard:events", {"kind": "system_state", "state": "RUNNING"})
        return {"ok": True, "state": "RUNNING"}

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
        plus the global system:state for the topbar banner."""
        from plata.core.bus import get_redis
        redis = get_redis()
        halted: list[str] = []
        async for k in redis.scan_iter(match="agent_status:*", count=100):
            data = await redis.hgetall(k)
            if (data.get("halted") or "").lower() == "true":
                halted.append(k.split(":")[-1])
        system_state = await redis.get("system:state") or "RUNNING"
        return {"count": len(halted), "names": sorted(halted), "system_state": system_state}

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
    app.include_router(risk_config.router)
    app.include_router(errors.router)
    app.include_router(agents.router)
    app.include_router(activity.router)
    app.include_router(workflow.router)
    app.include_router(history.router)
    app.include_router(graph.router)
    app.include_router(historian.router)
    app.include_router(dlq.router)
    app.include_router(settings_route.router)
    app.include_router(push_route.router)
    app.include_router(sse_route.router)
    app.include_router(translate_route.router)
    return app


app = create_app()
