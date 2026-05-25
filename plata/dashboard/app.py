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
    errors,
    proposals,
    risk_config,
    trades,
)


# Default for templates that don't pass `current_user` explicitly (kept harmless).
templates.env.globals.setdefault("current_user", None)


def _current_user_or_none(request: Request) -> str | None:
    try:
        return current_user_email(request)
    except Exception:  # noqa: BLE001
        return None


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    try:
        await ensure_admin_bootstrapped()
    except Exception as exc:  # noqa: BLE001
        import logging
        logging.getLogger("dashboard").warning("admin_bootstrap_skipped: %s", exc)
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Plata Dashboard", docs_url=None, redoc_url=None, lifespan=_lifespan)
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

    @app.post("/api/kill")
    async def api_kill():
        from plata.core.bus import Channels, get_redis, publish_channel
        await publish_channel(Channels.SYSTEM_HALT, {"reason": "manual_killswitch"})
        await get_redis().set("system:state", "HALTED")
        return {"ok": True, "state": "HALTED"}

    @app.post("/api/resume")
    async def api_resume():
        from plata.core.bus import Channels, get_redis, publish_channel
        await publish_channel(Channels.SYSTEM_RESUME, {"reason": "manual_resume"})
        await get_redis().set("system:state", "RUNNING")
        return {"ok": True, "state": "RUNNING"}

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse(
            request,
            "pages/dashboard.html",
            {"active": "dashboard", "current_user": _current_user_or_none(request)},
        )

    app.include_router(auth.router)
    app.include_router(proposals.router)
    app.include_router(trades.router)
    app.include_router(risk_config.router)
    app.include_router(errors.router)
    app.include_router(agents.router)
    app.include_router(activity.router)
    return app


app = create_app()
