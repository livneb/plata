"""FastAPI dashboard — Flowbite Admin UI."""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from plata.dashboard import BASE_DIR
from plata.dashboard.routes import agents, errors, proposals, risk_config, trades

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app() -> FastAPI:
    app = FastAPI(title="Plata Dashboard", docs_url=None, redoc_url=None)
    static_dir = BASE_DIR / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse(request, "pages/dashboard.html", {"active": "dashboard"})

    app.include_router(proposals.router)
    app.include_router(trades.router)
    app.include_router(risk_config.router)
    app.include_router(errors.router)
    app.include_router(agents.router)
    return app


app = create_app()
