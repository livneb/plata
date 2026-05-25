"""FastAPI dashboard — Flowbite Admin UI."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from inkcliq.dashboard.routes import agents, errors, proposals, risk_config, trades

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app() -> FastAPI:
    app = FastAPI(title="InkCliq Dashboard", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse("pages/dashboard.html", {"request": request, "active": "dashboard"})

    app.include_router(proposals.router)
    app.include_router(trades.router)
    app.include_router(risk_config.router)
    app.include_router(errors.router)
    app.include_router(agents.router)
    return app


app = create_app()
