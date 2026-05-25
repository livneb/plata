"""Settings page with tabbed controls."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from plata.config.settings import get_settings
from plata.core.bus import get_redis
from plata.dashboard import templates
from plata.dashboard.routes.risk_config import _load_rows

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, tab: str = "controls"):
    redis = get_redis()
    system_state = await redis.get("system:state") or "RUNNING"
    rows = await _load_rows()
    s = get_settings()
    return templates.TemplateResponse(
        request,
        "pages/settings.html",
        {
            "active": "settings",
            "tab": tab,
            "system_state": system_state,
            "risk_rows": rows,
            "paper_mode": s.default_paper_trading_mode,
            "app_version": s.app_version,
            "admin_email": s.dashboard_admin_email or "",
            "bybit_configured": bool(s.bybit_api_key and s.bybit_api_secret),
            "bybit_mode": "TESTNET" if s.bybit_testnet else "MAINNET",
            "alpaca_configured": bool(s.alpaca_api_key and s.alpaca_api_secret),
            "alpaca_mode": "PAPER" if s.alpaca_paper else "LIVE",
        },
    )
