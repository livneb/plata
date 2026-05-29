"""Settings page with tabbed controls."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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
    # Group rows by their declared field-meta group (execution / capital / …)
    # so the Risk tab can render friendly sliders + toggles instead of a flat table.
    from plata.dashboard.risk_field_meta import FIELDS as RISK_FIELDS, GROUPS as RISK_GROUPS, grouped_rows
    risk_grouped = grouped_rows(rows)
    s = get_settings()
    # UI-managed API credentials (show last-4 only, never the cleartext secret).
    try:
        from plata.config import credentials as _creds
        creds_rows = await _creds.list_all()
    except Exception:  # noqa: BLE001
        creds_rows = []
    PROVIDERS = [
        {"key": "openrouter",      "label": "OpenRouter",        "url": "https://openrouter.ai/settings/credits"},
        {"key": "voyage",          "label": "Voyage embeddings", "url": "https://dashboard.voyageai.com/"},
        {"key": "bybit_key",       "label": "Bybit API key",     "url": "https://www.bybit.com/app/user/api-management"},
        {"key": "bybit_secret",    "label": "Bybit API secret",  "url": "https://www.bybit.com/app/user/api-management"},
        {"key": "alpaca_key",      "label": "Alpaca API key",    "url": "https://app.alpaca.markets/paper/dashboard/overview"},
        {"key": "alpaca_secret",   "label": "Alpaca API secret", "url": "https://app.alpaca.markets/paper/dashboard/overview"},
        {"key": "telegram",        "label": "Telegram bot token","url": "https://t.me/BotFather"},
        {"key": "langfuse_public", "label": "Langfuse public",   "url": "https://cloud.langfuse.com/"},
        {"key": "langfuse_secret", "label": "Langfuse secret",   "url": "https://cloud.langfuse.com/"},
    ]
    by_key = {r["provider"]: r for r in creds_rows}
    creds_view = [{**p, **by_key.get(p["key"], {})} for p in PROVIDERS]

    return templates.TemplateResponse(
        request,
        "pages/settings.html",
        {
            "active": "settings",
            "tab": tab,
            "system_state": system_state,
            "risk_rows": rows,
            "risk_grouped": risk_grouped,
            "risk_groups": RISK_GROUPS,
            "risk_fields": RISK_FIELDS,
            "paper_mode": s.default_paper_trading_mode,
            "app_version": s.app_version,
            "admin_email": s.dashboard_admin_email or "",
            "bybit_configured": bool(
                (s.bybit_api_key and s.bybit_api_secret)
                or (by_key.get("bybit_key", {}).get("tail") and by_key.get("bybit_secret", {}).get("tail"))
            ),
            "bybit_mode": "TESTNET" if s.bybit_testnet else "MAINNET",
            "alpaca_configured": bool(
                (s.alpaca_api_key and s.alpaca_api_secret)
                or (by_key.get("alpaca_key", {}).get("tail") and by_key.get("alpaca_secret", {}).get("tail"))
            ),
            "alpaca_mode": "PAPER" if s.alpaca_paper else "LIVE",
            "creds_view": creds_view,
        },
    )


@router.post("/news/save")
async def news_save_legacy(request: Request):
    """Back-compat: news moved to /news/."""
    return RedirectResponse(url="/news/save", status_code=307)


@router.post("/news/filter_drops/reset")
async def news_drops_reset_legacy():
    return RedirectResponse(url="/news/filter_drops/reset", status_code=307)


@router.post("/credentials/{provider}/save")
async def credentials_save(provider: str, request: Request):
    from plata.config import credentials as _creds
    from plata.dashboard.auth import current_user_email
    form = await request.form()
    value = (form.get("value") or "").strip()
    if not value:
        return RedirectResponse(url="/settings/?tab=api", status_code=303)
    await _creds.set_(provider, value, by=current_user_email(request))
    return RedirectResponse(url="/settings/?tab=api", status_code=303)


@router.post("/credentials/{provider}/delete")
async def credentials_delete(provider: str):
    from plata.config import credentials as _creds
    await _creds.delete(provider)
    return RedirectResponse(url="/settings/?tab=api", status_code=303)


@router.post("/tuning/{audit_id}/{action}")
async def tuning_decision_legacy(audit_id: int, action: str):
    """Back-compat: tuning moved to /tuning/. Redirect old POSTs."""
    return RedirectResponse(url=f"/tuning/{audit_id}/{action}", status_code=307)
