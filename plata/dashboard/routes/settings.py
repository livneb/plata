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
        {"key": "google_ai_studio","label": "Google AI Studio",  "url": "https://aistudio.google.com/app/apikey"},
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

    # LLM model config — mode + per-agent overrides + suggestion catalog.
    from plata.core.llm import (
        AGENT_MODELS, AGENT_MODELS_FREE,
        MODEL_CATALOG_FREE, MODEL_CATALOG_PAID, MODEL_CATALOG_GOOGLE_FREE,
    )
    llm_cfg = await redis.hgetall("llm_config") or {}
    auto_active_free = await redis.get("llm_config:auto_active_free")
    agent_names = sorted(set(AGENT_MODELS) | set(AGENT_MODELS_FREE))
    models_view = []
    for ag in agent_names:
        models_view.append({
            "agent": ag,
            "paid": AGENT_MODELS.get(ag, ""),
            "free": AGENT_MODELS_FREE.get(ag, ""),
            "override": llm_cfg.get(f"override:{ag}", ""),
        })

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
            "llm_mode": (llm_cfg.get("mode") or "paid").lower(),
            "llm_auto_active_free": bool(auto_active_free),
            "models_view": models_view,
            "model_catalog_paid": MODEL_CATALOG_PAID,
            "model_catalog_free": MODEL_CATALOG_FREE,
            "model_catalog_google_free": MODEL_CATALOG_GOOGLE_FREE,
            "default_paid_model": llm_cfg.get("default_paid_model", ""),
            "default_free_provider": (llm_cfg.get("default_free_provider") or "both").lower(),
        },
    )


@router.post("/llm/save")
async def llm_save(request: Request):
    """Save LLM mode + per-agent model overrides."""
    redis = get_redis()
    form = await request.form()
    mode = (form.get("llm_mode") or "paid").lower()
    if mode not in ("paid", "auto", "free"):
        mode = "paid"
    await redis.hset("llm_config", "mode", mode)
    # Global defaults — pick a single paid model + free-provider preference.
    default_paid = (form.get("default_paid_model") or "").strip()
    if default_paid:
        await redis.hset("llm_config", "default_paid_model", default_paid)
    else:
        await redis.hdel("llm_config", "default_paid_model")
    free_provider = (form.get("default_free_provider") or "both").lower().strip()
    if free_provider not in ("openrouter", "google_ai_studio", "both"):
        free_provider = "both"
    await redis.hset("llm_config", "default_free_provider", free_provider)
    # Per-agent overrides — empty string means "clear override".
    from plata.core.llm import AGENT_MODELS, AGENT_MODELS_FREE
    for ag in sorted(set(AGENT_MODELS) | set(AGENT_MODELS_FREE)):
        val = (form.get(f"override:{ag}") or "").strip()
        if val:
            await redis.hset("llm_config", f"override:{ag}", val)
        else:
            await redis.hdel("llm_config", f"override:{ag}")
    # When switching to a non-auto mode, clear the sticky free pin so the
    # next call goes straight to the paid model.
    if mode != "auto":
        await redis.delete("llm_config:auto_active_free")
    return RedirectResponse(url="/settings/?tab=models", status_code=303)


@router.post("/llm/clear_free_pin")
async def llm_clear_free_pin():
    """Force-clear the sticky `auto-active-free` pin (e.g. after topping up credits)."""
    await get_redis().delete("llm_config:auto_active_free")
    return RedirectResponse(url="/settings/?tab=models", status_code=303)


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
