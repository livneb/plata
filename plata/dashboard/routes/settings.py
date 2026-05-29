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

    # News ingestion config — live-editable list of sources + content filters.
    try:
        from plata.agents.scraper.news_config import get_config as _get_news, DEFAULTS as _news_defaults
        news_cfg = await _get_news()
        news_drops = await redis.hgetall("scraper:filter_drops") or {}
    except Exception:  # noqa: BLE001
        news_cfg = {}
        _news_defaults = {}
        news_drops = {}

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
            "news_cfg": news_cfg,
            "news_defaults": _news_defaults,
            "news_drops": news_drops,
        },
    )


@router.post("/news/save")
async def news_save(request: Request):
    """Save the editable news ingestion config (sources, query, filters)."""
    from plata.agents.scraper.news_config import DEFAULTS, save_config
    form = await request.form()
    updates: dict = {}
    # Booleans (checkboxes — absent means false)
    for k in ("gdelt_enabled", "reddit_enabled", "cryptopanic_enabled", "rss_enabled",
              "telegram_channels_enabled"):
        if k in DEFAULTS:
            updates[k] = (form.get(k) == "on")
    # Plain text/int fields
    if "gdelt_query" in form:
        updates["gdelt_query"] = (form.get("gdelt_query") or "").strip()
    if "min_title_len" in form:
        try:
            updates["min_title_len"] = int(form.get("min_title_len") or 0)
        except ValueError:
            pass
    # Lists from textareas — one per line.
    def _lines(name: str) -> list[str]:
        raw = (form.get(name) or "").strip()
        if not raw:
            return []
        return [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if "reddit_subreddits" in form:
        updates["reddit_subreddits"] = _lines("reddit_subreddits")
    if "require_keywords" in form:
        updates["require_keywords"] = _lines("require_keywords")
    if "block_keywords" in form:
        updates["block_keywords"] = _lines("block_keywords")
    if "telegram_channel_ids" in form and "telegram_channel_ids" in DEFAULTS:
        ids = []
        for ln in _lines("telegram_channel_ids"):
            try:
                ids.append(int(ln))
            except ValueError:
                pass
        updates["telegram_channel_ids"] = ids
    # RSS feeds — parsed from a textarea: each line is "name | url" or just "url".
    if "rss_feeds" in form:
        feeds = []
        for ln in _lines("rss_feeds"):
            if "|" in ln:
                name, url = ln.split("|", 1)
                feeds.append({"name": name.strip(), "url": url.strip(), "enabled": True})
            else:
                feeds.append({"name": ln, "url": ln, "enabled": True})
        updates["rss_feeds"] = feeds
    await save_config(updates)
    return RedirectResponse(url="/settings/?tab=news", status_code=303)


@router.post("/news/filter_drops/reset")
async def news_filter_drops_reset():
    redis = get_redis()
    await redis.delete("scraper:filter_drops")
    return RedirectResponse(url="/settings/?tab=news", status_code=303)


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
