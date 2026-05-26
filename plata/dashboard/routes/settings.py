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

    # Pending tuner-proposed tweaks: most recent first, only ones still "pending".
    tuning_rows: list[dict] = []
    try:
        from sqlalchemy import desc as _desc, select as _select
        from plata.core.db import AuditLog, session_scope
        async with session_scope() as session:
            audits = (await session.execute(
                _select(AuditLog)
                .where(AuditLog.action == "proposed_config_tweak")
                .order_by(_desc(AuditLog.ts))
                .limit(50)
            )).scalars().all()
        for a in audits:
            p = a.payload or {}
            if (p.get("status") or "pending") != "pending":
                continue
            tuning_rows.append({
                "id": a.id, "ts": a.ts, "key": p.get("key"),
                "old": p.get("old"), "new": p.get("new"),
                "rationale": p.get("rationale"),
                "evidence": p.get("evidence") or {},
            })
    except Exception:  # noqa: BLE001
        pass
    return templates.TemplateResponse(
        request,
        "pages/settings.html",
        {
            "active": "settings",
            "tab": tab,
            "system_state": system_state,
            "risk_rows": rows,
            "tuning_rows": tuning_rows,
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
async def tuning_decision(audit_id: int, action: str):
    """Apply or reject a tuner-proposed config tweak. action ∈ {apply, reject}."""
    from datetime import datetime, timezone
    from sqlalchemy import select as _select
    from plata.core.bus import Channels, publish_channel
    from plata.core.db import AuditLog, ConfigSetting, session_scope
    redis = get_redis()
    async with session_scope() as session:
        row = (await session.execute(
            _select(AuditLog).where(AuditLog.id == audit_id)
        )).scalar_one_or_none()
        if row is None:
            return JSONResponse({"ok": False, "reason": "not_found"}, status_code=404)
        p = dict(row.payload or {})
        if action == "apply":
            key = p.get("key"); new_val = p.get("new")
            if key and new_val is not None:
                # Bump version + mirror to Redis (same as user-driven update).
                stmt = (_select(ConfigSetting)
                        .where(ConfigSetting.key == key)
                        .order_by(ConfigSetting.version.desc()).limit(1))
                latest = (await session.execute(stmt)).scalar_one_or_none()
                version = (latest.version + 1) if latest else 1
                session.add(ConfigSetting(
                    key=key, value={"value": new_val}, version=version,
                    updated_at=datetime.now(timezone.utc),
                ))
                await redis.hset("risk_config", key, str(new_val))
                await publish_channel(Channels.CONFIG_UPDATED, {"key": key, "value": new_val})
                p["status"] = "applied"
        elif action == "reject":
            p["status"] = "rejected"
        else:
            return JSONResponse({"ok": False, "reason": "bad_action"}, status_code=400)
        row.payload = p
    return RedirectResponse(url="/settings/?tab=tuning", status_code=303)
