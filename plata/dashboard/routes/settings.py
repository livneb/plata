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
            "bybit_configured": bool(s.bybit_api_key and s.bybit_api_secret),
            "bybit_mode": "TESTNET" if s.bybit_testnet else "MAINNET",
            "alpaca_configured": bool(s.alpaca_api_key and s.alpaca_api_secret),
            "alpaca_mode": "PAPER" if s.alpaca_paper else "LIVE",
        },
    )


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
