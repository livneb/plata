"""Tuning page — review pending Reviewer-proposed config tweaks.

Lives under the Trading sidebar group (was previously a tab inside Settings).
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import desc as _desc, select as _select

from plata.core.bus import Channels, get_redis, publish_channel
from plata.core.db import AuditLog, ConfigSetting, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/tuning", tags=["tuning"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    tuning_rows: list[dict] = []
    try:
        async with session_scope() as session:
            audits = (await session.execute(
                _select(AuditLog)
                .where(AuditLog.action == "proposed_config_tweak")
                .order_by(_desc(AuditLog.ts))
                .limit(50)
            )).scalars().all()
        seen: set[tuple] = set()
        for a in audits:
            p = a.payload or {}
            if (p.get("status") or "pending") != "pending":
                continue
            # Dedup: collapse repeat proposals with the same (key, old, new).
            # Reviewer can fire the same suggestion every 25 closures; only
            # show the newest. audits are already ordered by ts desc.
            dedup_key = (p.get("key"), str(p.get("old")), str(p.get("new")))
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            tuning_rows.append({
                "id": a.id, "ts": a.ts, "key": p.get("key"),
                "old": p.get("old"), "new": p.get("new"),
                "rationale": p.get("rationale"),
                "evidence": p.get("evidence") or {},
            })
    except Exception:  # noqa: BLE001
        pass
    return templates.TemplateResponse(
        request, "pages/tuning.html",
        {"active": "tuning", "tuning_rows": tuning_rows},
    )


@router.post("/{audit_id}/{action}")
async def decide(audit_id: int, action: str):
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
    return RedirectResponse(url="/tuning/", status_code=303)
