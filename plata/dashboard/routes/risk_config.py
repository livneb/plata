"""Live-edit risk configuration. Writes to Postgres (audited) and pushes to Redis."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select

from plata.core.bus import Channels, get_redis, publish_channel
from plata.core.db import ConfigSetting, session_scope
from plata.dashboard import BASE_DIR

router = APIRouter(prefix="/risk_config", tags=["risk_config"])
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    config = await redis.hgetall("risk_config")
    return templates.TemplateResponse(
        "pages/risk_config.html",
        {"request": request, "config": config, "active": "risk_config"},
    )


@router.post("/")
async def update(request: Request, key: str = Form(...), value: str = Form(...)):
    redis = get_redis()
    # Persist to Postgres with version bump (truth)
    async with session_scope() as session:
        stmt = select(ConfigSetting).where(ConfigSetting.key == key).order_by(desc(ConfigSetting.version)).limit(1)
        latest = (await session.execute(stmt)).scalar_one_or_none()
        version = (latest.version + 1) if latest else 1
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = value
        session.add(ConfigSetting(
            key=key, value={"value": parsed}, version=version,
            updated_at=datetime.now(timezone.utc),
        ))
    # Mirror to Redis hash + notify subscribers
    await redis.hset("risk_config", key, value)
    await publish_channel(Channels.CONFIG_UPDATED, {"key": key, "value": value})
    return RedirectResponse(url="/risk_config/", status_code=303)
