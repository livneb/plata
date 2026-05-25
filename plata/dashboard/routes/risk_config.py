"""Live-edit risk configuration as a CRUD table.

Writes to Postgres (audited, versioned) and mirrors to Redis hash `risk_config`.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import desc, select

from plata.core.bus import Channels, get_redis, publish_channel
from plata.core.db import ConfigSetting, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/risk_config", tags=["risk_config"])


async def _load_rows() -> list[dict]:
    redis = get_redis()
    current = await redis.hgetall("risk_config")
    async with session_scope() as session:
        # Latest version per key from Postgres
        stmt = select(ConfigSetting).order_by(ConfigSetting.key, desc(ConfigSetting.version))
        latest_by_key: dict[str, ConfigSetting] = {}
        for row in (await session.execute(stmt)).scalars():
            if row.key not in latest_by_key:
                latest_by_key[row.key] = row

    keys = set(current.keys()) | set(latest_by_key.keys())
    rows = []
    for k in sorted(keys):
        live = current.get(k)
        meta = latest_by_key.get(k)
        rows.append(
            {
                "key": k,
                "value": live if live is not None else (json.dumps(meta.value.get("value")) if meta else ""),
                "version": meta.version if meta else 0,
                "updated_at": meta.updated_at if meta else None,
            }
        )
    return rows


async def _write(key: str, value: str) -> None:
    redis = get_redis()
    async with session_scope() as session:
        stmt = (
            select(ConfigSetting)
            .where(ConfigSetting.key == key)
            .order_by(desc(ConfigSetting.version))
            .limit(1)
        )
        latest = (await session.execute(stmt)).scalar_one_or_none()
        version = (latest.version + 1) if latest else 1
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = value
        session.add(
            ConfigSetting(
                key=key,
                value={"value": parsed},
                version=version,
                updated_at=datetime.now(timezone.utc),
            )
        )
    await redis.hset("risk_config", key, value)
    await publish_channel(Channels.CONFIG_UPDATED, {"key": key, "value": value})


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    rows = await _load_rows()
    return templates.TemplateResponse(
        request, "pages/risk_config.html", {"rows": rows, "active": "risk_config"}
    )


@router.post("/create")
async def create(key: str = Form(...), value: str = Form(...)):
    await _write(key.strip(), value)
    return RedirectResponse(url="/risk_config/", status_code=303)


@router.post("/{key}/update")
async def update(key: str, value: str = Form(...)):
    await _write(key, value)
    return RedirectResponse(url="/risk_config/", status_code=303)


@router.post("/{key}/delete")
async def delete(key: str):
    redis = get_redis()
    await redis.hdel("risk_config", key)
    await publish_channel(Channels.CONFIG_UPDATED, {"key": key, "value": None})
    return RedirectResponse(url="/risk_config/", status_code=303)
