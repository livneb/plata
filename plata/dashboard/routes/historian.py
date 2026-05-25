"""Historian seed controller — bootstrap the graph with N dramatic historical events.

Generates events via LLM, embeds them, then attaches REAL OHLCV-derived price impact
metrics by querying the Bybit oracle for the affected symbols around the event date.
"""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/historian", tags=["historian"])
STATUS_KEY = "historian:status"


async def _status() -> dict[str, Any]:
    redis = get_redis()
    data = await redis.hgetall(STATUS_KEY)
    if not data:
        return {"state": "never_run"}
    # Mark as stale if claimed "running" but no progress in 3 minutes.
    if data.get("state") == "running":
        from datetime import datetime
        last_raw = data.get("last_progress_at") or data.get("started_at") or ""
        try:
            last_dt = datetime.fromisoformat(last_raw)
            age = (datetime.utcnow() - last_dt).total_seconds()
            if age > 180:
                data["state"] = "stale"
                data["stale_age_s"] = int(age)
        except Exception:  # noqa: BLE001
            pass
    return data


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    status = await _status()
    return templates.TemplateResponse(
        request, "pages/historian.html", {"active": "historian", "status": status}
    )


@router.get("/_fragment", response_class=HTMLResponse)
async def fragment(request: Request):
    status = await _status()
    return templates.TemplateResponse(
        request, "pages/_historian_fragment.html", {"status": status}
    )


@router.post("/reset")
async def reset():
    """Clear the historian status hash. Used to dismiss stale/zombie runs."""
    redis = get_redis()
    await redis.delete(STATUS_KEY)
    return RedirectResponse(url="/historian/", status_code=303)


@router.post("/start")
async def start(
    total: int = Form(100),
    batch_size: int = Form(10),
    start_date: str = Form("2005-01-01"),
    end_date: str = Form("2025-12-31"),
    brief: str = Form(""),
    focus: str = Form(""),
):
    total = max(10, min(int(total), 2000))
    batch_size = max(1, min(int(batch_size), 25))
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    # Honor a "running" guard only if it's not actually stale.
    status = await _status()
    if status.get("state") == "running":
        return RedirectResponse(url="/historian/", status_code=303)
    from plata.core.observability import get_logger
    _log = get_logger("historian.start")
    _log.info("historian_seed_requested",
              total=total, batch_size=batch_size,
              start_date=start_date, end_date=end_date,
              brief_preview=brief[:80], focus=focus[:80])

    async def _run() -> None:
        try:
            from plata.agents.historian import seed
            _log.info("historian_seed_invoking")
            await seed(
                total_events=total,
                batch_size=batch_size,
                start_date=start_date,
                end_date=end_date,
                brief=brief[:2000],
                focus=focus[:500],
            )
        except Exception as exc:  # noqa: BLE001
            _log.exception("historian_seed_crashed", error=str(exc))
            try:
                redis = get_redis()
                await redis.hset(STATUS_KEY, mapping={
                    "state": "failed",
                    "last_error": f"{type(exc).__name__}: {str(exc)[:200]}",
                    "finished_at": datetime.utcnow().isoformat(),
                })
            except Exception:  # noqa: BLE001
                pass

    asyncio.create_task(_run(), name="historian-seed")
    return RedirectResponse(url="/historian/", status_code=303)
