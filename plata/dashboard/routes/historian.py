"""Historian seed controller — bootstrap the graph with N dramatic historical events.

Generates events via LLM, embeds them, then attaches REAL OHLCV-derived price impact
metrics by querying the Bybit oracle for the affected symbols around the event date.
"""
from __future__ import annotations

import asyncio
from typing import Any

from datetime import datetime

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/historian", tags=["historian"])
STATUS_KEY = "historian:status"

# Keep strong references to running seed tasks so Python's GC doesn't reap them
# before they execute. Without this, `asyncio.create_task(...)` can vanish silently.
_RUNNING_TASKS: set = set()


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


def _kick_seed(total: int, batch_size: int, start_date: str, end_date: str,
               brief: str, focus: str) -> tuple[bool, str]:
    """Spawn the seed task and stash a reference so it isn't GC'd. Returns (ok, msg)."""
    from plata.core.observability import get_logger
    _log = get_logger("historian.start")
    print(f"[historian] kick_seed total={total} batch={batch_size} "
          f"window={start_date}..{end_date} brief={brief[:40]!r}",
          flush=True)
    _log.info("historian_seed_requested",
              total=total, batch_size=batch_size,
              start_date=start_date, end_date=end_date,
              brief_preview=brief[:80], focus=focus[:80])

    async def _run() -> None:
        print("[historian] seed task started", flush=True)
        # Immediately write a "running" placeholder so the UI sees the kick.
        try:
            redis = get_redis()
            await redis.hset(STATUS_KEY, mapping={
                "state": "running",
                "started_at": datetime.utcnow().isoformat(),
                "last_progress_at": datetime.utcnow().isoformat(),
                "total_target": total,
                "batch_size": batch_size,
                "start_date": start_date,
                "end_date": end_date,
                "brief": brief[:240],
                "focus": focus[:240],
                "written": 0,
                "failed_batches": 0,
                "last_error": "",
                "phase": "starting",
            })
        except Exception as exc:  # noqa: BLE001
            print(f"[historian] could not write initial status: {exc}", flush=True)

        try:
            from plata.agents.historian import seed
            print("[historian] invoking seed()", flush=True)
            _log.info("historian_seed_invoking")
            await seed(
                total_events=total,
                batch_size=batch_size,
                start_date=start_date,
                end_date=end_date,
                brief=brief[:2000],
                focus=focus[:500],
            )
            print("[historian] seed() returned cleanly", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[historian] seed CRASHED: {type(exc).__name__}: {exc}", flush=True)
            _log.exception("historian_seed_crashed", error=str(exc))
            # Surface to /errors/ table too so it's visible in the UI.
            try:
                from plata.core.error_reporter import get_error_reporter
                await get_error_reporter().capture_exception(
                    exc, agent="historian", severity="CRITICAL",
                    context={
                        "phase": "seed", "total": total, "batch_size": batch_size,
                        "window": f"{start_date}..{end_date}",
                    },
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                redis = get_redis()
                await redis.hset(STATUS_KEY, mapping={
                    "state": "failed",
                    "last_error": f"{type(exc).__name__}: {str(exc)[:200]}",
                    "finished_at": datetime.utcnow().isoformat(),
                })
            except Exception:  # noqa: BLE001
                pass

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False, "no running event loop"
    task = loop.create_task(_run(), name="historian-seed")
    _RUNNING_TASKS.add(task)
    task.add_done_callback(_RUNNING_TASKS.discard)
    print(f"[historian] task scheduled: {task.get_name()}", flush=True)
    return True, "scheduled"


@router.post("/start")
async def start(
    request: Request,
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
    status = await _status()
    if status.get("state") == "running":
        accepts_json = "application/json" in (request.headers.get("accept") or "")
        if accepts_json:
            return JSONResponse({"ok": False, "reason": "already_running"}, status_code=409)
        return RedirectResponse(url="/historian/", status_code=303)
    ok, msg = _kick_seed(total, batch_size, start_date, end_date, brief, focus)
    accepts_json = "application/json" in (request.headers.get("accept") or "")
    if accepts_json:
        return JSONResponse({"ok": ok, "reason": msg})
    return RedirectResponse(url="/historian/", status_code=303)
