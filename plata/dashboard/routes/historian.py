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


@router.get("/events")
async def events(limit: int = 100) -> JSONResponse:
    """Return recent events seeded by the historian (newest first by timestamp)."""
    redis = get_redis()
    # Best-effort scan; cap at 1000 to keep latency bounded on big graphs.
    keys: list[str] = []
    async for k in redis.scan_iter(match="event:*", count=500):
        keys.append(k)
        if len(keys) >= 1000:
            break
    out: list[dict[str, Any]] = []
    if keys:
        pipe = redis.pipeline()
        for k in keys:
            pipe.json().get(k)
        docs = await pipe.execute()
        for k, d in zip(keys, docs, strict=True):
            if not isinstance(d, dict):
                continue
            if (d.get("source") or "") != "historian":
                continue
            ents = d.get("entity_refs") or []
            out.append({
                "key": k,
                "ulid": d.get("ulid"),
                "ts": d.get("ts"),
                "category": d.get("category"),
                "region": d.get("region"),
                "summary": d.get("summary"),
                "entities": [
                    {"type": e.get("type"), "name": e.get("name")}
                    for e in ents[:10] if isinstance(e, dict)
                ],
                "has_price_impact": bool(d.get("price_impact")),
                "price_impact_symbols": list((d.get("price_impact") or {}).keys()),
            })
    out.sort(key=lambda e: e.get("ts") or "", reverse=True)
    return JSONResponse({"count": len(out), "events": out[:limit]})


def _kick_seed(total: int, batch_size: int, start_date: str, end_date: str,
               brief: str, focus: str, resume: bool = False) -> tuple[bool, str]:
    """Spawn the seed task and stash a reference so it isn't GC'd. Returns (ok, msg)."""
    from plata.core.observability import get_logger
    _log = get_logger("historian.start")
    print(f"[historian] kick_seed total={total} batch={batch_size} "
          f"window={start_date}..{end_date} brief={brief[:40]!r} resume={resume}",
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
                resume=resume,
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


async def _resume_if_interrupted() -> None:
    """Called on dashboard startup. If the previous process died mid-seed,
    pick up from where it left off using the recorded `next_batch` offset."""
    redis = get_redis()
    data = await redis.hgetall(STATUS_KEY)
    if not data:
        return
    if (data.get("state") or "") not in ("running", "stale"):
        return
    try:
        total = int(data.get("total_target") or 0)
        batch_size = int(data.get("batch_size") or 0)
        next_batch = int(data.get("next_batch") or 0)
    except (TypeError, ValueError):
        return
    if total <= 0 or batch_size <= 0:
        return
    batches = total // batch_size
    if next_batch >= batches:
        await redis.hset(STATUS_KEY, mapping={
            "state": "done",
            "finished_at": datetime.utcnow().isoformat(),
        })
        return
    print(f"[historian] auto-resume detected: batch {next_batch}/{batches}", flush=True)
    _kick_seed(
        total=total,
        batch_size=batch_size,
        start_date=data.get("start_date") or "2005-01-01",
        end_date=data.get("end_date") or "2025-12-31",
        brief=data.get("brief") or "",
        focus=data.get("focus") or "",
        resume=True,
    )
