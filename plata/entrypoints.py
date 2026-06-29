"""Container dispatcher.

Reads $SERVICE_ENTRYPOINT and runs the appropriate set of agents.
This is the single entry-point used by all three Railway containers.
"""
from __future__ import annotations

import asyncio
import os
import signal

import uvicorn

from plata.config.settings import ServiceEntrypoint, get_settings
from plata.core.observability import configure, get_logger

configure()
_log = get_logger("entrypoints")


async def _run_dashboard(started: asyncio.Event | None = None) -> None:
    try:
        from plata.dashboard.app import app
    except Exception as exc:  # noqa: BLE001 — fall back to bare health endpoint
        _log.error("dashboard_import_failed_using_health_only", error=str(exc))
        await _run_health_server(started=started)
        return

    settings = get_settings()
    config = uvicorn.Config(
        app, host="0.0.0.0", port=settings.dashboard_port, log_level="info", lifespan="on"
    )
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve(), name="uvicorn-serve")
    if started is not None:
        while not server.started and not serve_task.done():
            await asyncio.sleep(0.05)
        started.set()
    await serve_task


async def _run_health_server(started: asyncio.Event | None = None) -> None:
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    settings = get_settings()
    config = uvicorn.Config(
        app, host="0.0.0.0", port=settings.dashboard_port, log_level="warning", lifespan="on"
    )
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve(), name="uvicorn-serve")
    if started is not None:
        while not server.started and not serve_task.done():
            await asyncio.sleep(0.05)
        started.set()
    await serve_task


async def _supervise(name: str, coro_factory) -> None:
    """Run an agent coroutine FOREVER. If it crashes, sleep with exponential
    backoff (capped at 60s) and restart. Until we added this, an unhandled
    exception in any agent's loop killed the task permanently — its sibling
    agents in the same container kept running, but the dead one stayed dead
    until the next container redeploy. That's how strategist + reviewer +
    graph_ingestion + executor + risk_manager + orchestrator have been silent
    for 6 days while the container shows "up" and position_monitor still
    heartbeats. Each agent is now self-healing.

    Records the last crash + restart count to Redis so /sysop/ can show
    "agent X crash-looped Y times" instead of just "stale".
    """
    backoff = 2
    restarts = 0
    from datetime import datetime, timezone
    try:
        from plata.core.bus import get_redis
        redis = get_redis()
    except Exception:  # noqa: BLE001
        redis = None
    while True:
        try:
            await coro_factory()
            # If the coroutine returns cleanly (no agent does this currently
            # but be safe) just exit — nothing left to supervise.
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            restarts += 1
            _log.error("agent_crashed_will_restart",
                       agent=name, restarts=restarts,
                       backoff_sec=backoff,
                       error=str(exc)[:200])
            if redis is not None:
                try:
                    await redis.hset(f"agent_supervisor:{name}", mapping={
                        "last_crash_at": datetime.now(timezone.utc).isoformat(),
                        "last_crash_error": str(exc)[:240],
                        "restart_count": str(restarts),
                    })
                except Exception:  # noqa: BLE001
                    pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            # Reset the exponential climb after a long-enough lull.
            # (If we keep crashing immediately, backoff stays at 60s.)


def _agent_task(name: str, factory) -> asyncio.Task:
    return asyncio.create_task(_supervise(name, factory), name=name)


async def _bind_then_run(http_runner, agent_factories: list[tuple[str, Any]]) -> None:
    """Bind the HTTP server first (so healthchecks pass), then start agents."""
    # Wait for Redis to finish loading the RDB before any agent touches it.
    # Same defense as the dashboard lifespan v2.24.207. Without this, agents
    # boot during Railway's Redis cold-start window, hit BusyLoadingError on
    # the first scan_iter / FT.INFO / hget call, crash, and the supervisor
    # races the same window on every restart -- silently wedging the whole
    # service. v2.24.210 fix.
    try:
        from plata.core.bus import get_redis as _gr
        r = _gr()
        for attempt in range(15):
            try:
                await r.ping()
                break
            except Exception as exc:  # noqa: BLE001 -- BusyLoading + Connection
                _log.info("entrypoint_waiting_for_redis",
                            attempt=attempt + 1, error=str(exc)[:120])
                await asyncio.sleep(2.0)
        else:
            _log.warning("entrypoint_redis_ping_timeout_proceeding")
    except Exception:  # noqa: BLE001
        pass
    # Make sure aux tables exist BEFORE any agent starts writing to them.
    # Each Railway service runs this independently — no service depends on
    # the dashboard having booted first.
    try:
        from plata.core.db import ensure_aux_tables
        await ensure_aux_tables()
    except Exception as exc:  # noqa: BLE001
        _log.warning("ensure_aux_tables_failed", error=str(exc)[:160])
    started = asyncio.Event()
    http_task = asyncio.create_task(http_runner(started=started), name="http")
    await started.wait()
    tasks = [http_task]
    for name, factory in agent_factories:
        tasks.append(_agent_task(name, factory))
    _log.info("entrypoint_started", tasks=[t.get_name() for t in tasks])
    await asyncio.gather(*tasks)


async def _run_ingestion_hub() -> None:
    from plata.agents.orchestrator import Orchestrator
    from plata.agents.scraper.runner import Scraper
    from plata.hitl.telegram_bot import TelegramBot

    await _bind_then_run(
        _run_dashboard,
        [
            ("orchestrator", lambda: Orchestrator().run()),
            ("scraper", lambda: Scraper().run()),
            ("telegram_bot", lambda: TelegramBot().run()),
        ],
    )


async def _run_intelligence_sandbox() -> None:
    from plata.agents import calibrator as _calibrator
    from plata.agents import self_improver as _self_improver
    from plata.agents.graph_ingestion import GraphIngestion
    from plata.agents.historian import HistorianResearchAgent
    from plata.agents.position_monitor import PositionMonitor
    from plata.agents.postmortem import Postmortem
    from plata.agents.researcher import Researcher
    from plata.agents.reviewer import Reviewer
    from plata.agents.strategist import Strategist

    await _bind_then_run(
        _run_health_server,
        [
            ("graph_ingestion", lambda: GraphIngestion().run()),
            ("strategist", lambda: Strategist().run()),
            ("reviewer", lambda: Reviewer().run()),
            ("position_monitor", lambda: PositionMonitor().run()),
            ("historian_research", lambda: HistorianResearchAgent().run()),
            ("researcher", lambda: Researcher().run()),
            ("postmortem", lambda: Postmortem().run()),
            # Layer 1: deterministic conviction calibration from realised win-rates.
            ("calibrator", lambda: _calibrator.run()),
            # Layer 3: LLM-driven strategic tuning, queued to /tuning/ as HITL.
            ("self_improver", lambda: _self_improver.run()),
        ],
    )


async def _run_execution_vault() -> None:
    from plata.agents.executor import Executor
    from plata.agents.risk_manager import RiskManager

    settings = get_settings()
    if not settings.bybit_api_key or not settings.bybit_api_secret:
        _log.warning("execution_vault_missing_bybit_keys_running_paper_only")

    from plata.execution.trade_sampler import run_sampler_loop

    await _bind_then_run(
        _run_health_server,
        [
            ("risk_manager", lambda: RiskManager().run()),
            ("executor", lambda: Executor().run()),
            ("trade_sampler", lambda: run_sampler_loop()),
        ],
    )


DISPATCH = {
    ServiceEntrypoint.INGESTION_HUB: _run_ingestion_hub,
    ServiceEntrypoint.INTELLIGENCE_SANDBOX: _run_intelligence_sandbox,
    ServiceEntrypoint.EXECUTION_VAULT: _run_execution_vault,
}


async def _main() -> None:
    settings = get_settings()
    entry = settings.service_entrypoint
    _log.info("dispatching", container=entry, env=settings.env)
    runner = DISPATCH.get(entry)
    if runner is None:
        raise SystemExit(f"Unknown SERVICE_ENTRYPOINT: {entry!r}")

    stop = asyncio.Event()

    def _handle_signal(*_):
        _log.info("shutdown_signal_received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:  # pragma: no cover (Windows)
            pass

    runner_task = asyncio.create_task(runner(), name=f"runner-{entry}")
    stopper_task = asyncio.create_task(stop.wait(), name="stopper")

    done, pending = await asyncio.wait(
        {runner_task, stopper_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    for task in done:
        if exc := task.exception():
            _log.error("runner_failed", error=str(exc))
            raise exc


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    asyncio.run(_main())
