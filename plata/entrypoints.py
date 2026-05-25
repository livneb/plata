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


async def _run_dashboard() -> None:
    from plata.dashboard.app import app

    settings = get_settings()
    config = uvicorn.Config(
        app, host="0.0.0.0", port=settings.dashboard_port, log_level="info", lifespan="on"
    )
    server = uvicorn.Server(config)
    await server.serve()


async def _run_health_server() -> None:
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
    await server.serve()


async def _supervise(name: str, coro_factory) -> None:
    """Run an agent coroutine, logging any exception so it doesn't tear down siblings."""
    try:
        await coro_factory()
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        _log.error("agent_crashed", agent=name, error=str(exc))


def _agent_task(name: str, factory) -> asyncio.Task:
    return asyncio.create_task(_supervise(name, factory), name=name)


async def _run_ingestion_hub() -> None:
    from plata.agents.orchestrator import Orchestrator
    from plata.agents.scraper.runner import Scraper
    from plata.hitl.telegram_bot import TelegramBot

    tasks = [
        _agent_task("orchestrator", lambda: Orchestrator().run()),
        _agent_task("scraper", lambda: Scraper().run()),
        _agent_task("telegram_bot", lambda: TelegramBot().run()),
        asyncio.create_task(_run_dashboard(), name="dashboard"),
    ]
    _log.info("ingestion_hub_started", agents=[t.get_name() for t in tasks])
    await asyncio.gather(*tasks)


async def _run_intelligence_sandbox() -> None:
    from plata.agents.graph_ingestion import GraphIngestion
    from plata.agents.reviewer import Reviewer
    from plata.agents.strategist import Strategist

    tasks = [
        _agent_task("graph_ingestion", lambda: GraphIngestion().run()),
        _agent_task("strategist", lambda: Strategist().run()),
        _agent_task("reviewer", lambda: Reviewer().run()),
        asyncio.create_task(_run_health_server(), name="health"),
    ]
    _log.info("intelligence_sandbox_started", agents=[t.get_name() for t in tasks])
    await asyncio.gather(*tasks)


async def _run_execution_vault() -> None:
    from plata.agents.executor import Executor
    from plata.agents.risk_manager import RiskManager

    settings = get_settings()
    if not settings.bybit_api_key or not settings.bybit_api_secret:
        _log.warning("execution_vault_missing_bybit_keys_running_paper_only")

    tasks = [
        _agent_task("risk_manager", lambda: RiskManager().run()),
        _agent_task("executor", lambda: Executor().run()),
        asyncio.create_task(_run_health_server(), name="health"),
    ]
    _log.info("execution_vault_started", agents=[t.get_name() for t in tasks])
    await asyncio.gather(*tasks)


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
