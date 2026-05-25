"""Unified CLI: `plata run | bootstrap-historian | backtest`."""
from __future__ import annotations

import asyncio
from datetime import datetime

import typer

app = typer.Typer(help="Plata command-line interface.")


@app.command()
def run() -> None:
    """Run the dispatcher (uses $SERVICE_ENTRYPOINT)."""
    from plata.entrypoints import _main

    asyncio.run(_main())


@app.command(name="bootstrap-historian")
def bootstrap_historian(total: int = 1000, batch_size: int = 10) -> None:
    from plata.agents.historian import seed

    asyncio.run(seed(total_events=total, batch_size=batch_size))


@app.command()
def backtest(start: str, end: str, name: str = "manual") -> None:
    from plata.backtest.engine import run_backtest

    rid = asyncio.run(run_backtest(
        name=name,
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
    ))
    typer.echo(f"Run id: {rid}")


if __name__ == "__main__":
    app()
