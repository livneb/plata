"""CLI to launch a backtest run."""
from __future__ import annotations

import asyncio
from datetime import datetime

import typer

from plata.backtest.engine import run_backtest

app = typer.Typer()


@app.command()
def main(
    start: str = typer.Option(..., help="YYYY-MM-DD start of replay window"),
    end: str = typer.Option(..., help="YYYY-MM-DD end of replay window"),
    name: str = typer.Option("manual", help="Run name"),
    prompt_version: str = typer.Option("v1"),
) -> None:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    run_id = asyncio.run(run_backtest(
        name=name, start=start_dt, end=end_dt, prompt_version=prompt_version,
    ))
    typer.echo(f"Run id: {run_id}")


if __name__ == "__main__":
    app()
