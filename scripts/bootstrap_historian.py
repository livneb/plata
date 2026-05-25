"""Run once to seed the graph with ~1000 historical events."""
from __future__ import annotations

import asyncio

import typer

from plata.agents.historian import seed

app = typer.Typer()


@app.command()
def main(total: int = 1000, batch_size: int = 10) -> None:
    """Seed the graph with `total` synthetic historical events in batches."""
    asyncio.run(seed(total_events=total, batch_size=batch_size))


if __name__ == "__main__":
    app()
