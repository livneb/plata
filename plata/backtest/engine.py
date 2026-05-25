"""Backtest engine — replays Strategist→Risk over historical signal_archive."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select

from plata.backtest.report import summarize
from plata.core.db import (
    BacktestRun,
    BacktestTrade,
    EventPriceWindow,
    SignalArchive,
    session_scope,
)
from plata.core.observability import get_logger
from plata.core.ulid import new_ulid

_log = get_logger("backtest")


async def run_backtest(
    *,
    name: str,
    start: datetime,
    end: datetime,
    prompt_version: str = "v1",
    risk_snapshot_version: int | None = None,
) -> UUID:
    """Run a backtest over a time-window. Returns the BacktestRun id.

    For MVP, this is a *naive* engine: it iterates signal_archive rows that
    were ingested to graph, and uses the matching event_price_windows row
    (BTCUSDT) as the synthetic trade outcome (1h pct move = the trade PnL).
    Full Strategist+Risk replay is a v2 enhancement; the schema/foundation here
    makes that drop-in.
    """
    run_id = uuid4()
    async with session_scope() as session:
        session.add(BacktestRun(
            id=run_id,
            name=name,
            status="running",
            config={
                "start": start.isoformat(), "end": end.isoformat(),
                "prompt_version": prompt_version,
                "risk_snapshot_version": risk_snapshot_version,
            },
        ))

    pnls: list[Decimal] = []
    async with session_scope() as session:
        stmt = (
            select(SignalArchive, EventPriceWindow)
            .join(EventPriceWindow, SignalArchive.graph_event_ulid == EventPriceWindow.event_ulid)
            .where(SignalArchive.fetched_at.between(start, end))
            .where(SignalArchive.ingested_to_graph.is_(True))
            .where(EventPriceWindow.symbol == "BTCUSDT")
        )
        result = await session.execute(stmt)
        for sig, win in result.all():
            move = win.pct_move_1h or Decimal("0")
            pnl = Decimal("100") * move  # $100 notional per synthetic trade
            pnls.append(pnl)
            session.add(BacktestTrade(
                run_id=run_id,
                signal_ulid=sig.signal_ulid,
                trade_ulid=new_ulid(),
                symbol="BTCUSDT",
                side="long",
                qty=Decimal("100"),
                entry_price=Decimal("0"),
                exit_price=Decimal("0"),
                net_pnl=pnl,
                opened_at=sig.fetched_at,
                closed_at=sig.fetched_at,
                close_reason="tp" if pnl >= 0 else "sl",
            ))

    summary = summarize(pnls)
    async with session_scope() as session:
        run = await session.get(BacktestRun, run_id)
        if run:
            run.status = "completed"
            run.results = summary

    _log.info("backtest_complete", run_id=str(run_id), **summary)
    return run_id
