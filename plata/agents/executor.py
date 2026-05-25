"""Executor — places real Bybit testnet orders (live) or simulated trades (paper)."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from plata.agents.base import BaseAgent
from plata.core.bus import Streams, get_redis, publish
from plata.core.db import TradeLedger, session_scope
from plata.core.schemas import ExecutedTrade, RiskDecision, Side, TradeMode
from plata.core.ulid import new_ulid
from plata.execution.bybit_client import BybitClient
from plata.execution.universe import get_symbol


class Executor(BaseAgent):
    name = "executor"
    input_stream = Streams.APPROVED_TRADES
    group = "executor-grp"

    def __init__(self) -> None:
        super().__init__()
        self._bybit: BybitClient | None = None

    async def setup(self) -> None:
        try:
            self._bybit = BybitClient(agent=self.name)
        except Exception as e:
            self.log.warning("bybit_init_failed", error=str(e))

    async def handle(self, payload: dict[str, Any]) -> None:
        decision = RiskDecision(**payload)
        if not decision.approved:
            return

        # Read proposal details indirectly via the decision risk_snapshot or by lookup.
        # In MVP we trust the decision fields for qty/sl/tp; symbol/side are needed from
        # the proposal store (Strategist publishes them).
        proposal = await self._load_proposal(decision.proposal_ulid)
        if not proposal:
            self.log.warning("proposal_not_found", ulid=decision.proposal_ulid)
            return
        symbol = proposal["symbol"]
        side = Side(proposal["side"])
        sym = get_symbol(symbol)
        if sym is None:
            return

        # Mode flag
        redis = get_redis()
        paper = (await redis.hget("risk_config", "paper_trading_mode") or "true").lower() == "true"
        mode = TradeMode.PAPER if paper else TradeMode.LIVE

        # Price for paper fills (current ticker)
        last_price = await self._last_price(symbol)
        entry_price = Decimal(str(last_price)) if last_price else Decimal("0")

        trade_ulid = new_ulid()
        raw_response: dict[str, Any] = {}

        if mode == TradeMode.LIVE and self._bybit:
            try:
                order = await self._bybit.create_market_order(
                    symbol=symbol,
                    side="buy" if side == Side.LONG else "sell",
                    qty=decision.final_qty or Decimal("0"),
                    sl_price=decision.final_sl_price,
                    tp_price=decision.final_tp_price,
                )
                raw_response = dict(order)
                if order.get("average") or order.get("price"):
                    entry_price = Decimal(str(order.get("average") or order.get("price")))
            except Exception as e:
                await self.error_reporter.capture_exception(
                    e, agent=self.name, severity="ERROR",
                    context={"symbol": symbol, "decision_ulid": decision.ulid},
                )
                return

        executed = ExecutedTrade(
            decision_ulid=decision.ulid,
            proposal_ulid=decision.proposal_ulid,
            trade_ulid=trade_ulid,
            mode=mode,
            symbol=symbol,
            venue=sym.venue,
            instrument_type=sym.instrument_type,
            side=side,
            qty=decision.final_qty or Decimal("0"),
            entry_price=entry_price,
            sl_price=decision.final_sl_price,
            tp_price=decision.final_tp_price,
            raw_bybit_response=raw_response,
        )

        # Persist to ledger
        async with session_scope() as session:
            session.add(TradeLedger(
                trade_ulid=trade_ulid,
                proposal_id=decision.proposal_ulid,
                symbol=symbol,
                venue=sym.venue,
                instrument_type=sym.instrument_type,
                side=str(side),
                mode=str(mode),
                qty=executed.qty,
                entry_price=entry_price,
                sl_price=executed.sl_price,
                tp_price=executed.tp_price,
                fees=Decimal("0"),
                raw_bybit_response=raw_response,
                opened_at=datetime.now(timezone.utc),
            ))

        await publish(Streams.EXECUTED_TRADES, executed)
        self.log.info("trade_executed", trade_ulid=trade_ulid, mode=str(mode), symbol=symbol)

    async def _load_proposal(self, proposal_ulid: str) -> dict[str, Any] | None:
        """Look up the proposal from a Redis cache that Strategist populates.

        Strategist could also write proposals to a 'proposals:{ulid}' HASH. For
        MVP we read the original message from the trading_proposals stream
        (XRANGE) by ulid as a fallback.
        """
        redis = get_redis()
        # XRANGE search (capped) — for MVP scale this is fine. For prod, write
        # the proposal to a `proposal:{ulid}` JSON key when Strategist publishes.
        entries = await redis.xrevrange(Streams.TRADING_PROPOSALS, count=500)
        import json
        for _eid, fields in entries:
            try:
                data = json.loads(fields.get("data", "{}"))
            except Exception:
                continue
            if data.get("ulid") == proposal_ulid:
                return data
        return None

    async def _last_price(self, symbol: str) -> float | None:
        if self._bybit:
            try:
                t = await self._bybit.fetch_ticker(symbol)
                return float(t.get("last") or t.get("close") or 0)
            except Exception:
                return None
        return 50000.0 if symbol.startswith("BTC") else 1.0
