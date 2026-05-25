"""Risk Manager — per-trade rules + portfolio-level checks + HITL gate."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

from inkcliq.agents.base import BaseAgent
from inkcliq.config.settings import get_settings
from inkcliq.core.bus import Channels, Streams, get_redis, publish, subscribe
from inkcliq.core.schemas import RiskDecision, Side, TradeProposal
from inkcliq.execution.bybit_client import BybitClient
from inkcliq.execution.universe import get_symbol
from inkcliq.hitl.approval_store import create_pending

DEFAULT_RISK_CONFIG: dict[str, Any] = {
    "paper_trading_mode": "true",
    "risk_per_trade_pct": "1.0",
    "max_open_positions": "3",
    "max_gross_exposure_pct": "30.0",
    "max_net_exposure_pct": "20.0",
    "max_correlated_positions": "2",
    "max_daily_loss_pct": "5.0",
    "auto_approve_threshold_usd": "1000",
}


class RiskManager(BaseAgent):
    name = "risk_manager"
    input_stream = Streams.TRADING_PROPOSALS
    group = "risk-grp"

    def __init__(self) -> None:
        super().__init__()
        self._bybit: BybitClient | None = None
        self._config: dict[str, str] = {}

    async def setup(self) -> None:
        await self._reload_config()
        # Re-load on config_updated
        asyncio.create_task(self._config_watcher(), name="risk-config-watcher")
        # Listen on per-proposal approval channels via short-lived subscriptions
        asyncio.create_task(self._approval_router(), name="risk-approval-router")
        try:
            self._bybit = BybitClient(agent=self.name)
        except Exception as e:
            self.log.warning("bybit_init_failed", error=str(e))

    async def _reload_config(self) -> None:
        redis = get_redis()
        raw = await redis.hgetall("risk_config")
        if not raw:
            await redis.hset("risk_config", mapping=DEFAULT_RISK_CONFIG)
            raw = DEFAULT_RISK_CONFIG
        self._config = dict(raw)

    async def _config_watcher(self) -> None:
        async for _channel, _msg in subscribe(Channels.CONFIG_UPDATED):
            await self._reload_config()
            self.log.info("risk_config_reloaded")

    async def _approval_router(self) -> None:
        """We can't keep N subscriptions open. The handler awaits per-proposal pub/sub inline."""
        return  # Placeholder; per-proposal subscription is done inside handle().

    async def handle(self, payload: dict[str, Any]) -> None:
        proposal = TradeProposal(**payload)
        sym = get_symbol(proposal.symbol)
        if sym is None:
            await self._reject(proposal, "symbol_not_in_universe")
            return

        # Hard rules
        positions = await self._fetch_positions()
        if len(positions) >= int(self._config.get("max_open_positions", 3)):
            await self._reject(proposal, "max_open_positions_reached")
            return

        if sym.sector and self._sector_count(positions, sym.sector) >= int(
            self._config.get("max_per_sector", 5)
        ):
            await self._reject(proposal, "sector_cap")
            return

        # Approximate notional sizing: 1% of equity per trade (fallback if no Bybit)
        equity = await self._fetch_equity()
        risk_pct = Decimal(self._config.get("risk_per_trade_pct", "1.0")) / Decimal("100")
        notional_usd = (Decimal(str(equity)) * risk_pct) if equity else Decimal("100")

        ticker = await self._fetch_price(proposal.symbol)
        if ticker is None:
            await self._reject(proposal, "no_price_feed")
            return
        qty = (notional_usd / Decimal(str(ticker))).quantize(Decimal("0.000001"))
        if qty < sym.min_qty:
            await self._reject(proposal, "qty_below_min")
            return

        sl_pct = Decimal(str(proposal.suggested_sl_pct or 0.02))
        tp_pct = Decimal(str(proposal.suggested_tp_pct or 0.04))
        if proposal.side == Side.LONG:
            sl_price = Decimal(str(ticker)) * (Decimal("1") - sl_pct)
            tp_price = Decimal(str(ticker)) * (Decimal("1") + tp_pct)
        else:
            sl_price = Decimal(str(ticker)) * (Decimal("1") + sl_pct)
            tp_price = Decimal(str(ticker)) * (Decimal("1") - tp_pct)

        threshold = Decimal(self._config.get("auto_approve_threshold_usd", "1000"))
        requires_hitl = notional_usd > threshold

        decision = RiskDecision(
            proposal_ulid=proposal.ulid,
            approved=True,
            requires_hitl=requires_hitl,
            final_qty=qty,
            final_notional_usd=notional_usd,
            final_sl_price=sl_price,
            final_tp_price=tp_price,
            risk_snapshot={
                "equity": str(equity) if equity else None,
                "open_positions": len(positions),
                "config": dict(self._config),
            },
        )

        if requires_hitl:
            await create_pending(
                proposal.ulid,
                proposal=proposal.model_dump(mode="json"),
                reason=f"notional ${notional_usd:.2f} > auto_approve ${threshold}",
            )
            approved = await self._await_approval(proposal.ulid)
            decision = decision.model_copy(update={"approved": approved})
            if not approved:
                await publish(Streams.RISK_DECISIONS, decision)
                return

        await publish(Streams.RISK_DECISIONS, decision)
        await publish(Streams.APPROVED_TRADES, decision)

    async def _await_approval(self, proposal_ulid: str, timeout_sec: int = 1800) -> bool:
        channel = Channels.approval(proposal_ulid)
        try:
            async def _wait():
                async for _c, msg in subscribe(channel):
                    if isinstance(msg, dict) and "approved" in msg:
                        return bool(msg["approved"])
                return False
            return await asyncio.wait_for(_wait(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            return False

    async def _reject(self, proposal: TradeProposal, reason: str) -> None:
        decision = RiskDecision(
            proposal_ulid=proposal.ulid, approved=False, requires_hitl=False,
            rejection_reason=reason,
        )
        await publish(Streams.RISK_DECISIONS, decision)
        await self.error_reporter.capture(
            agent=self.name, severity="INFO", error_type="ProposalRejected",
            message=reason,
            context={"proposal_ulid": proposal.ulid, "symbol": proposal.symbol},
        )

    def _sector_count(self, positions: list[dict[str, Any]], sector: str) -> int:
        n = 0
        for p in positions:
            sym = get_symbol(p.get("symbol", ""))
            if sym and sym.sector == sector:
                n += 1
        return n

    async def _fetch_equity(self) -> float | None:
        if not self._bybit:
            return None
        try:
            bal = await self._bybit.fetch_balance()
            total = bal.get("total", {})
            usdt = total.get("USDT") or sum(v for v in total.values() if isinstance(v, (int, float)))
            return float(usdt) if usdt else None
        except Exception:
            return None

    async def _fetch_positions(self) -> list[dict[str, Any]]:
        if not self._bybit:
            return []
        try:
            return await self._bybit.fetch_positions()
        except Exception:
            return []

    async def _fetch_price(self, symbol: str) -> float | None:
        if not self._bybit:
            # Fallback synthetic for paper mode
            return 50000.0 if symbol.startswith("BTC") else 1.0
        try:
            t = await self._bybit.fetch_ticker(symbol)
            return float(t.get("last") or t.get("close") or 0)
        except Exception:
            return None
