"""Risk Manager — per-trade rules + portfolio-level checks + HITL gate."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import desc, select

from plata.agents.base import BaseAgent
from plata.config.settings import get_settings
from plata.core.bus import Channels, Streams, get_redis, publish, subscribe
from plata.core.db import TradeLedger, session_scope
from plata.core.schemas import RiskDecision, Side, TradeProposal
from plata.execution.bybit_client import BybitClient
from plata.execution.universe import get_symbol
from plata.hitl.approval_store import create_pending

DEFAULT_RISK_CONFIG: dict[str, Any] = {
    "paper_trading_mode": "true",
    "risk_per_trade_pct": "1.0",
    "max_open_positions": "3",
    "max_gross_exposure_pct": "30.0",
    "max_net_exposure_pct": "20.0",
    "max_correlated_positions": "2",
    "max_daily_loss_pct": "5.0",
    "auto_approve_threshold_usd": "1000",
    # Layer-1 guards (configurable; defaults are safe-but-permissive)
    "guard_block_opposing_side": "true",
    "guard_symbol_cooldown_min": "15",
    "guard_dedup_event_ulid": "true",
    "guard_min_conviction": "0.6",
    "guard_max_per_category_day": "3",
    # Strategist-side threshold — events below this sentiment_magnitude are dropped
    # before the strategist LLM is even called. 0.5 = "noticeable"; raise to 0.7
    # for only-the-big-news; lower to 0.3 to consider more events (costs more LLM $).
    "strategist_sentiment_threshold": "0.5",
    # Number of historical analog events the strategist asks for via KNN. More
    # = more context per LLM call (more tokens) but better-grounded reasoning.
    "strategist_analog_k": "8",
    # Position monitor — watches every open trade and reacts to SL/TP, timeouts,
    # drift from the strategist's predicted milestones, and new high-magnitude
    # events on held symbols.
    "monitor_check_interval_sec": "60",
    "monitor_drift_threshold_pct": "25.0",
    "monitor_off_track_threshold_pct": "50.0",
    "monitor_max_hold_min": "10080",          # 7 days
    "monitor_llm_cooldown_min": "30",
    "monitor_event_sentiment_min": "0.7",
    "monitor_auto_close_sl_tp": "true",       # SL/TP hit → auto close
    "monitor_auto_close_timeout": "true",     # Held > max_hold_min → auto close
    "monitor_auto_close_offtrack": "false",   # Off-track LLM verdict → HITL
    "monitor_auto_scale_up": "false",         # Event-driven scale up → HITL
    "monitor_auto_scale_down": "false",       # Event-driven scale down → HITL
}


class RiskManager(BaseAgent):
    name = "risk_manager"
    input_stream = Streams.TRADING_PROPOSALS
    group = "risk-grp"

    def __init__(self) -> None:
        super().__init__()
        self._bybit: BybitClient | None = None
        self._alpaca: Any | None = None
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
        try:
            from plata.execution.alpaca_client import AlpacaClient
            a = AlpacaClient(agent=self.name)
            if a.configured():
                self._alpaca = a
        except Exception as e:  # noqa: BLE001
            self.log.warning("alpaca_init_failed", error=str(e))

    async def _reload_config(self) -> None:
        redis = get_redis()
        raw = await redis.hgetall("risk_config")
        if not raw:
            await redis.hset("risk_config", mapping=DEFAULT_RISK_CONFIG)
            raw = DEFAULT_RISK_CONFIG
        else:
            # Backfill any new keys we've added since this hash was first seeded.
            missing = {k: v for k, v in DEFAULT_RISK_CONFIG.items() if k not in raw}
            if missing:
                await redis.hset("risk_config", mapping=missing)
                raw = {**raw, **missing}
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

        # ---- Layer-1 guards (config-driven, fail-closed where reasonable) ----
        # Conviction floor.
        try:
            min_conviction = float(self._config.get("guard_min_conviction", "0.6"))
        except (TypeError, ValueError):
            min_conviction = 0.6
        if float(proposal.conviction or 0) < min_conviction:
            await self._reject(proposal, f"conviction_below_floor:{min_conviction}")
            return

        # Open-trades guard set: needed for netting, cooldown, dedup, exposure.
        open_trades = await self._fetch_open_trades_local()

        # Dedupe by triggering_event_ulid.
        if self._cfg_bool("guard_dedup_event_ulid", True):
            if any((t.get("proposal_event_ulid") or "") == proposal.triggering_event_ulid
                   for t in open_trades):
                await self._reject(proposal, "event_already_traded")
                return

        # Opposing-side block.
        if self._cfg_bool("guard_block_opposing_side", True):
            same_sym = [t for t in open_trades if (t.get("symbol") or "").upper() == proposal.symbol.upper()]
            # Side is a StrEnum but Pydantic deserialization from the stream
            # can leave it as a plain string in some paths — str() handles both.
            proposal_side = str(proposal.side).lower()
            if any((t.get("side") or "").lower() != proposal_side for t in same_sym):
                await self._reject(proposal, "opposing_side_open_on_symbol")
                return

        # Per-symbol cooldown.
        try:
            cooldown_min = int(self._config.get("guard_symbol_cooldown_min", "15"))
        except (TypeError, ValueError):
            cooldown_min = 15
        if cooldown_min > 0:
            now = datetime.now(timezone.utc)
            same_sym = [t for t in open_trades if (t.get("symbol") or "").upper() == proposal.symbol.upper()]
            for t in same_sym:
                opened = t.get("opened_at")
                if opened and (now - opened) < timedelta(minutes=cooldown_min):
                    await self._reject(
                        proposal,
                        f"symbol_cooldown:{cooldown_min}min (last opened {int((now-opened).total_seconds())}s ago)",
                    )
                    return

        # Per-category daily cap.
        try:
            max_per_cat = int(self._config.get("guard_max_per_category_day", "3"))
        except (TypeError, ValueError):
            max_per_cat = 3
        if max_per_cat > 0:
            # Need the event's category — look up from Redis graph if possible.
            try:
                from plata.core.graph import get_event
                event_doc = await get_event(proposal.triggering_event_ulid)
                cat = (event_doc or {}).get("category")
            except Exception:  # noqa: BLE001
                cat = None
            if cat:
                today = datetime.now(timezone.utc).date()
                count_today = sum(
                    1 for t in open_trades
                    if t.get("opened_at") and t["opened_at"].date() == today
                    and (t.get("category") or "") == cat
                )
                if count_today >= max_per_cat:
                    await self._reject(proposal, f"category_cap:{cat}:{count_today}/{max_per_cat}")
                    return

        # ---- Existing portfolio caps ----
        from plata.execution.router import venue_for
        venue = venue_for(proposal.symbol)
        positions = await self._fetch_positions(venue)
        # Use the larger of venue positions or local-ledger open count.
        local_open = len(open_trades)
        effective_open = max(len(positions), local_open)
        if effective_open >= int(self._config.get("max_open_positions", 3)):
            await self._reject(proposal, "max_open_positions_reached")
            return

        if sym.sector and self._sector_count(positions, sym.sector) >= int(
            self._config.get("max_per_sector", 5)
        ):
            await self._reject(proposal, "sector_cap")
            return

        # Approximate notional sizing: 1% of equity per trade (fallback if venue unset)
        equity = await self._fetch_equity(venue)
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
            try:
                from plata.core.proposals import update_state
                await update_state(
                    proposal.ulid, state="pending_hitl",
                    reason=f"notional ${notional_usd:.2f} > auto_approve ${threshold}",
                    actor=self.name,
                )
            except Exception:  # noqa: BLE001
                pass
            approved = await self._await_approval(proposal.ulid)
            decision = decision.model_copy(update={"approved": approved})
            try:
                from plata.core.proposals import update_state
                await update_state(
                    proposal.ulid,
                    state="hitl_approved" if approved else "hitl_rejected",
                    actor="hitl",
                )
            except Exception:  # noqa: BLE001
                pass
            if not approved:
                await publish(Streams.RISK_DECISIONS, decision)
                return

        await publish(Streams.RISK_DECISIONS, decision)
        await publish(Streams.APPROVED_TRADES, decision)
        try:
            from plata.core.proposals import update_state
            await update_state(
                proposal.ulid, state="approved",
                reason=f"sized ${notional_usd:.2f} ({qty} @ ${ticker})", actor=self.name,
                extras={
                    "final_qty": str(qty), "final_notional_usd": str(notional_usd),
                    "final_sl_price": str(sl_price) if sl_price else None,
                    "final_tp_price": str(tp_price) if tp_price else None,
                    "equity": str(equity) if equity else None,
                },
            )
        except Exception:  # noqa: BLE001
            pass

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
        # NB: rejected proposals are NOT errors — they're a normal lifecycle
        # state and already visible on /proposals/?state=rejected. They were
        # being written to error_log as INFO entries that cluttered /errors/.
        try:
            from plata.core.proposals import update_state
            await update_state(proposal.ulid, state="rejected", reason=reason, actor=self.name)
        except Exception:  # noqa: BLE001
            pass

    def _sector_count(self, positions: list[dict[str, Any]], sector: str) -> int:
        n = 0
        for p in positions:
            sym = get_symbol(p.get("symbol", ""))
            if sym and sym.sector == sector:
                n += 1
        return n

    async def _fetch_equity(self, venue: str = "bybit") -> float | None:
        """Return free equity (USD) at the given venue, or None if unconfigured.
        Caller passes the venue resolved from `venue_for(proposal.symbol)`."""
        if venue == "alpaca":
            if not self._alpaca:
                return None
            try:
                bal = await self._alpaca.fetch_balance()
                # AlpacaClient returns {"total": {"USD": float}}
                total = bal.get("total", {})
                usd = total.get("USD") or total.get("USDT")
                return float(usd) if usd else None
            except Exception:  # noqa: BLE001
                return None
        # bybit
        if not self._bybit:
            return None
        try:
            bal = await self._bybit.fetch_balance()
            total = bal.get("total", {})
            usdt = total.get("USDT") or sum(v for v in total.values() if isinstance(v, (int, float)))
            return float(usdt) if usdt else None
        except Exception:
            return None

    async def _fetch_positions(self, venue: str = "bybit") -> list[dict[str, Any]]:
        if venue == "alpaca":
            if not self._alpaca:
                return []
            try:
                return await self._alpaca.fetch_positions()
            except Exception:  # noqa: BLE001
                return []
        if not self._bybit:
            return []
        try:
            return await self._bybit.fetch_positions()
        except Exception:
            return []

    async def _fetch_open_trades_local(self) -> list[dict[str, Any]]:
        """Open trades from the local Postgres ledger — works in paper mode too."""
        out: list[dict[str, Any]] = []
        try:
            async with session_scope() as session:
                rows = (await session.execute(
                    select(TradeLedger).where(TradeLedger.exit_price.is_(None))
                    .order_by(desc(TradeLedger.opened_at))
                )).scalars().all()
            for r in rows:
                out.append({
                    "trade_ulid": r.trade_ulid,
                    "proposal_id": r.proposal_id,
                    "proposal_event_ulid": (r.raw_bybit_response or {}).get("triggering_event_ulid"),
                    "symbol": r.symbol,
                    "side": r.side,
                    "qty": float(r.qty or 0),
                    "entry_price": float(r.entry_price or 0),
                    "opened_at": r.opened_at,
                    "category": (r.raw_bybit_response or {}).get("category"),
                })
        except Exception as exc:  # noqa: BLE001
            self.log.warning("local_open_trades_fetch_failed", error=str(exc)[:160])
        return out

    def _cfg_bool(self, key: str, default: bool) -> bool:
        raw = str(self._config.get(key, str(default))).strip().lower()
        return raw in ("1", "true", "yes", "on")

    async def _fetch_price(self, symbol: str) -> float | None:
        from plata.execution.router import venue_for
        venue = venue_for(symbol)
        if venue == "alpaca":
            if not self._alpaca:
                # Fallback synthetic so paper-mode flow continues without Alpaca keys.
                return 100.0
            try:
                t = await self._alpaca.fetch_ticker(symbol)
                return float(t.get("last") or t.get("close") or 0) or None
            except Exception:  # noqa: BLE001
                return None
        if not self._bybit:
            return 50000.0 if symbol.startswith("BTC") else 1.0
        try:
            t = await self._bybit.fetch_ticker(symbol)
            return float(t.get("last") or t.get("close") or 0)
        except Exception:
            return None
