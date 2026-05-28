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
        self._alpaca = None  # type: ignore[assignment]

    async def setup(self) -> None:
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

    def _client_for(self, symbol: str, hint_venue: str | None = None,
                    hint_class: str | None = None):
        """Pick the right execution client for a symbol.
        Returns None when the venue is determined but its client isn't
        initialized — the caller falls back to a paper fill so a stock
        symbol never gets sent to Bybit (where it'd raise BadSymbol)."""
        from plata.execution.router import venue_for
        venue = venue_for(symbol, hint_venue=hint_venue, hint_class=hint_class)
        if venue == "alpaca":
            return self._alpaca  # may be None if Alpaca isn't configured
        if venue == "bybit":
            return self._bybit
        return None

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

        # Pick the venue client based on the symbol (crypto → Bybit, stock → Alpaca).
        client = self._client_for(symbol, hint_venue=proposal.get("venue"),
                                  hint_class=proposal.get("instrument_type"))
        # If we're LIVE but the required venue's client isn't configured (e.g.
        # a stock symbol with no Alpaca keys), fall back to paper fill rather
        # than route to the wrong venue (which would raise BadSymbol).
        if mode == TradeMode.LIVE and client is None:
            from plata.execution.router import venue_for as _vf
            need_venue = _vf(symbol, hint_venue=proposal.get("venue"),
                              hint_class=proposal.get("instrument_type"))
            self.log.warning("venue_client_unconfigured_paper_fallback",
                              symbol=symbol, need_venue=need_venue)
            try:
                await redis.hset(f"venue:blocked:{need_venue}", mapping={
                    "reason": "unconfigured",
                    "code": "no_client",
                    "message": f"{need_venue} client not initialized — set API keys on /settings/?tab=api",
                    "ts": datetime.now(timezone.utc).isoformat(),
                })
                await redis.expire(f"venue:blocked:{need_venue}", 7 * 24 * 60 * 60)
            except Exception:  # noqa: BLE001
                pass
            mode = TradeMode.PAPER
            raw_response = {
                "regulatory_fallback": False,
                "unconfigured_venue": need_venue,
                "note": "Forced paper fill — required venue's API client is not configured.",
            }
        if mode == TradeMode.LIVE and client:
            try:
                order = await client.create_market_order(
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
                # Special-case: Bybit (and other venues) return PermissionDenied
                # / KYC-block errors based on IP / account region. That's not a
                # bug — it's a venue policy. Fall back to a paper-mode fill so
                # the trade lands in the ledger with a clear `regulatory_fallback`
                # flag, surface a venue-wide warning, and don't DLQ the message.
                msg = str(e) or repr(e)
                is_regulatory = (
                    "retCode" in msg and "10024" in msg
                ) or "PermissionDenied" in type(e).__name__ or "regulatory" in msg.lower()
                # BadSymbol: venue rejected the symbol — same paper-fallback
                # path, different label so the audit row says "wrong venue"
                # not "regulatory". Means routing missed (e.g. stock symbol
                # reached Bybit because Alpaca client wasn't init'd).
                is_bad_symbol = "BadSymbol" in type(e).__name__ or "does not have market symbol" in msg
                if is_bad_symbol and not is_regulatory:
                    venue_key = "alpaca" if "alpaca" in type(client).__name__.lower() else "bybit"
                    try:
                        await redis.hset(f"venue:blocked:{venue_key}", mapping={
                            "reason": "bad_symbol",
                            "code": "BadSymbol",
                            "message": f"{venue_key} rejected symbol {symbol} — likely a routing bug or missing other-venue keys",
                            "ts": datetime.now(timezone.utc).isoformat(),
                        })
                        await redis.expire(f"venue:blocked:{venue_key}", 7 * 24 * 60 * 60)
                    except Exception:  # noqa: BLE001
                        pass
                    self.log.warning("venue_bad_symbol_paper_fallback",
                                      symbol=symbol, decision_ulid=decision.ulid)
                    mode = TradeMode.PAPER
                    raw_response = {
                        "regulatory_fallback": False,
                        "bad_symbol_fallback": True,
                        "rejected_by": venue_key,
                        "message": msg[:400],
                    }
                elif is_regulatory:
                    try:
                        venue_key = "alpaca" if "alpaca" in type(client).__name__.lower() else "bybit"
                        await redis.hset(f"venue:blocked:{venue_key}", mapping={
                            "reason": "regulatory",
                            "code": "10024",
                            "message": msg[:400],
                            "ts": datetime.now(timezone.utc).isoformat(),
                        })
                        await redis.expire(f"venue:blocked:{venue_key}", 7 * 24 * 60 * 60)
                    except Exception:  # noqa: BLE001
                        pass
                    self.log.warning(
                        "venue_regulatory_fallback_to_paper",
                        symbol=symbol, decision_ulid=decision.ulid,
                    )
                    mode = TradeMode.PAPER
                    raw_response = {
                        "regulatory_fallback": True,
                        "blocked_venue_code": "10024",
                        "blocked_venue_message": msg[:400],
                    }
                    # entry_price stays at the current ticker; trade still recorded.
                else:
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
        try:
            from plata.core.proposals import update_state
            await update_state(
                getattr(decision, "proposal_ulid", "") or "",
                state="executed", trade_ulid=trade_ulid, actor=self.name,
                reason=f"opened {symbol} {side} @ ${entry_price}",
            )
        except Exception:  # noqa: BLE001
            pass
        self.log.info("trade_executed", trade_ulid=trade_ulid, mode=str(mode), symbol=symbol)
        # Push to the dashboard SSE pipe for real-time UI updates.
        try:
            from plata.core.bus import publish_channel
            await publish_channel("dashboard:events", {
                "kind": "trade_opened",
                "trade_ulid": trade_ulid,
                "symbol": symbol,
                "side": str(side),
                "mode": str(mode),
                "entry_price": float(entry_price or 0),
            })
        except Exception:  # noqa: BLE001
            pass

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
        client = self._client_for(symbol)
        if client:
            try:
                t = await client.fetch_ticker(symbol)
                return float(t.get("last") or t.get("close") or 0)
            except Exception:
                return None
        return 50000.0 if symbol.startswith("BTC") else 1.0
