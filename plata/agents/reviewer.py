"""Reviewer Agent — post-trade analysis, writes feedback edges to the graph."""
from __future__ import annotations

from typing import Any

from sqlalchemy import update

from plata.agents.base import BaseAgent
from plata.core.bus import Streams
from plata.core.db import TradeLedger, session_scope
from plata.core.graph import upsert_edge
from plata.core.llm import LLMClient
from plata.core.schemas import TradeClosure

REVIEW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "key_factor", "calibration_note"],
    "properties": {
        "verdict": {"type": "string", "enum": ["success", "failure", "mixed"]},
        "key_factor": {"type": "string", "minLength": 5, "maxLength": 300},
        "calibration_note": {"type": "string", "minLength": 5, "maxLength": 300},
    },
}

SYSTEM_PROMPT = """You are a post-trade analyst. Given a closed trade and the
events that triggered it, return a short structured verdict so future strategist
runs can learn from it.

Be honest: if the trade lost money, classify as 'failure' and identify what about
the triggering event was misleading."""


class Reviewer(BaseAgent):
    name = "reviewer"
    input_stream = Streams.TRADE_CLOSURES
    group = "reviewer-grp"

    def __init__(self) -> None:
        super().__init__()
        self._llm = LLMClient(self.name)

    async def handle(self, payload: dict[str, Any]) -> None:
        closure = TradeClosure(**payload)

        # Update Postgres ledger row
        async with session_scope() as session:
            await session.execute(
                update(TradeLedger)
                .where(TradeLedger.trade_ulid == closure.trade_ulid)
                .values(
                    exit_price=closure.exit_price,
                    gross_pnl=closure.gross_pnl,
                    net_pnl=closure.net_pnl,
                    fees=closure.fees,
                    closed_at=closure.closed_at,
                    close_reason=str(closure.close_reason),
                )
            )

        prompt = (
            f"Trade: {closure.symbol} {closure.side} qty={closure.qty}\n"
            f"Entry: {closure.entry_price} → Exit: {closure.exit_price}\n"
            f"Net PnL: {closure.net_pnl}  Close reason: {closure.close_reason}\n"
        )
        review = await self._llm.structured(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            schema=REVIEW_SCHEMA,
            schema_name="trade_review",
            metadata={"trade_ulid": closure.trade_ulid},
        )

        # Weight signed by PnL so edges accumulate calibration
        weight = float(closure.net_pnl)
        await upsert_edge(
            src=f"proposal:{closure.proposal_ulid}",
            rel="caused",
            dst=f"trade:{closure.trade_ulid}",
            weight=weight,
            extra={
                "verdict": review["verdict"],
                "key_factor": review["key_factor"],
                "calibration_note": review["calibration_note"],
            },
        )
        self.log.info(
            "trade_reviewed", trade_ulid=closure.trade_ulid,
            verdict=review["verdict"], net_pnl=str(closure.net_pnl),
        )
