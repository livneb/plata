"""Council deliberation — multiple agents weigh in on high-conviction trades
from their own POV, the strategist applies the aggregated verdict.

Fires from Strategist.handle() AFTER the strategist's own LLM has produced
a proposal AND the conviction passes `council_min_conviction` (default
0.65). Each role-agent (risk_manager, reviewer, historian, position_monitor)
runs a short structured-output LLM call producing one critique. An
aggregator call then returns `{conviction_delta, blockers, council_notes}`.

If `blockers` is non-empty, the proposal drops with reason `council_blocked`.
Otherwise `final_conviction = original + conviction_delta` (clamped to [0,1])
and the council notes get persisted on Proposal.extras.council so the trade
detail page can surface them.

Failure handling: every POV call has a 5s timeout; total wall budget 15s.
On any failure the council returns a "no_change" verdict — the original
conviction stands.
"""
from __future__ import annotations

import asyncio
from typing import Any

from plata.core.llm import LLMClient
from plata.core.observability import get_logger

_log = get_logger("council")


_POV_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["stance", "rationale"],
    "properties": {
        "stance": {"type": "string",
                    "enum": ["agree", "concerned", "block"]},
        "rationale": {"type": "string", "minLength": 20, "maxLength": 400},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
}

_AGG_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["conviction_delta", "blockers", "council_notes"],
    "properties": {
        "conviction_delta": {"type": "number", "minimum": -0.3, "maximum": 0.1},
        "blockers": {"type": "array", "items": {"type": "string"}, "maxItems": 5},
        "council_notes": {"type": "string", "minLength": 20, "maxLength": 600},
    },
}


_POV_PROMPTS = {
    "risk_manager": (
        "You are the RISK MANAGER on a trading council. Given this proposed "
        "trade, what failure modes worry you? Consider: correlated existing "
        "positions, recent loss streaks on this symbol/category, sizing vs "
        "current budget, regulatory risk. Vote agree / concerned / block."
    ),
    "reviewer": (
        "You are the REVIEWER on a trading council, with memory of recent "
        "closed trades. Given the proposal's (symbol, conviction bucket), "
        "what does our recent performance suggest? Are we in a winning or "
        "losing streak on this pattern? Vote agree / concerned / block."
    ),
    "historian": (
        "You are the HISTORIAN on a trading council. Given the proposal's "
        "supplied analogs (similar past events with their actual price impact), "
        "do they support the directional bias? Or do they contradict it? "
        "Vote agree / concerned / block."
    ),
    "position_monitor": (
        "You are the POSITION MONITOR on a trading council. Given the current "
        "open book, is this proposal correlated with existing exposure (would "
        "concentrate risk), opposing it (would hedge), or independent? Vote "
        "agree / concerned / block."
    ),
}


def _proposal_summary(decision: dict, event_summary: str | None,
                       analogs: list | None) -> str:
    """Compact human-readable proposal description for the council prompts."""
    analog_block = ""
    if analogs:
        analog_lines = []
        for a in (analogs or [])[:5]:
            sim = float(getattr(a, "similarity", 0) or 0)
            summ = (getattr(a, "summary", "") or "")[:160]
            analog_lines.append(f"  (sim={sim:.2f}) {summ}")
        analog_block = "\nClosest historical analogs:\n" + "\n".join(analog_lines)
    return (
        f"Proposed trade:\n"
        f"  symbol: {decision.get('symbol')}\n"
        f"  side:   {decision.get('side')}\n"
        f"  conviction (strategist): {decision.get('conviction')}\n"
        f"  reasoning: {decision.get('reasoning')}\n"
        f"  suggested SL%: {decision.get('suggested_sl_pct')}\n"
        f"  suggested TP%: {decision.get('suggested_tp_pct')}\n"
        f"Triggering event: {(event_summary or '(none)')[:240]}"
        f"{analog_block}"
    )


async def _pov_call(role: str, summary: str, *, timeout: float = 5.0
                     ) -> dict | None:
    """One POV's LLM critique. Returns the parsed JSON or None on timeout/failure."""
    try:
        client = LLMClient(role)
        sys = _POV_PROMPTS.get(role, "")
        user = summary + "\n\nReturn JSON only."
        result = await asyncio.wait_for(
            client.structured(
                messages=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": user},
                ],
                schema=_POV_SCHEMA,
                schema_name=f"council_pov_{role}",
                metadata={"council_role": role},
            ),
            timeout=timeout,
        )
        return result
    except asyncio.TimeoutError:
        _log.warning("council_pov_timeout", role=role)
        return None
    except Exception as exc:  # noqa: BLE001
        _log.warning("council_pov_failed", role=role, error=str(exc)[:200])
        return None


async def deliberate(*, decision: dict, event_summary: str | None,
                      analogs: list | None) -> dict | None:
    """Run the council. Returns a verdict dict or None on total failure
    (caller should treat None as "no change" and proceed with original
    conviction).

    Verdict shape: `{conviction_delta, blockers, council_notes, povs}`.
    """
    summary = _proposal_summary(decision, event_summary, analogs)
    roles = ["risk_manager", "reviewer", "historian", "position_monitor"]
    # Fire all four critiques in parallel — total wall budget 15s.
    try:
        povs = await asyncio.wait_for(
            asyncio.gather(*(_pov_call(r, summary) for r in roles)),
            timeout=15.0,
        )
    except asyncio.TimeoutError:
        _log.warning("council_overall_timeout")
        return None
    pov_results = {r: p for r, p in zip(roles, povs) if p is not None}
    if not pov_results:
        # Every POV failed/timed out — no signal to aggregate.
        return None

    # Aggregator call: one final LLM that reads the critiques and returns
    # the delta + blockers.
    agg_input = (
        summary + "\n\nCouncil critiques:\n"
        + "\n".join(
            f"- {role}: stance={p.get('stance')}, rationale=\"{p.get('rationale','')}\""
            for role, p in pov_results.items()
        )
        + "\n\nAggregate into a single verdict. Return JSON only."
    )
    try:
        agg = await asyncio.wait_for(
            LLMClient("council").structured(
                messages=[
                    {"role": "system", "content":
                        "You aggregate council critiques into a single verdict. "
                        "Be decisive. `conviction_delta` is the change to apply "
                        "to the strategist's original conviction — negative means "
                        "the council saw concerns, slightly positive means it "
                        "reinforces the call. Reserve `blockers` for genuine "
                        "show-stoppers (regulatory red flag, recent disastrous "
                        "loss on this exact pattern, etc.)."},
                    {"role": "user", "content": agg_input},
                ],
                schema=_AGG_SCHEMA,
                schema_name="council_aggregator",
            ),
            timeout=8.0,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("council_aggregator_failed", error=str(exc)[:200])
        return None
    agg["povs"] = pov_results
    return agg
