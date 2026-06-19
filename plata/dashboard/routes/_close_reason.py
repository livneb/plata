"""Shared close-reason → plain-English label map.

Used by the trade detail page (outcome hero) and the closures banner so
both surfaces speak the same way when describing why a position ended.

`label_for` returns `(label, tooltip)` for a given close_reason string;
unknown reasons fall through to a literal echo so the source isn't lost.
"""
from __future__ import annotations


_REASON_MAP: dict[str, tuple[str, str]] = {
    "sl":          ("🛑 Stop-loss triggered",
                     "Price hit the stop-loss level — automatic exit."),
    "tp":          ("🎯 Take-profit reached",
                     "Price hit the take-profit level — automatic exit."),
    "manual":      ("✋ Closed by you",
                     "You clicked Close at market on this trade."),
    "kill_switch": ("🛑 Kill switch",
                     "System-wide halt triggered — every open position was force-closed."),
    "timeout":     ("⏰ Held too long",
                     "Position monitor closed it because max-hold-minutes elapsed."),
    "reset":       ("🔄 Reset",
                     "Operator clicked 'Start from scratch' — every open position was book-closed at the last mark."),
    "off_track":   ("📉 Off-track",
                     "Position monitor's LLM judged this trade had drifted from the predicted trajectory and recommended close."),
    "event_driven":("📰 Event-driven exit",
                     "A new high-impact event arrived while we held this position; the LLM recommended close."),
    "agent_close": ("🤖 Closed by agent",
                     "Position monitor closed this trade automatically — either an auto-close rule fired or its LLM judged the trade should exit. The agent's reasoning is on the trade detail page."),
}


def label_for(reason: str | None,
              *, llm_reasoning_present: bool = False) -> tuple[str, str]:
    """Return (display_label, tooltip) for `reason`. Unknown reasons echo
    back as `Closed (<reason>)` so the source isn't lost.

    `llm_reasoning_present` retroactively corrects pre-v2.24.205 rows
    where position-monitor LLM closures were stored as close_reason='manual':
    if the manual row also has adjustment_executed_reasoning attached, we
    KNOW it was actually an agent close (no human path writes that field)
    so we render it as such.
    """
    r = (reason or "").lower().strip()
    if r == "manual" and llm_reasoning_present:
        return _REASON_MAP["agent_close"]
    if r in _REASON_MAP:
        return _REASON_MAP[r]
    if r:
        return f"Closed ({r})", ""
    return "Closed", ""
