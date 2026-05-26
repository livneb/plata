"""Metadata for every key in the `risk_config` Redis hash — drives the
friendly Settings → Risk tab (sliders, toggles, grouped sections, help
text). The raw key/value table stays as a fallback for anything not
covered here (so unknown keys still render and remain editable)."""

from __future__ import annotations

# Field types:
#   bool      → toggle switch
#   percent   → slider (value is interpreted as % — e.g. 1.0 == 1%)
#   fraction  → slider (value in [0,1] — used for guard_min_conviction etc.)
#   int       → slider with integer steps
#   float     → slider with decimal steps
#   currency  → number input with $ prefix
#   minutes   → slider with "Xm / Xh" formatting
#
# `group` controls section grouping order.

GROUPS: list[tuple[str, str, str]] = [
    # (key, title, description)
    ("execution",   "Execution mode",         "How orders go out — paper vs live, what gets auto-approved."),
    ("capital",     "Capital & sizing",       "How big each trade is and the daily loss ceiling."),
    ("portfolio",   "Portfolio limits",       "How many positions can be open at once."),
    ("guards",      "Behavioural guards",     "Per-proposal sanity checks that block obviously-bad trades."),
    ("strategist",  "Strategist tuning",      "Pre-LLM gates — what makes the strategist even consider an event."),
]

FIELDS: dict[str, dict] = {
    "paper_trading_mode": {
        "label": "Paper trading mode",
        "group": "execution",
        "type": "bool",
        "help": "When ON, no real orders go to Bybit or Alpaca — trades are recorded in the ledger as if filled. Turn OFF only when you've tested everything and want to risk real money.",
    },
    "auto_approve_threshold_usd": {
        "label": "Auto-approve under",
        "group": "execution",
        "type": "currency",
        "min": 0, "max": 10000, "step": 50,
        "help": "Proposals with notional under this dollar amount execute automatically. Above it, you (or Telegram) must approve.",
    },

    "risk_per_trade_pct": {
        "label": "Risk per trade",
        "group": "capital",
        "type": "percent",
        "min": 0.1, "max": 10.0, "step": 0.1,
        "help": "Notional size = this % of account equity per trade. 1% on a $10k account = $100 trades.",
    },
    "max_daily_loss_pct": {
        "label": "Daily loss kill-switch",
        "group": "capital",
        "type": "percent",
        "min": 0.5, "max": 25.0, "step": 0.5,
        "danger": True,
        "help": "If realized PnL since 00:00 UTC drops below this %, the system auto-halts. Higher = more rope to give the strategy.",
    },

    "max_open_positions": {
        "label": "Max simultaneous positions",
        "group": "portfolio",
        "type": "int",
        "min": 1, "max": 20, "step": 1,
        "help": "Hard ceiling on open trades — applies across all venues. Reject incoming proposals once full.",
    },
    "max_correlated_positions": {
        "label": "Max correlated positions",
        "group": "portfolio",
        "type": "int",
        "min": 1, "max": 10, "step": 1,
        "help": "Cap on positions in the same sector (us_megacap, crypto_l1, …). Stops over-exposure to one narrative.",
    },
    "max_gross_exposure_pct": {
        "label": "Max gross exposure",
        "group": "portfolio",
        "type": "percent",
        "min": 5.0, "max": 200.0, "step": 5.0,
        "help": "Sum of |notional| across all open positions as a % of equity. 100% = fully invested; >100% means leveraged.",
    },
    "max_net_exposure_pct": {
        "label": "Max net exposure",
        "group": "portfolio",
        "type": "percent",
        "min": 0.0, "max": 100.0, "step": 5.0,
        "help": "Long minus short, as % of equity. Lower → more market-neutral. 0% = perfectly hedged.",
    },

    "guard_min_conviction": {
        "label": "Min conviction to accept",
        "group": "guards",
        "type": "fraction",
        "min": 0.0, "max": 1.0, "step": 0.05,
        "help": "Reject proposals whose LLM-stated conviction is below this. 0.6 = "more confident than 50/50". Raise to be pickier.",
    },
    "guard_block_opposing_side": {
        "label": "Block opposing side",
        "group": "guards",
        "type": "bool",
        "help": "If you're already LONG SPY, reject a new SHORT SPY proposal (and vice versa). Prevents paying margin on two opposite sides.",
    },
    "guard_dedup_event_ulid": {
        "label": "One trade per event",
        "group": "guards",
        "type": "bool",
        "help": "If an open trade was already triggered by this event, reject another proposal that re-mentions the same headline.",
    },
    "guard_symbol_cooldown_min": {
        "label": "Symbol cooldown",
        "group": "guards",
        "type": "minutes",
        "min": 0, "max": 240, "step": 5,
        "help": "After a trade closes on a symbol, ignore new proposals on it for this many minutes. Prevents whipsaw on noisy news days.",
    },
    "guard_max_per_category_day": {
        "label": "Max trades / category / day",
        "group": "guards",
        "type": "int",
        "min": 1, "max": 20, "step": 1,
        "help": "How many trades you'll take per EventCategory (war / macro / earnings / …) per UTC day.",
    },

    "strategist_sentiment_threshold": {
        "label": "Sentiment magnitude gate",
        "group": "strategist",
        "type": "fraction",
        "min": 0.0, "max": 1.0, "step": 0.05,
        "help": "Events with `sentiment_magnitude` below this are dropped BEFORE the strategist LLM runs (saves $). 0.5 = "noticeable"; raise to 0.7 for only-the-big-news; lower to 0.3 to consider more candidates.",
    },
    "strategist_analog_k": {
        "label": "Historical analogs (K)",
        "group": "strategist",
        "type": "int",
        "min": 1, "max": 16, "step": 1,
        "help": "How many KNN-similar past events the strategist sees per call. More = better-grounded reasoning but more LLM tokens per event.",
    },
}


def field_type(key: str) -> str:
    """Return the configured field type, or 'unknown' for keys not in FIELDS."""
    return FIELDS.get(key, {}).get("type", "unknown")


def grouped_rows(rows: list[dict]) -> dict[str, list[dict]]:
    """Group rows by their declared group; unknown keys land in 'advanced'."""
    out: dict[str, list[dict]] = {g[0]: [] for g in GROUPS}
    out["advanced"] = []
    for r in rows:
        meta = FIELDS.get(r["key"])
        if not meta:
            out["advanced"].append(r)
            continue
        r = {**r, "meta": meta}
        out.setdefault(meta["group"], []).append(r)
    return out
