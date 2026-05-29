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
    ("monitor",     "Position monitor",       "Watches every open position: auto-exits SL/TP/timeout, judges drift, reacts to new events."),
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
        "help": 'Reject proposals whose LLM-stated conviction is below this. 0.6 = "more confident than 50/50". Raise to be pickier.',
    },
    "guard_block_opposing_side": {
        "label": "Block opposing side",
        "group": "guards",
        "type": "bool",
        "help": "If you're already LONG SPY, reject a new SHORT SPY proposal (and vice versa). Prevents paying margin on two opposite sides.",
    },
    "guard_one_per_symbol_side": {
        "label": "One position per (symbol, side)",
        "group": "guards",
        "type": "bool",
        "help": "When ON, the strategist can't open a second long (or second short) on a symbol you already hold. New events on held symbols flow to the position monitor's event loop instead, which decides scale_up / scale_down / close. Recommended ON — without it, two different events triggering the same thesis on GLD open two duplicate positions with inconsistent sizing.",
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
        "help": 'Events with `sentiment_magnitude` below this are dropped BEFORE the strategist LLM runs (saves $). 0.5 = "noticeable"; raise to 0.7 for only-the-big-news; lower to 0.3 to consider more candidates.',
    },
    "strategist_analog_k": {
        "label": "Historical analogs (K)",
        "group": "strategist",
        "type": "int",
        "min": 1, "max": 16, "step": 1,
        "help": "How many KNN-similar past events the strategist sees per call. More = better-grounded reasoning but more LLM tokens per event.",
    },

    "monitor_check_interval_sec": {
        "label": "Check interval",
        "group": "monitor",
        "type": "int",
        "min": 10, "max": 600, "step": 10,
        "help": "How often (seconds) the monitor scans every open position. Drives SL/TP auto-exit speed too.",
    },
    "monitor_drift_threshold_pct": {
        "label": "Drift threshold",
        "group": "monitor",
        "type": "percent",
        "min": 5.0, "max": 50.0, "step": 1.0,
        "help": "If actual % move deviates this much from the strategist's predicted trajectory, the trade is flagged ⚠ drifting.",
    },
    "monitor_off_track_threshold_pct": {
        "label": "Off-track threshold",
        "group": "monitor",
        "type": "percent",
        "min": 20.0, "max": 200.0, "step": 5.0,
        "help": "Past this deviation (or if the trade moves OPPOSITE the prediction), the trade is 🛑 off-track and triggers an LLM re-evaluation.",
    },
    "monitor_max_hold_min": {
        "label": "Max hold time",
        "group": "monitor",
        "type": "minutes",
        "min": 60, "max": 43200, "step": 60,
        "help": "If a trade is still open after this many minutes, auto-close with reason=timeout. 10080 = 7 days.",
    },
    "monitor_llm_cooldown_min": {
        "label": "LLM re-eval cooldown",
        "group": "monitor",
        "type": "minutes",
        "min": 5, "max": 240, "step": 5,
        "help": "After the monitor LLM looks at an off-track trade, don't re-evaluate the same trade for this many minutes (saves LLM $).",
    },
    "monitor_event_sentiment_min": {
        "label": "Event re-eval threshold",
        "group": "monitor",
        "type": "fraction",
        "min": 0.0, "max": 1.0, "step": 0.05,
        "help": "When a new event arrives on a symbol you already hold, only trigger an LLM re-evaluation if the event's sentiment_magnitude is at least this. 0.7 = only big news.",
    },
    "monitor_auto_close_sl_tp": {
        "label": "Auto-exit on SL/TP",
        "group": "monitor",
        "type": "bool",
        "help": "When ON, the monitor publishes a TradeClosure the moment price crosses SL or TP. Recommended ON — without this paper-mode trades never honour their stops.",
    },
    "monitor_auto_close_timeout": {
        "label": "Auto-exit on timeout",
        "group": "monitor",
        "type": "bool",
        "help": "When ON, trades open longer than Max hold time auto-close.",
    },
    "monitor_auto_close_offtrack": {
        "label": "Auto-close off-track trades",
        "group": "monitor",
        "type": "bool",
        "danger": True,
        "help": "When ON, the monitor closes any trade its LLM judges off-track without your approval. Recommended OFF — the LLM suggestion goes to the Proposals page as adjustment_suggested for you to review.",
    },
    "monitor_auto_scale_up": {
        "label": "Auto-scale up",
        "group": "monitor",
        "type": "bool",
        "danger": True,
        "help": "When ON, if a new event suggests increasing a position, the monitor places the extra trade automatically. Recommended OFF — too easy to compound errors.",
    },
    "monitor_auto_scale_down": {
        "label": "Auto-scale down / close",
        "group": "monitor",
        "type": "bool",
        "danger": True,
        "help": "When ON, if a new event suggests reducing or closing a position, the monitor acts automatically. Recommended OFF.",
    },
    "monitor_auto_approve_conviction_threshold": {
        "label": "Auto-approve above conviction",
        "group": "monitor",
        "type": "fraction",
        "min": 0.0, "max": 1.0, "step": 0.05,
        "help": "Conviction-based auto-approval shortcut: any monitor adjustment (close / scale up / scale down) whose LLM conviction is ≥ this threshold gets auto-applied, bypassing the HITL toggles above. 0.6 = trip on confident verdicts; 1.0 = disable, every adjustment stays HITL.",
    },
    "account_baseline_equity_usd": {
        "label": "Baseline equity (USD)",
        "group": "capital",
        "type": "currency",
        "min": 100, "max": 10000000, "step": 100,
        "help": "Reference equity used to compute % change in the topbar's Show-details panel and in dashboard PnL summaries. Paper accounts default to $10k; bump to match your live account size.",
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
