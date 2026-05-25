"""Standard quant performance metrics from a list of per-trade PnLs."""
from __future__ import annotations

from decimal import Decimal
from math import sqrt
from typing import Sequence


def win_rate(pnls: Sequence[Decimal]) -> float:
    if not pnls:
        return 0.0
    wins = sum(1 for p in pnls if p > 0)
    return wins / len(pnls)


def profit_factor(pnls: Sequence[Decimal]) -> float:
    gains = sum(float(p) for p in pnls if p > 0)
    losses = -sum(float(p) for p in pnls if p < 0)
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return gains / losses


def sharpe(returns: Sequence[float], rf: float = 0.0) -> float:
    if len(returns) < 2:
        return 0.0
    excess = [r - rf for r in returns]
    mean = sum(excess) / len(excess)
    var = sum((x - mean) ** 2 for x in excess) / len(excess)
    sd = sqrt(var)
    return mean / sd if sd > 0 else 0.0


def max_drawdown(equity_curve: Sequence[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = (eq - peak) / peak if peak else 0
        if dd < max_dd:
            max_dd = dd
    return max_dd


def summarize(pnls: Sequence[Decimal], starting_equity: float = 10000.0) -> dict[str, float]:
    if not pnls:
        return {"trades": 0}
    eq = starting_equity
    curve = [eq]
    returns: list[float] = []
    for p in pnls:
        prev = eq
        eq += float(p)
        curve.append(eq)
        returns.append((eq - prev) / prev if prev else 0)
    total_pnl = sum(float(p) for p in pnls)
    return {
        "trades": len(pnls),
        "win_rate": win_rate(pnls),
        "profit_factor": profit_factor(pnls),
        "total_pnl": total_pnl,
        "sharpe": sharpe(returns),
        "max_drawdown": max_drawdown(curve),
        "final_equity": curve[-1],
    }
