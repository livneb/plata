"""Compute derived market-reaction metrics from an OHLCV series."""
from __future__ import annotations

from decimal import Decimal
from math import sqrt
from typing import Sequence


def _close(bar: Sequence[float]) -> float:
    # OHLCV bar = [ts, open, high, low, close, volume]
    return float(bar[4])


def pct_move(bars: Sequence[Sequence[float]], minutes: int) -> Decimal | None:
    if not bars:
        return None
    base = _close(bars[0])
    if base == 0:
        return None
    # Find the bar closest to base_ts + minutes (assumes uniform 1-min spacing).
    idx = min(minutes, len(bars) - 1)
    target = _close(bars[idx])
    return Decimal(str((target - base) / base))


def max_drawdown(bars: Sequence[Sequence[float]]) -> Decimal | None:
    if not bars:
        return None
    base = _close(bars[0])
    if base == 0:
        return None
    lowest = min(_close(b) for b in bars)
    return Decimal(str((lowest - base) / base))


def realized_vol(bars: Sequence[Sequence[float]]) -> Decimal | None:
    if len(bars) < 2:
        return None
    rets = []
    prev = _close(bars[0])
    for b in bars[1:]:
        c = _close(b)
        if prev <= 0:
            prev = c
            continue
        rets.append((c - prev) / prev)
        prev = c
    if not rets:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    return Decimal(str(sqrt(var)))


def recovery_minutes(bars: Sequence[Sequence[float]], tolerance: float = 0.001) -> int | None:
    """Minutes after the event to return to within `tolerance` of the pre-event close."""
    if not bars:
        return None
    base = _close(bars[0])
    for i, bar in enumerate(bars[1:], start=1):
        if abs(_close(bar) - base) / max(base, 1e-9) <= tolerance:
            return i
    return None


def all_metrics(bars: Sequence[Sequence[float]]) -> dict[str, Decimal | int | None]:
    return {
        "pct_move_1h": pct_move(bars, 60),
        "pct_move_4h": pct_move(bars, 240),
        "pct_move_24h": pct_move(bars, 24 * 60),
        "max_drawdown_24h": max_drawdown(bars[: 24 * 60]),
        "realized_vol_24h": realized_vol(bars[: 24 * 60]),
        "recovery_minutes": recovery_minutes(bars),
    }
