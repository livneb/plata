from decimal import Decimal

from inkcliq.oracle.impact_metrics import (
    all_metrics,
    max_drawdown,
    pct_move,
    realized_vol,
    recovery_minutes,
)


def _bars(closes):
    return [[i * 60, c, c, c, c, 1.0] for i, c in enumerate(closes)]


def test_pct_move_basic():
    bars = _bars([100, 101, 102, 103])
    assert pct_move(bars, 1) == Decimal("0.01")


def test_max_drawdown_negative():
    bars = _bars([100, 95, 90, 92])
    dd = max_drawdown(bars)
    assert dd is not None
    assert dd < 0


def test_realized_vol_nonzero():
    bars = _bars([100, 105, 95, 110, 90])
    vol = realized_vol(bars)
    assert vol is not None
    assert vol > 0


def test_recovery_minutes_returns_index():
    bars = _bars([100, 90, 85, 100, 102])
    assert recovery_minutes(bars) == 3


def test_all_metrics_returns_all_keys():
    bars = _bars([100 + i * 0.1 for i in range(60 * 24 + 60)])
    out = all_metrics(bars)
    assert {"pct_move_1h", "pct_move_4h", "pct_move_24h", "max_drawdown_24h",
            "realized_vol_24h", "recovery_minutes"}.issubset(out.keys())
