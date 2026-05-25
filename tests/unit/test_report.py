from decimal import Decimal

from inkcliq.backtest.report import max_drawdown, profit_factor, summarize, win_rate


def test_win_rate():
    pnls = [Decimal("10"), Decimal("-5"), Decimal("0"), Decimal("3")]
    assert win_rate(pnls) == 0.5


def test_profit_factor():
    pnls = [Decimal("20"), Decimal("-5"), Decimal("-5")]
    assert profit_factor(pnls) == 2.0


def test_max_drawdown_zero_when_only_up():
    assert max_drawdown([100, 110, 120]) == 0


def test_summarize_returns_metrics():
    pnls = [Decimal("10"), Decimal("-5"), Decimal("15"), Decimal("-3")]
    out = summarize(pnls, starting_equity=1000.0)
    assert out["trades"] == 4
    assert "sharpe" in out
    assert "win_rate" in out
    assert out["total_pnl"] == 17.0
