"""Tests for plata/agents/calibrator.py — the conviction calibrator.

Covers the deterministic math (bucketing + Beta-smoothing) and the
`build_table` aggregation against a small in-memory Redis stub.
"""
from __future__ import annotations

import json

import pytest

from plata.agents import calibrator


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (None,  "<0.6"),
    (0.0,   "<0.6"),
    (0.59,  "<0.6"),
    (0.6,   "0.6-0.7"),
    (0.69,  "0.6-0.7"),
    (0.7,   "0.7-0.8"),
    (0.79,  "0.7-0.8"),
    (0.8,   "0.8-0.9"),
    (0.89,  "0.8-0.9"),
    (0.9,   "0.9-1.0"),
    (1.0,   "0.9-1.0"),
    ("garbage", "<0.6"),
])
def test_conviction_bucket(value, expected):
    assert calibrator.conviction_bucket(value) == expected


def test_beta_smoothing_no_data_returns_prior():
    # 0/0 → 0.5 — the Beta(1,1) prior is uniform, mean = 0.5.
    assert calibrator.beta_smoothed_winrate(0, 0) == pytest.approx(0.5)


def test_beta_smoothing_all_wins_pulled_toward_half():
    # 1/1 should NOT be 1.0 — that's the whole point of smoothing.
    assert calibrator.beta_smoothed_winrate(1, 1) == pytest.approx(2 / 3)
    assert calibrator.beta_smoothed_winrate(3, 3) == pytest.approx(4 / 5)


def test_beta_smoothing_all_losses_pulled_toward_half():
    assert calibrator.beta_smoothed_winrate(0, 1) == pytest.approx(1 / 3)
    assert calibrator.beta_smoothed_winrate(0, 3) == pytest.approx(1 / 5)


def test_beta_smoothing_large_sample_converges():
    # 80/100 → 81/102 ≈ 0.794, close to the naive 0.80.
    assert calibrator.beta_smoothed_winrate(80, 100) == pytest.approx(0.7941, abs=0.001)


def test_beta_smoothing_handles_garbage():
    # Wins > trades is a data error; treat as the prior, don't crash.
    assert calibrator.beta_smoothed_winrate(5, 1) == 0.5
    assert calibrator.beta_smoothed_winrate(-1, 5) == 0.5


# ---------------------------------------------------------------------------
# build_table — uses an in-memory Redis stub via monkeypatch
# ---------------------------------------------------------------------------

class _FakeRedis:
    """Tiny async stub that satisfies what `build_table` reaches for:
    `scan_iter(match=...)` and `hgetall(key)`. No need to mirror full
    Redis semantics."""

    def __init__(self, data: dict[str, dict[str, str]]):
        self._data = data

    async def scan_iter(self, match: str, count: int = 200):
        # Crude glob: only the trailing `*` case the calibrator uses.
        prefix = match.rstrip("*")
        for k in list(self._data.keys()):
            if k.startswith(prefix):
                yield k

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._data.get(key, {}))


@pytest.fixture
def fake_redis(monkeypatch):
    """Inject a fake redis into the calibrator module."""
    fake = _FakeRedis(data={})
    monkeypatch.setattr(calibrator, "get_redis", lambda: fake)
    return fake


async def test_build_table_empty(fake_redis):
    out = await calibrator.build_table(min_samples=1)
    assert out["fields"] == {}
    assert out["meta"]["cells_written"] == 0
    assert out["meta"]["trades_observed"] == 0


async def test_build_table_skips_below_min_samples(fake_redis):
    fake_redis._data = {
        # Only 2 trades in this bucket — under min_samples=3 → skipped.
        "reviewer:stats:BTCUSDT:CYBER:0.7-0.8": {
            "trades": "2", "wins": "1", "losses": "1",
        },
    }
    out = await calibrator.build_table(min_samples=3)
    assert out["fields"] == {}
    assert out["meta"]["trades_observed"] == 2  # observed but not written


async def test_build_table_aggregates_both_views(fake_redis):
    # Two symbols, same category + bucket. The category view should
    # aggregate trades across both symbols; the symbol views stay split.
    fake_redis._data = {
        "reviewer:stats:BTCUSDT:CYBER:0.7-0.8": {
            "trades": "5", "wins": "3", "losses": "2",
        },
        "reviewer:stats:ETHUSDT:CYBER:0.7-0.8": {
            "trades": "5", "wins": "2", "losses": "3",
        },
    }
    out = await calibrator.build_table(min_samples=3)
    fields = out["fields"]
    # Category view: 10 trades, 5 wins → Beta-smoothed 6/12 = 0.5.
    cat = json.loads(fields["cat:CYBER:0.7-0.8"])
    assert cat["trades"] == 10
    assert cat["wins"] == 5
    assert cat["wr"] == pytest.approx(0.5)
    assert cat["midpoint"] == 0.75
    # Symbol views are separate.
    btc = json.loads(fields["sym:BTCUSDT:0.7-0.8"])
    eth = json.loads(fields["sym:ETHUSDT:0.7-0.8"])
    assert btc["trades"] == 5 and btc["wins"] == 3
    assert eth["trades"] == 5 and eth["wins"] == 2
    # Meta: largest |wr - midpoint| should reflect the worst cell.
    assert out["meta"]["cells_written"] == 3
    assert out["meta"]["trades_observed"] == 10
    assert out["meta"]["largest_wr_delta"] > 0


async def test_build_table_ignores_malformed_keys(fake_redis):
    fake_redis._data = {
        "reviewer:stats:malformed": {"trades": "10", "wins": "5"},
        # Unknown bucket label → ignored.
        "reviewer:stats:BTCUSDT:CYBER:weird-bucket": {
            "trades": "10", "wins": "5",
        },
        # Non-numeric counters → skipped.
        "reviewer:stats:BTCUSDT:WAR:0.6-0.7": {
            "trades": "lots", "wins": "many",
        },
    }
    out = await calibrator.build_table(min_samples=1)
    assert out["fields"] == {}


# ---------------------------------------------------------------------------
# lookup — strategist-side helper
# ---------------------------------------------------------------------------

class _FakeRedisLookup(_FakeRedis):
    """Extended stub: also supports `hget` and `hgetall` for risk_config."""
    def __init__(self, table: dict[str, str], cfg: dict[str, str] | None = None):
        super().__init__(data={})
        self._table = table
        self._cfg = cfg or {}

    async def hgetall(self, key: str) -> dict[str, str]:
        if key == "risk_config":
            return dict(self._cfg)
        return {}

    async def hget(self, key: str, field: str) -> str | None:
        if key == "calibration:conviction_table":
            return self._table.get(field)
        return None


async def test_lookup_prefers_symbol_over_category(monkeypatch):
    table = {
        "sym:BTCUSDT:0.7-0.8": json.dumps(
            {"wr": 0.42, "trades": 9, "midpoint": 0.75}),
        "cat:CYBER:0.7-0.8":   json.dumps(
            {"wr": 0.55, "trades": 30, "midpoint": 0.75}),
    }
    monkeypatch.setattr(calibrator, "get_redis",
                        lambda: _FakeRedisLookup(table))
    cell = await calibrator.lookup(
        category="CYBER", symbol="BTCUSDT", conviction=0.75)
    assert cell is not None
    assert cell["source"] == "symbol"
    assert cell["wr"] == pytest.approx(0.42)


async def test_lookup_falls_back_to_category(monkeypatch):
    table = {
        "cat:CYBER:0.7-0.8": json.dumps(
            {"wr": 0.55, "trades": 30, "midpoint": 0.75}),
    }
    monkeypatch.setattr(calibrator, "get_redis",
                        lambda: _FakeRedisLookup(table))
    cell = await calibrator.lookup(
        category="CYBER", symbol="BTCUSDT", conviction=0.75)
    assert cell is not None
    assert cell["source"] == "category"
    assert cell["wr"] == pytest.approx(0.55)


async def test_lookup_returns_none_when_empty(monkeypatch):
    monkeypatch.setattr(calibrator, "get_redis",
                        lambda: _FakeRedisLookup({}))
    cell = await calibrator.lookup(
        category="WAR", symbol="SPY", conviction=0.65)
    assert cell is None


async def test_lookup_skips_cells_under_min_samples(monkeypatch):
    table = {
        "sym:BTCUSDT:0.7-0.8": json.dumps(
            {"wr": 0.42, "trades": 2, "midpoint": 0.75}),
    }
    monkeypatch.setattr(calibrator, "get_redis",
                        lambda: _FakeRedisLookup(
                            table, cfg={"calibrator_min_samples": "5"}))
    cell = await calibrator.lookup(
        category="CYBER", symbol="BTCUSDT", conviction=0.75)
    assert cell is None
