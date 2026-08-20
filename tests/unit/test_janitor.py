from datetime import datetime, timedelta, timezone

from plata.agents.janitor import DEFAULTS, cutoff_utc, merge_config, summarize


def test_merge_config_defaults_when_empty():
    assert merge_config(None) == DEFAULTS
    assert merge_config({}) == DEFAULTS


def test_merge_config_applies_numeric_overrides():
    cfg = merge_config({"signal_archive_days": "90", "interval_hours": 12})
    assert cfg["signal_archive_days"] == 90
    assert cfg["interval_hours"] == 12
    # Untouched keys keep their defaults.
    assert cfg["error_log_days"] == DEFAULTS["error_log_days"]


def test_merge_config_ignores_garbage_and_unknown_keys():
    cfg = merge_config({
        "signal_archive_days": "not-a-number",
        "error_log_days": "-5",
        "totally_unknown_key": "123",
    })
    assert cfg == DEFAULTS


def test_merge_config_zero_disables():
    cfg = merge_config({"proposals_days": "0", "graph_event_retention_days": 0})
    assert cfg["proposals_days"] == 0
    assert cfg["graph_event_retention_days"] == 0


def test_proposal_defaults_clean_noise_but_not_trades():
    # dropped diagnostic rows die fast, never-traded proposals eventually,
    # and there is intentionally NO knob that deletes traded proposals.
    assert DEFAULTS["proposals_dropped_days"] == 14
    assert DEFAULTS["proposals_days"] == 90
    assert not any("executed" in k or "traded" in k for k in DEFAULTS)


def test_cutoff_utc():
    now = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    assert cutoff_utc(30, now=now) == now - timedelta(days=30)
    assert cutoff_utc(30, now=now).tzinfo is not None


def test_summarize_nothing_cleaned():
    assert "nothing to clean" in summarize({
        "streams": {}, "graph_events": {"deleted": 0},
        "graph_edges": {"capped": 0}, "postgres": {},
    })


def test_summarize_reports_each_layer():
    line = summarize({
        "streams": {"agent_heartbeats:stream": 90000, "raw_signals:stream": 1500},
        "graph_events": {"deleted": 4200, "kept_with_impact": 10},
        "graph_edges": {"scanned": 100, "capped": 7},
        "postgres": {"signal_archive": 12000, "error_log": -1},  # -1 = errored sweep
    })
    assert "91,500 stream entries" in line
    assert "4,200 graph events" in line
    assert "7 edge evidence lists" in line
    # The errored sweep (-1) must not be summed into the row count.
    assert "12,000 Postgres rows" in line
