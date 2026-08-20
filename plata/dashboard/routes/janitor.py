"""/janitor/ — retention-sweep status page.

Shows the janitor agent's last run (what was trimmed/deleted per layer),
recent run history, the effective retention config, and a Run-now button.
The underlying JSON endpoints under /controls/janitor/* remain for scripts.
"""
from __future__ import annotations

import contextlib
import json
from datetime import datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from plata.agents.janitor import DEFAULTS, get_config
from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/janitor", tags=["janitor"])

# Human labels for the config table, grouped per storage layer.
CONFIG_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("Scheduling", [
        ("interval_hours", "Sweep interval (hours)"),
    ]),
    ("Redis streams (max entries kept)", [
        ("stream_heartbeats_maxlen", "Heartbeats stream"),
        ("stream_signals_maxlen", "Raw + enriched signal streams"),
        ("stream_trading_maxlen", "Proposal / risk / trade streams"),
        ("stream_dlq_maxlen", "Each dead-letter queue"),
    ]),
    ("Knowledge graph", [
        ("graph_event_retention_days", "Events without price impact (days)"),
        ("graph_event_impact_retention_days", "Events with price impact (days)"),
        ("graph_event_delete_cap_per_run", "Max event deletions per run"),
        ("graph_edge_evidence_cap", "Edge evidence list cap"),
    ]),
    ("Postgres retention (days, 0 = keep forever)", [
        ("signal_archive_duplicate_days", "signal_archive — duplicates"),
        ("signal_archive_days", "signal_archive — all rows"),
        ("error_log_resolved_days", "error_log — resolved"),
        ("error_log_days", "error_log — all rows"),
        ("agent_activity_log_days", "agent_activity_log"),
        ("sysop_findings_days", "sysop_findings — handled"),
        ("audit_log_days", "audit_log"),
        ("event_price_window_days", "event_price_windows"),
        ("llm_cost_days", "llm_cost"),
        ("proposals_dropped_days", "proposals — dropped (diagnostics)"),
        ("proposals_days", "proposals — never became a trade"),
        ("config_settings_keep_versions", "config_settings versions kept per key"),
    ]),
]


def _parse_summary(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _totals(summary: dict) -> dict:
    """Flatten a run summary into the four headline numbers."""
    streams = summary.get("streams") or {}
    graph = summary.get("graph_events") or {}
    edges = summary.get("graph_edges") or {}
    pg = summary.get("postgres") or {}
    return {
        "stream_entries": sum(v for v in streams.values() if isinstance(v, int)),
        "graph_events": graph.get("deleted") or 0,
        "edges_capped": edges.get("capped") or 0,
        "pg_rows": sum(v for v in pg.values() if isinstance(v, int) and v > 0),
        "pg_errors": sum(1 for v in pg.values() if v == -1),
    }


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    cfg = await get_config()
    try:
        overrides = await redis.hgetall("janitor_config") or {}
    except Exception:  # noqa: BLE001
        overrides = {}

    last_run = _parse_summary(await redis.get("janitor:last_run"))
    history_raw = await redis.lrange("janitor:history", 0, 29) or []
    history = []
    for raw in history_raw:
        s = _parse_summary(raw)
        if s:
            history.append({**_totals(s), "ran_at": s.get("ran_at"),
                            "duration_ms": s.get("duration_ms")})

    next_run_at = None
    last_at_raw = await redis.get("janitor:last_run_at")
    if last_at_raw:
        with contextlib.suppress(ValueError):
            next_run_at = (datetime.fromisoformat(last_at_raw)
                           + timedelta(hours=max(cfg["interval_hours"], 1)))

    config_groups = [
        (group_label, [
            {
                "key": key,
                "label": label,
                "value": cfg[key],
                "default": DEFAULTS[key],
                "overridden": key in overrides,
            }
            for key, label in items
        ])
        for group_label, items in CONFIG_GROUPS
    ]

    return templates.TemplateResponse(
        request, "pages/janitor.html",
        {
            "active": "janitor",
            "last_run": last_run,
            "last_totals": _totals(last_run) if last_run else None,
            "history": history,
            "next_run_at": next_run_at.isoformat() if next_run_at else None,
            "run_now_pending": bool(await redis.get("janitor:run_now")),
            "config_groups": config_groups,
        },
    )


@router.post("/run_now")
async def run_now():
    """Queue an immediate sweep; the janitor loop picks it up within ~30s."""
    redis = get_redis()
    await redis.set("janitor:run_now", "1", ex=600)
    return RedirectResponse(url="/janitor/", status_code=303)
