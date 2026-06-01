"""Unified historical timeline across signals, HITL decisions, trades, and errors.

Pulls from Postgres tables (signal_archive, audit_log, trade_ledger, error_log) and
merges into a single timeline so you can see what the system actually did over time.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, select

from plata.core.db import AuditLog, ErrorLog, SignalArchive, TradeLedger, session_scope
from plata.dashboard import templates

router = APIRouter(prefix="/history", tags=["history"])

KINDS = {
    "signal": ("📥", "ingestion"),
    "decision": ("👤", "hitl"),
    "trade": ("💱", "execution"),
    "error": ("🚨", "ops"),
}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, hours: int = 24, kind: str | None = None,
                source: str | None = None):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    events: list[dict[str, Any]] = []

    async with session_scope() as session:
        if kind in (None, "signal"):
            stmt = (select(SignalArchive)
                    .where(SignalArchive.fetched_at >= cutoff)
                    .order_by(desc(SignalArchive.fetched_at))
                    .limit(200))
            if source:
                stmt = stmt.where(SignalArchive.source == source)
            rows = (await session.execute(stmt)).scalars().all()
            for r in rows:
                # Try a few common spots for an image: metadata fields or a URL ending in .jpg/.png/etc.
                image_url = None
                md = r.metadata_ or {}
                for k in ("image", "image_url", "thumbnail", "img"):
                    if isinstance(md.get(k), str) and md[k].startswith("http"):
                        image_url = md[k]; break
                if not image_url and r.url:
                    low = r.url.lower().split("?", 1)[0]
                    if any(low.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
                        image_url = r.url
                events.append({
                    "ts": r.fetched_at,
                    "kind": "signal",
                    "actor": "scraper",
                    "title": (r.title or r.body or "")[:140],
                    "subtitle": f"source={r.source}{' · dup' if r.is_duplicate else ''}",
                    "ref": r.signal_ulid,
                    "url": r.url,
                    "image_url": image_url,
                })
        if source:
            # When filtering by signal source, suppress the other event types
            # so the page is a pure last-N-signals view for that source.
            pass
        elif kind in (None, "decision"):
            rows = (await session.execute(
                select(AuditLog)
                .where(AuditLog.ts >= cutoff)
                .order_by(desc(AuditLog.ts))
                .limit(200)
            )).scalars().all()
            for r in rows:
                events.append({
                    "ts": r.ts,
                    "kind": "decision",
                    "actor": r.actor,
                    "title": f"{r.action} · {r.target or ''}",
                    "subtitle": (r.payload or {}).get("reason") or "",
                    "ref": r.target,
                    "url": None,
                })
        if not source and kind in (None, "trade"):
            rows = (await session.execute(
                select(TradeLedger)
                .where(TradeLedger.opened_at >= cutoff)
                .order_by(desc(TradeLedger.opened_at))
                .limit(200)
            )).scalars().all()
            for r in rows:
                pnl_part = ""
                if r.net_pnl is not None:
                    pnl_part = f" · pnl {float(r.net_pnl):+.4f}"
                events.append({
                    "ts": r.opened_at,
                    "kind": "trade",
                    "actor": "executor",
                    "title": f"{r.symbol} {r.side.upper()} qty={r.qty}",
                    "subtitle": f"mode={r.mode} · entry={r.entry_price}{pnl_part}",
                    "ref": r.trade_ulid,
                    "url": None,
                })
        if not source and kind in (None, "error"):
            rows = (await session.execute(
                select(ErrorLog)
                .where(ErrorLog.ts >= cutoff)
                .order_by(desc(ErrorLog.ts))
                .limit(200)
            )).scalars().all()
            for r in rows:
                events.append({
                    "ts": r.ts,
                    "kind": "error",
                    "actor": r.agent,
                    "title": f"[{r.severity}] {r.error_type}",
                    "subtitle": (r.message or "")[:160],
                    "ref": str(r.id),
                    "url": None,
                })

    events.sort(key=lambda e: e["ts"], reverse=True)
    return templates.TemplateResponse(
        request,
        "pages/history.html",
        {
            "active": "history",
            "events": events,
            "hours": hours,
            "kind": kind,
            "kinds": KINDS,
            "source_filter": source,
        },
    )
