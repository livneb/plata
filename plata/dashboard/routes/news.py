"""News pipeline editor — sources, RSS feeds, Telegram channels, content filters.

Lives under the Knowledge sidebar group.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from plata.agents.scraper.news_config import (
    DEFAULTS as NEWS_DEFAULTS,
    get_config as get_news_config,
    save_config as save_news_config,
)
from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/news", tags=["news"])


SOURCE_NAMES = ["gdelt", "reddit", "cryptopanic", "rss"]


async def _diagnose(name: str, h: dict, cfg: dict, now_ts: float,
                     scraper_status: dict, settings_obj) -> tuple[str, str]:
    """Return (severity, message) — '' severity means healthy."""
    # 1. Scraper agent stale or never booted?
    scraper_hb = scraper_status.get("last_heartbeat")
    if scraper_hb:
        from datetime import datetime as _dt
        try:
            age = (_dt.now().astimezone() - _dt.fromisoformat(scraper_hb)).total_seconds()
            if age > 180:
                return ("error", f"Scraper agent heartbeat is {int(age // 60)}m old — "
                        "the ingestion_hub container is probably dead. "
                        "Restart it on Railway; no source will poll until then.")
        except Exception:  # noqa: BLE001
            pass
    elif scraper_status:
        return ("error", "Scraper agent has never heartbeated since boot — its "
                "background loop may have crashed before reaching the poll routine. "
                "Check /errors/.")

    # 2. Last poll too long ago? (interval × 3 is generous slack for jitter)
    last_poll = h.get("last_poll_at")
    interval = int(h.get("interval_sec") or 0)
    if last_poll and interval:
        from datetime import datetime as _dt
        try:
            age = (_dt.now().astimezone() - _dt.fromisoformat(last_poll)).total_seconds()
            if age > interval * 3 and age > 600:
                hrs = int(age // 3600); mins = int((age % 3600) // 60)
                return ("error", f"Last poll was {hrs}h{mins}m ago — interval is "
                        f"{interval}s, so this should have polled multiple times "
                        "by now. Scraper task is wedged or the container restarted "
                        "without re-starting the poll loops.")
        except Exception:  # noqa: BLE001
            pass

    # 3. Source-specific prerequisites
    if name == "reddit":
        if not (settings_obj.reddit_client_id and settings_obj.reddit_client_secret):
            return ("warn", "Reddit credentials are missing — `reddit_client_id` "
                    "and `reddit_client_secret` are not set in /settings/?tab=api "
                    "or as env vars. The poll() method returns [] until they're set.")
    if name == "rss":
        feeds = cfg.get("rss_feeds") or []
        if not feeds:
            return ("warn", "No RSS feeds configured. Add at least one feed in the "
                    "'RSS feeds' section below (format: 'Name | https://feed/url'), "
                    "then save.")

    # 4. Disabled in config
    enabled_key = f"{name}_enabled"
    if enabled_key in cfg and not cfg.get(enabled_key):
        return ("info", f"This source is disabled in the Sources panel below. "
                "Enable the checkbox + Save to start polling.")

    # 5. Last error?
    last_err = h.get("last_error") or ""
    if last_err:
        return ("warn", f"Last poll errored: {last_err}")

    # 6. Polled but raw=0 consistently
    polls = int(h.get("lifetime_polls") or 0)
    raw = int(h.get("lifetime_raw") or 0)
    if polls >= 5 and raw == 0:
        if name == "gdelt":
            return ("warn", "GDELT has run 5+ times and returned zero articles — "
                    "either the query is too narrow or you're getting rate-limited. "
                    "Edit the GDELT query in the panel below.")
        return ("warn", f"Source has run {polls} times with zero results — the "
                "upstream API may be returning empty for the current query/config.")

    return ("", "")


async def _source_rows(redis) -> list[dict]:
    import time
    from plata.config.settings import get_settings as _gs
    settings_obj = _gs()
    cfg = await get_news_config()
    scraper_status = await redis.hgetall("agent_status:scraper") or {}
    rows = []
    now_ts = time.time()
    for name in SOURCE_NAMES:
        h = await redis.hgetall(f"scraper:source:{name}") or {}
        next_poll_at = h.get("next_poll_at")
        seconds_until = None
        if next_poll_at:
            try:
                seconds_until = int(next_poll_at) - int(now_ts)
            except ValueError:
                pass
        severity, message = await _diagnose(name, h, cfg, now_ts, scraper_status, settings_obj)
        rows.append({
            "name": name,
            "status": h.get("status") or "—",
            "last_poll_at": h.get("last_poll_at"),
            "last_fetched": h.get("last_fetched"),
            "last_published": h.get("last_published"),
            "last_dup": h.get("last_dup"),
            "last_filtered": h.get("last_filtered"),
            "last_filtered_reasons": h.get("last_filtered_reasons"),
            "last_error": h.get("last_error"),
            "interval_sec": h.get("interval_sec"),
            "seconds_until_next": seconds_until,
            "run_now_pending": h.get("run_now") == "1",
            "lifetime_raw": h.get("lifetime_raw") or "0",
            "lifetime_published": h.get("lifetime_published") or "0",
            "lifetime_dup": h.get("lifetime_dup") or "0",
            "lifetime_filtered": h.get("lifetime_filtered") or "0",
            "lifetime_polls": h.get("lifetime_polls") or "0",
            "diagnose_severity": severity,
            "diagnose_message": message,
        })
    return rows


@router.get("/source/{name}/log", response_class=HTMLResponse)
async def source_log(name: str, request: Request):
    """Last 20 polls for a single source — what came in, what got dropped, why."""
    if name not in SOURCE_NAMES:
        return RedirectResponse(url="/news/", status_code=303)
    redis = get_redis()
    import json as _json
    raw = await redis.lrange(f"scraper:source:{name}:log", 0, 19) or []
    entries = []
    for r in raw:
        try:
            entries.append(_json.loads(r))
        except Exception:  # noqa: BLE001
            continue
    h = await redis.hgetall(f"scraper:source:{name}") or {}
    return templates.TemplateResponse(
        request, "pages/news_source_log.html",
        {"active": "news", "source_name": name, "entries": entries, "stats": h},
    )


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    cfg = await get_news_config()
    drops = await redis.hgetall("scraper:filter_drops") or {}
    sources = await _source_rows(redis)
    return templates.TemplateResponse(
        request, "pages/news.html",
        {"active": "news", "news_cfg": cfg, "news_drops": drops,
         "news_defaults": NEWS_DEFAULTS, "sources": sources},
    )


@router.post("/source/{name}/run_now")
async def run_now(name: str):
    if name not in SOURCE_NAMES:
        return RedirectResponse(url="/news/", status_code=303)
    redis = get_redis()
    await redis.hset(f"scraper:source:{name}", "run_now", "1")
    # Also flip status off "halted" if user is forcing a run.
    if (await redis.hget(f"scraper:source:{name}", "status")) == "halted":
        await redis.hset(f"scraper:source:{name}", "status", "idle")
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/source/{name}/toggle")
async def toggle_source(name: str):
    """Halt/resume a single source from the news page."""
    if name not in SOURCE_NAMES:
        return RedirectResponse(url="/news/", status_code=303)
    redis = get_redis()
    cur = (await redis.hget(f"scraper:source:{name}", "status")) or "idle"
    new = "halted" if cur != "halted" else "idle"
    await redis.hset(f"scraper:source:{name}", "status", new)
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/save")
async def save(request: Request):
    form = await request.form()
    updates: dict = {}
    for k in ("gdelt_enabled", "reddit_enabled", "cryptopanic_enabled",
              "rss_enabled", "telegram_channels_enabled",
              "require_keywords_enforce"):
        if k in NEWS_DEFAULTS:
            updates[k] = (form.get(k) == "on")
    if "gdelt_query" in form:
        updates["gdelt_query"] = (form.get("gdelt_query") or "").strip()
    if "min_title_len" in form:
        try:
            updates["min_title_len"] = int(form.get("min_title_len") or 0)
        except ValueError:
            pass

    def _lines(name: str) -> list[str]:
        raw = (form.get(name) or "").strip()
        return [ln.strip() for ln in raw.splitlines() if ln.strip()] if raw else []

    if "reddit_subreddits" in form:
        updates["reddit_subreddits"] = _lines("reddit_subreddits")
    if "require_keywords" in form:
        updates["require_keywords"] = _lines("require_keywords")
    if "block_keywords" in form:
        updates["block_keywords"] = _lines("block_keywords")
    if "telegram_channel_ids" in form:
        ids = []
        for ln in _lines("telegram_channel_ids"):
            try:
                ids.append(int(ln))
            except ValueError:
                pass
        updates["telegram_channel_ids"] = ids
    if "rss_feeds" in form:
        feeds = []
        for ln in _lines("rss_feeds"):
            if "|" in ln:
                name, url = ln.split("|", 1)
                feeds.append({"name": name.strip(), "url": url.strip(), "enabled": True})
            else:
                feeds.append({"name": ln, "url": ln, "enabled": True})
        updates["rss_feeds"] = feeds
    await save_news_config(updates)
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/filter_drops/reset")
async def reset_drops():
    redis = get_redis()
    await redis.delete("scraper:filter_drops")
    return RedirectResponse(url="/news/", status_code=303)
