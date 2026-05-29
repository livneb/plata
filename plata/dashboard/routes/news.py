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


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    cfg = await get_news_config()
    drops = await redis.hgetall("scraper:filter_drops") or {}
    return templates.TemplateResponse(
        request, "pages/news.html",
        {"active": "news", "news_cfg": cfg, "news_drops": drops,
         "news_defaults": NEWS_DEFAULTS},
    )


@router.post("/save")
async def save(request: Request):
    form = await request.form()
    updates: dict = {}
    for k in ("gdelt_enabled", "reddit_enabled", "cryptopanic_enabled",
              "rss_enabled", "telegram_channels_enabled"):
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
