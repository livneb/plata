"""Editable news ingestion config — sources, queries, content filters.

Stored in Redis hash `news_config` so it can be edited live from /settings/?tab=news
without restarting the scraper. Sources read this on each poll.
"""
from __future__ import annotations

import json
from typing import Any

from plata.core.bus import get_redis

HASH = "news_config"

DEFAULTS: dict[str, Any] = {
    # --- Source enable flags ---
    "gdelt_enabled": True,
    "reddit_enabled": True,
    "cryptopanic_enabled": True,
    "rss_enabled": True,
    "telegram_channels_enabled": False,
    # Chat IDs of Telegram channels / groups to listen on. The bot must be
    # added to each channel (use /joininfo in DM for instructions). Negative
    # numbers for groups/channels (e.g. -1001234567890).
    "telegram_channel_ids": [],
    # --- Source-specific config ---
    "gdelt_query": (
        'sourcelang:eng (war OR sanction OR cyberattack OR "central bank" OR '
        'inflation OR crypto OR bitcoin OR ethereum OR regulation OR '
        '"interest rate" OR fed OR ecb OR earnings OR tariff)'
    ),
    "reddit_subreddits": ["CryptoCurrency", "wallstreetbets", "Bitcoin", "ethfinance"],
    "rss_feeds": [
        # name, url, enabled — list of dicts
        # {"name": "Reuters Business", "url": "https://...", "enabled": True}
    ],
    # --- Content filter (applied to every signal before publish) ---
    # Drop if title length < this (junk one-word headlines).
    "min_title_len": 20,
    # If non-empty: signal MUST match at least one of these (case-insensitive
    # substring). Empty list = no allowlist requirement.
    "require_keywords": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "stablecoin",
        "fed", "ecb", "boj", "interest rate", "inflation", "cpi", "ppi",
        "gdp", "tariff", "sanction", "war", "regulation", "sec ",
        "earnings", "guidance", "merger", "acquisition", "ipo",
        "stock", "nasdaq", "s&p", "treasury", "yield", "bond",
        "oil", "gold", "silver",
    ],
    # If signal matches any of these (case-insensitive substring), drop it.
    "block_keywords": [
        "neighbor", "fence", "celebrity", "kardashian", "horoscope",
        "recipe", "tiktok dance", "viral video", "pet", "puppy",
        "wedding", "divorce", "obituary", "high school", "weather forecast",
    ],
}


def _coerce_list(v: Any) -> list:
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            j = json.loads(v)
            if isinstance(j, list):
                return j
        except json.JSONDecodeError:
            return [s.strip() for s in v.replace("\n", ",").split(",") if s.strip()]
    return []


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("1", "true", "yes", "on")
    return bool(v)


async def get_config() -> dict[str, Any]:
    """Return the merged config (defaults + overrides from Redis)."""
    redis = get_redis()
    raw = await redis.hgetall(HASH) or {}
    out: dict[str, Any] = {}
    for k, default in DEFAULTS.items():
        if k not in raw:
            out[k] = default
            continue
        v = raw[k]
        if isinstance(default, bool):
            out[k] = _coerce_bool(v)
        elif isinstance(default, int):
            try:
                out[k] = int(v)
            except (ValueError, TypeError):
                out[k] = default
        elif isinstance(default, list):
            out[k] = _coerce_list(v) or default
        else:
            out[k] = v if isinstance(v, str) else default
    return out


async def save_config(updates: dict[str, Any]) -> None:
    redis = get_redis()
    mapping: dict[str, str] = {}
    for k, v in updates.items():
        if k not in DEFAULTS:
            continue  # ignore unknown keys
        if isinstance(v, (list, dict)):
            mapping[k] = json.dumps(v)
        elif isinstance(v, bool):
            mapping[k] = "true" if v else "false"
        else:
            mapping[k] = str(v)
    if mapping:
        await redis.hset(HASH, mapping=mapping)


def should_drop(title: str | None, body: str | None, cfg: dict[str, Any]) -> str | None:
    """Return a drop-reason string, or None if the signal passes.

    Applied AFTER dedup, BEFORE publish, in the scraper runner.
    """
    title = (title or "").strip()
    body = (body or "").strip()
    if len(title) < int(cfg.get("min_title_len") or 0):
        return f"title_too_short<{cfg.get('min_title_len')}"
    hay = f"{title}\n{body}".lower()
    block = [s.strip().lower() for s in (cfg.get("block_keywords") or []) if s and s.strip()]
    for term in block:
        if term in hay:
            return f"blocked:{term}"
    require = [s.strip().lower() for s in (cfg.get("require_keywords") or []) if s and s.strip()]
    if require:
        if not any(term in hay for term in require):
            return "no_required_keyword"
    return None
