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
    # GDELT 429s on Railway's shared egress IP (other tenants exhaust the
    # 1-req-per-5s budget). Disabled by default — RSS is the primary feed now.
    # Re-enable on /news/ if you're hosting from an IP that GDELT will accept.
    "gdelt_enabled": False,
    "reddit_enabled": True,
    "cryptopanic_enabled": False,
    "rss_enabled": True,
    "telegram_channels_enabled": False,
    # Market-ticker source: poll live prices for top crypto + stocks; emit a
    # PRICE_ACTION signal when a tracked symbol moves > threshold % in window.
    # Gives the strategist momentum/breakout signals independent of news.
    "market_ticker_enabled": True,
    "market_ticker_threshold_pct": 3.0,
    "market_ticker_window_min": 60,
    "market_ticker_crypto_ids": [
        "bitcoin", "ethereum", "solana", "binancecoin", "ripple",
        "cardano", "dogecoin", "tron", "avalanche-2", "polkadot",
    ],
    "market_ticker_stock_symbols": ["SPY", "QQQ", "NVDA", "TSLA", "AAPL", "MSFT"],
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
    # Default RSS feeds — public, no-auth, generally reliable. Users can edit
    # this list on /news/. Picked for: high volume of finance/macro headlines,
    # don't 401 / require partner agreements, return well-formed RSS.
    "rss_feeds": [
        {"name": "CoinDesk",          "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",   "enabled": True},
        {"name": "Cointelegraph",     "url": "https://cointelegraph.com/rss",                      "enabled": True},
        {"name": "Decrypt",           "url": "https://decrypt.co/feed",                            "enabled": True},
        {"name": "The Block",         "url": "https://www.theblock.co/rss.xml",                    "enabled": True},
        {"name": "Yahoo Finance",     "url": "https://finance.yahoo.com/news/rssindex",            "enabled": True},
        {"name": "CNBC Top News",     "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "enabled": True},
        {"name": "MarketWatch Top",   "url": "https://feeds.marketwatch.com/marketwatch/topstories/", "enabled": True},
        {"name": "Reuters Business",  "url": "https://www.reuters.com/business/rss",               "enabled": True},
        {"name": "Bloomberg Markets", "url": "https://feeds.bloomberg.com/markets/news.rss",       "enabled": True},
        {"name": "Investing.com",     "url": "https://www.investing.com/rss/news.rss",             "enabled": True},
    ],
    # --- Content filter (applied to every signal before publish) ---
    # Drop if title length < this (junk one-word headlines). Was 20 which
    # killed a lot of legit Yahoo Finance / MarketWatch headlines (often 10-19
    # chars). Lowered to 10 — anything shorter is genuinely useless.
    "min_title_len": 10,
    # If false, the require_keywords allowlist is treated as advisory and signals
    # that don't match it still pass through (blocklist alone is enforced). Set
    # true to make the allowlist a hard gate. Default false so the strategist
    # doesn't starve from a too-narrow allowlist.
    "require_keywords_enforce": False,
    # If non-empty AND require_keywords_enforce=true: signal MUST match at least
    # one of these (case-insensitive substring). Otherwise this list is unused.
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
    if require and bool(cfg.get("require_keywords_enforce")):
        if not any(term in hay for term in require):
            return "no_required_keyword"
    return None


async def record_poll_probe(source: str, **fields) -> None:
    """Persist a small `last_probe` record per source so the dashboard can
    show actual HTTP-level evidence (status, final URL, items, errors)
    instead of guessing why a source returned 0.

    Fields accepted (all optional):
        http_status: int / str
        url: str (the URL we hit)
        response_size: int (bytes)
        sample: str (first ~300 chars of the body)
        item_count: int (items returned by the API before our dedup/filter)
        error_type: str (exception class name when the request failed)
        error_message: str (exception message tail)
        ts: ISO string (defaults to now)
    """
    from datetime import datetime, timezone
    redis = get_redis()
    key = f"scraper:source:{source}:probe"
    payload = {k: ("" if v is None else str(v)) for k, v in fields.items()}
    payload.setdefault("ts", datetime.now(timezone.utc).isoformat())
    # Drop empty values so a successful run doesn't carry forward old error text.
    payload = {k: v for k, v in payload.items() if v != ""}
    if not payload:
        return
    # Replace the probe wholesale on each poll (small hash, ~6 fields).
    pipe = redis.pipeline()
    pipe.delete(key)
    pipe.hset(key, mapping=payload)
    pipe.expire(key, 60 * 60 * 24 * 7)
    await pipe.execute()
