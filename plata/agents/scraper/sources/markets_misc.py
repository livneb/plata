"""Misc no-auth market sources that are small enough to share a module:

  - StockTwits trending tickers (sentiment + watch-counts)
  - Polymarket prediction-market odds (top binary markets, swing > threshold)
  - Fear & Greed (alternative.me) — daily crypto regime gauge
  - DeFiLlama TVL flow changes — protocol-level capital movement

Each is a separate `BaseSource` subclass; one file keeps the source-loader
in `runner.py` from sprawling. Each polls on its own interval and is
toggle-able from /news/.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

from plata.agents.scraper.news_config import (
    get_config as get_news_config,
    record_poll_probe,
)
from plata.agents.scraper.sources.base_source import BaseSource
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.markets_misc")


# ============================================================
# StockTwits trending — what retail-equity flow is talking about
# ============================================================
class StockTwitsSource(BaseSource):
    """https://api.stocktwits.com/api/2/trending/symbols.json — free, no auth.
    Returns top 30 trending tickers + last 24h watch_count. We emit one
    signal per ticker that's newly trending vs the last poll."""
    name = "stocktwits"
    poll_interval_sec = 15 * 60
    _URL = "https://api.stocktwits.com/api/2/trending/symbols.json?limit=30"

    def __init__(self) -> None:
        self._prev_symbols: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("stocktwits_enabled", True):
            await record_poll_probe("stocktwits", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            try:
                r = await client.get(self._URL,
                                      headers={"User-Agent": "plata-research-bot"})
                if r.status_code >= 400:
                    await record_poll_probe("stocktwits",
                                             http_status=r.status_code,
                                             error_type=f"HTTP{r.status_code}")
                    return []
                data = r.json()
            except Exception as exc:  # noqa: BLE001
                await record_poll_probe("stocktwits",
                                         error_type=type(exc).__name__,
                                         error_message=str(exc)[:200])
                return []
        symbols = data.get("symbols") or []
        now_symbols = {s.get("symbol") for s in symbols if s.get("symbol")}
        newly_hot = now_symbols - self._prev_symbols
        signals: list[RawSignal] = []
        for s in symbols:
            sym = s.get("symbol")
            if sym not in newly_hot:
                continue
            title = (f"{sym} trending on StockTwits — "
                       f"{int(s.get('watchlist_count') or 0)} watchers")
            signals.append(RawSignal(
                source=SignalSource.STOCKTWITS,
                url=f"https://stocktwits.com/symbol/{sym}",
                title=title,
                body=f"{s.get('title') or sym} ({s.get('exchange') or ''}). "
                     f"Trending now on StockTwits.",
                source_published_at=datetime.now(timezone.utc),
                metadata={
                    "symbol": sym,
                    "watchlist_count": s.get("watchlist_count"),
                    "exchange": s.get("exchange"),
                },
            ))
        self._prev_symbols = now_symbols
        await record_poll_probe("stocktwits", item_count=len(signals),
                                  http_status=200)
        return signals


# ============================================================
# Polymarket — prediction-market odds swings
# ============================================================
class PolymarketSource(BaseSource):
    """https://gamma-api.polymarket.com/events?closed=false&order=volume&limit=20
    Returns top-volume binary prediction markets. We emit a signal when a
    market's YES probability moves more than `polymarket_swing_threshold_pct`
    (default 8%) between polls — that's the prediction crowd repricing
    something material."""
    name = "polymarket"
    poll_interval_sec = 15 * 60
    _URL = "https://gamma-api.polymarket.com/events?closed=false&order=volume&ascending=false&limit=20"

    def __init__(self) -> None:
        self._last_probs: dict[str, float] = {}

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("polymarket_enabled", True):
            await record_poll_probe("polymarket", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        threshold = float(cfg.get("polymarket_swing_threshold_pct") or 8.0) / 100.0
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                r = await client.get(self._URL,
                                      headers={"User-Agent": "plata-research-bot"})
                if r.status_code >= 400:
                    await record_poll_probe("polymarket",
                                             http_status=r.status_code,
                                             error_type=f"HTTP{r.status_code}")
                    return []
                events = r.json() or []
            except Exception as exc:  # noqa: BLE001
                await record_poll_probe("polymarket",
                                         error_type=type(exc).__name__,
                                         error_message=str(exc)[:200])
                return []
        signals: list[RawSignal] = []
        new_probs: dict[str, float] = {}
        for ev in events:
            for mkt in (ev.get("markets") or []):
                mid = str(mkt.get("id") or "")
                if not mid:
                    continue
                # outcomePrices is a stringified JSON list like '["0.62","0.38"]'.
                raw = mkt.get("outcomePrices")
                yes_prob = None
                if isinstance(raw, list) and raw:
                    try: yes_prob = float(raw[0])
                    except (TypeError, ValueError): pass
                elif isinstance(raw, str):
                    try:
                        import json as _json
                        arr = _json.loads(raw)
                        yes_prob = float(arr[0]) if arr else None
                    except Exception:  # noqa: BLE001
                        pass
                if yes_prob is None:
                    continue
                new_probs[mid] = yes_prob
                prev = self._last_probs.get(mid)
                if prev is None:
                    continue
                delta = yes_prob - prev
                if abs(delta) < threshold:
                    continue
                question = mkt.get("question") or ev.get("title") or "Polymarket"
                direction = "↑" if delta > 0 else "↓"
                signals.append(RawSignal(
                    source=SignalSource.POLYMARKET,
                    url=f"https://polymarket.com/event/{ev.get('slug') or ''}",
                    title=f"Polymarket {direction} {abs(delta)*100:.1f}pp: {question}",
                    body=(f"Polymarket implied probability moved from "
                           f"{prev*100:.1f}% to {yes_prob*100:.1f}% on: "
                           f"{question}"),
                    source_published_at=datetime.now(timezone.utc),
                    metadata={
                        "market_id": mid,
                        "yes_prob": yes_prob,
                        "prev_yes_prob": prev,
                        "delta": delta,
                        "volume_24h": mkt.get("volume24hr"),
                    },
                ))
        self._last_probs = new_probs
        await record_poll_probe("polymarket", item_count=len(signals),
                                  http_status=200)
        return signals


# ============================================================
# Crypto Fear & Greed — one regime tag per day
# ============================================================
class FearGreedSource(BaseSource):
    """https://api.alternative.me/fng/?limit=1 — daily 0-100 sentiment index.
    We emit a signal only when the bucket (Extreme Fear / Fear / Neutral /
    Greed / Extreme Greed) CHANGES vs the last poll. Daily-resolution data
    so we poll once an hour, no spam."""
    name = "fear_greed"
    poll_interval_sec = 60 * 60
    _URL = "https://api.alternative.me/fng/?limit=1"

    def __init__(self) -> None:
        self._last_bucket: str | None = None

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("fear_greed_enabled", True):
            await record_poll_probe("fear_greed", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                r = await client.get(self._URL,
                                      headers={"User-Agent": "plata-research-bot"})
                if r.status_code >= 400:
                    await record_poll_probe("fear_greed",
                                             http_status=r.status_code,
                                             error_type=f"HTTP{r.status_code}")
                    return []
                data = r.json()
            except Exception as exc:  # noqa: BLE001
                await record_poll_probe("fear_greed",
                                         error_type=type(exc).__name__,
                                         error_message=str(exc)[:200])
                return []
        items = data.get("data") or []
        if not items:
            await record_poll_probe("fear_greed", item_count=0)
            return []
        latest = items[0]
        try:
            value = int(latest.get("value") or 0)
        except (TypeError, ValueError):
            value = 0
        bucket = (latest.get("value_classification") or "").strip()
        signals: list[RawSignal] = []
        if bucket and bucket != self._last_bucket:
            prev = self._last_bucket or "?"
            signals.append(RawSignal(
                source=SignalSource.FEAR_GREED,
                url="https://alternative.me/crypto/fear-and-greed-index/",
                title=f"Crypto regime: {prev} → {bucket} ({value}/100)",
                body=(f"The Crypto Fear & Greed Index moved from '{prev}' to "
                       f"'{bucket}' (value {value}/100). This is a daily "
                       f"composite of volatility, momentum, social media, "
                       f"dominance and surveys."),
                source_published_at=datetime.now(timezone.utc),
                metadata={"value": value, "bucket": bucket,
                          "prev_bucket": prev},
            ))
        self._last_bucket = bucket
        await record_poll_probe("fear_greed", item_count=len(signals),
                                  http_status=200)
        return signals


# ============================================================
# DeFiLlama — protocol TVL flow changes
# ============================================================
class DefiLlamaSource(BaseSource):
    """https://api.llama.fi/protocols — current TVL + 1d/7d % changes per
    protocol. We emit a signal when a top-50 protocol's 1d change exceeds
    `defillama_change_threshold_pct` (default 8%). Big outflows precede
    related-token price moves; big inflows are sometimes a buy signal."""
    name = "defillama"
    poll_interval_sec = 30 * 60
    _URL = "https://api.llama.fi/protocols"

    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("defillama_enabled", True):
            await record_poll_probe("defillama", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        threshold = float(cfg.get("defillama_change_threshold_pct") or 8.0)
        top_n = int(cfg.get("defillama_top_n") or 50)
        async with httpx.AsyncClient(timeout=20.0) as client:
            try:
                r = await client.get(self._URL,
                                      headers={"User-Agent": "plata-research-bot"})
                if r.status_code >= 400:
                    await record_poll_probe("defillama",
                                             http_status=r.status_code,
                                             error_type=f"HTTP{r.status_code}")
                    return []
                protocols = r.json() or []
            except Exception as exc:  # noqa: BLE001
                await record_poll_probe("defillama",
                                         error_type=type(exc).__name__,
                                         error_message=str(exc)[:200])
                return []
        protocols.sort(key=lambda p: float(p.get("tvl") or 0), reverse=True)
        top = protocols[:top_n]
        signals: list[RawSignal] = []
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for p in top:
            change_1d = p.get("change_1d")
            try:
                pct = float(change_1d) if change_1d is not None else 0.0
            except (TypeError, ValueError):
                pct = 0.0
            if abs(pct) < threshold:
                continue
            # Dedup per (protocol, day) so we don't refire the same big move
            # every poll cycle.
            slug = p.get("slug") or p.get("name") or "?"
            dedup_key = f"{slug}:{now_iso}"
            if dedup_key in self._seen:
                continue
            self._seen.add(dedup_key)
            direction = "↑" if pct > 0 else "↓"
            tvl = float(p.get("tvl") or 0)
            signals.append(RawSignal(
                source=SignalSource.DEFILLAMA,
                url=f"https://defillama.com/protocol/{slug}",
                title=f"{p.get('name')} TVL {direction} {abs(pct):.1f}% (24h)",
                body=(f"{p.get('name')} ({p.get('category') or 'protocol'}): "
                       f"TVL {direction} {abs(pct):.1f}% in 24h. "
                       f"Current TVL ${tvl:,.0f}. "
                       f"Tokens: {','.join(p.get('symbol', '').split(',')[:3])}."),
                source_published_at=datetime.now(timezone.utc),
                metadata={
                    "slug": slug,
                    "tvl_usd": tvl,
                    "change_1d_pct": pct,
                    "category": p.get("category"),
                    "symbol": p.get("symbol"),
                },
            ))
        if len(self._seen) > 2000:
            self._seen = set(list(self._seen)[-1000:])
        await record_poll_probe("defillama", item_count=len(signals),
                                  http_status=200)
        return signals
