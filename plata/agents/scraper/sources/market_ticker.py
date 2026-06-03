"""Market ticker source — emits PRICE_ACTION signals on threshold moves.

Polls top crypto prices from CoinGecko (no auth required) and, if Alpaca
creds are present, top equity snapshots. For each tracked symbol it
remembers the price `window_min` ago and emits a RawSignal when the
absolute % change crosses the configured threshold (default 3%).

Gives the strategist independent momentum/breakout signals — useful
when the news pipeline is silent or the news doesn't catch a move.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from plata.agents.scraper.news_config import (
    get_config as get_news_config,
    record_poll_probe,
)
from plata.agents.scraper.sources.base_source import BaseSource
from plata.config.settings import get_settings
from plata.core.bus import get_redis
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.market_ticker")

CG_URL = "https://api.coingecko.com/api/v3/simple/price"

# How long to keep historical price points for the rolling-window comparison.
_PRICE_HISTORY_TTL_SEC = 60 * 60 * 24  # 24h


def _hist_key(symbol: str) -> str:
    return f"market_ticker:hist:{symbol.upper()}"


def _cooldown_key(symbol: str) -> str:
    return f"market_ticker:cooldown:{symbol.upper()}"


# Map CoinGecko id → trading symbol on Bybit (what strategist will use).
CG_TO_TRADE_SYMBOL = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "binancecoin": "BNBUSDT",
    "ripple": "XRPUSDT",
    "cardano": "ADAUSDT",
    "dogecoin": "DOGEUSDT",
    "tron": "TRXUSDT",
    "avalanche-2": "AVAXUSDT",
    "polkadot": "DOTUSDT",
}


class MarketTickerSource(BaseSource):
    name = "market_ticker"
    poll_interval_sec = 5 * 60  # poll every 5 min; window default is 60 min

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("market_ticker_enabled", True):
            return []

        threshold_pct = float(cfg.get("market_ticker_threshold_pct") or 3.0)
        window_min = int(cfg.get("market_ticker_window_min") or 60)
        crypto_ids: list[str] = list(cfg.get("market_ticker_crypto_ids") or [])
        stock_symbols: list[str] = list(cfg.get("market_ticker_stock_symbols") or [])

        signals: list[RawSignal] = []
        probe_kwargs: dict = {
            "threshold_pct": threshold_pct,
            "window_min": window_min,
        }
        per_symbol: list[str] = []

        # 1. Crypto via CoinGecko (no auth needed).
        crypto_prices: dict[str, float] = {}
        if crypto_ids:
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    r = await client.get(CG_URL, params={
                        "ids": ",".join(crypto_ids),
                        "vs_currencies": "usd",
                    })
                    probe_kwargs.setdefault("http_status", r.status_code)
                    probe_kwargs.setdefault("response_size", len(r.content or b""))
                    probe_kwargs.setdefault("sample", (r.text or "")[:300])
                    r.raise_for_status()
                    data = r.json() or {}
                for cg_id, payload in data.items():
                    if isinstance(payload, dict) and "usd" in payload:
                        sym = CG_TO_TRADE_SYMBOL.get(cg_id, cg_id.upper())
                        try:
                            crypto_prices[sym] = float(payload["usd"])
                        except (TypeError, ValueError):
                            pass
            except Exception as exc:  # noqa: BLE001
                probe_kwargs["error_type"] = type(exc).__name__
                probe_kwargs["error_message"] = str(exc)[:200]
                _log.exception("market_ticker_cg_failed")

        # 2. Stocks via Alpaca data v2 (free, requires API key).
        stock_prices: dict[str, float] = {}
        s = get_settings()
        if stock_symbols and s.alpaca_api_key and s.alpaca_api_secret:
            try:
                url = "https://data.alpaca.markets/v2/stocks/snapshots"
                headers = {
                    "APCA-API-KEY-ID": s.alpaca_api_key.get_secret_value(),
                    "APCA-API-SECRET-KEY": s.alpaca_api_secret.get_secret_value(),
                }
                async with httpx.AsyncClient(timeout=20) as client:
                    r = await client.get(url, params={"symbols": ",".join(stock_symbols)},
                                          headers=headers)
                    r.raise_for_status()
                    snaps = r.json() or {}
                for sym, payload in snaps.items():
                    if not isinstance(payload, dict):
                        continue
                    px = None
                    for path in (("latestTrade", "p"), ("latestQuote", "ap"),
                                  ("dailyBar", "c")):
                        node: Any = payload
                        for key in path:
                            if isinstance(node, dict) and key in node:
                                node = node[key]
                            else:
                                node = None; break
                        if isinstance(node, (int, float)):
                            px = float(node); break
                    if px is not None and px > 0:
                        stock_prices[sym.upper()] = px
            except Exception as exc:  # noqa: BLE001
                probe_kwargs["alpaca_error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
                _log.exception("market_ticker_alpaca_failed")

        all_prices = {**crypto_prices, **stock_prices}
        probe_kwargs["item_count"] = len(all_prices)

        # 3. For each symbol: record the price, compare to oldest in window,
        # emit RawSignal if |% change| > threshold AND not in cooldown.
        redis = get_redis()
        now = datetime.now(timezone.utc)
        now_ts = int(now.timestamp())
        cutoff_ts = now_ts - window_min * 60
        cooldown_sec = max(window_min * 60 // 2, 15 * 60)

        for sym, price in all_prices.items():
            hk = _hist_key(sym)
            # Push current price as "<ts>:<price>"
            await redis.zadd(hk, {f"{now_ts}:{price}": now_ts})
            await redis.zremrangebyscore(hk, 0, now_ts - _PRICE_HISTORY_TTL_SEC)
            await redis.expire(hk, _PRICE_HISTORY_TTL_SEC + 60)
            # Find the price closest to (but not after) the window cutoff.
            oldest_in_window = await redis.zrangebyscore(hk, cutoff_ts, now_ts, start=0, num=1)
            if not oldest_in_window:
                per_symbol.append(f"{sym}=warming")
                continue
            try:
                _, base_str = oldest_in_window[0].split(":", 1)
                base_price = float(base_str)
            except (ValueError, IndexError):
                continue
            if base_price <= 0:
                continue
            pct = (price - base_price) / base_price * 100.0
            per_symbol.append(f"{sym}={pct:+.2f}%")
            if abs(pct) < threshold_pct:
                continue
            # Cooldown to avoid spamming on a sustained move.
            if await redis.exists(_cooldown_key(sym)):
                continue
            await redis.set(_cooldown_key(sym), "1", ex=cooldown_sec)
            direction = "up" if pct > 0 else "down"
            arrow = "↑" if pct > 0 else "↓"
            title = (f"{sym} {arrow} {abs(pct):.2f}% in {window_min}min "
                     f"(${base_price:,.4g} → ${price:,.4g})")
            body = (
                f"Market-ticker price-action alert for {sym}: "
                f"moved {direction} by {abs(pct):.2f}% over the last "
                f"{window_min} minutes, from ${base_price:,.6g} to ${price:,.6g}. "
                f"Threshold for emitting is {threshold_pct}%. "
                f"This is a deterministic price-action signal, not a news event."
            )
            signals.append(RawSignal(
                source=SignalSource.MARKET_TICKER,
                url=f"market://ticker/{sym}",
                title=title,
                body=body,
                source_published_at=now,
                metadata={
                    "symbol": sym,
                    "base_price": base_price,
                    "current_price": price,
                    "pct_change": round(pct, 4),
                    "window_min": window_min,
                    "threshold_pct": threshold_pct,
                    "direction": direction,
                    "category_hint": "price_action",
                },
            ))

        probe_kwargs["per_symbol"] = ", ".join(per_symbol)[:300]
        await record_poll_probe("market_ticker", **probe_kwargs)
        return signals
