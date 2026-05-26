"""Alpaca (US equities) execution adapter.

Mirrors the small subset of `BybitClient` that the rest of the system uses
(`fetch_balance`, `fetch_positions`, `fetch_ticker`, `fetch_ohlcv`, `create_market_order`)
so the executor / risk_manager / trade_sampler can stay venue-agnostic.

We talk to Alpaca directly via httpx (no extra dep — `alpaca-py` is heavyweight and we
already have httpx). Paper account is the default; flip ALPACA_PAPER=false for live.

Docs: https://docs.alpaca.markets/reference/
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from plata.config.settings import get_settings
from plata.core.observability import get_logger

_log = get_logger("alpaca")

PAPER_BASE = "https://paper-api.alpaca.markets"
LIVE_BASE = "https://api.alpaca.markets"
DATA_BASE = "https://data.alpaca.markets"


class AlpacaClient:
    """Thin async wrapper around the Alpaca REST API."""

    def __init__(self, *, agent: str = "executor") -> None:
        self._agent = agent
        s = get_settings()
        from plata.config import credentials as _creds
        self._key = _creds.get_sync("alpaca_key") or (
            s.alpaca_api_key.get_secret_value() if s.alpaca_api_key else None
        )
        self._secret = _creds.get_sync("alpaca_secret") or (
            s.alpaca_api_secret.get_secret_value() if s.alpaca_api_secret else None
        )
        self._paper = bool(getattr(s, "alpaca_paper", True))
        self._base = PAPER_BASE if self._paper else LIVE_BASE
        self._headers = {
            "APCA-API-KEY-ID": self._key or "",
            "APCA-API-SECRET-KEY": self._secret or "",
            "Accept": "application/json",
        }

    def configured(self) -> bool:
        return bool(self._key and self._secret)

    @property
    def venue(self) -> str:
        return "alpaca_paper" if self._paper else "alpaca_live"

    async def _get(self, base: str, path: str, **params: Any) -> Any:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(base + path, headers=self._headers,
                            params={k: v for k, v in params.items() if v is not None})
            r.raise_for_status()
            return r.json()

    async def _post(self, base: str, path: str, payload: dict[str, Any]) -> Any:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(base + path, headers=self._headers, json=payload)
            r.raise_for_status()
            return r.json()

    async def fetch_balance(self) -> dict[str, Any]:
        """Returns {'total': {'USD': <equity>}, 'free': ..., 'used': ...}
        in a shape similar to ccxt for drop-in use."""
        doc = await self._get(self._base, "/v2/account")
        equity = float(doc.get("equity") or 0)
        cash = float(doc.get("cash") or 0)
        return {
            "total": {"USD": equity},
            "free":  {"USD": cash},
            "used":  {"USD": max(0.0, equity - cash)},
            "raw": doc,
        }

    async def fetch_positions(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        try:
            rows = await self._get(self._base, "/v2/positions")
        except Exception:  # noqa: BLE001
            return []
        out = []
        for r in rows:
            sym = r.get("symbol")
            if symbols and sym not in symbols:
                continue
            side = "long" if float(r.get("qty") or 0) >= 0 else "short"
            out.append({
                "symbol": sym,
                "side": side,
                "contracts": float(r.get("qty") or 0),
                "entryPrice": float(r.get("avg_entry_price") or 0),
                "markPrice":  float(r.get("current_price") or 0),
                "unrealizedPnl": float(r.get("unrealized_pl") or 0),
                "raw": r,
            })
        return out

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        """Latest trade price (1-min bar close)."""
        doc = await self._get(
            DATA_BASE, f"/v2/stocks/{symbol}/trades/latest",
        )
        trade = doc.get("trade") or {}
        price = float(trade.get("p") or 0)
        return {"symbol": symbol, "last": price, "close": price, "raw": doc}

    async def fetch_ohlcv(
        self, symbol: str, *, timeframe: str = "1Min", limit: int = 100,
    ) -> list[list[float]]:
        """Returns [[unix_ms, open, high, low, close, volume], ...]."""
        doc = await self._get(
            DATA_BASE, f"/v2/stocks/{symbol}/bars",
            timeframe=timeframe, limit=limit, feed="iex",
        )
        bars = doc.get("bars") or []
        out: list[list[float]] = []
        from datetime import datetime as _dt
        for b in bars:
            try:
                ts = int(_dt.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:  # noqa: BLE001
                ts = 0
            out.append([ts, float(b.get("o", 0)), float(b.get("h", 0)),
                        float(b.get("l", 0)), float(b.get("c", 0)), float(b.get("v", 0))])
        return out

    async def create_market_order(
        self, *, symbol: str, side: str, qty: Decimal,
        sl_price: Decimal | None = None, tp_price: Decimal | None = None,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,  # buy | sell
            "type": "market",
            "time_in_force": "day",
        }
        # Alpaca supports OCO/bracket with one shot; SL+TP go in `order_class=bracket`.
        if sl_price is not None or tp_price is not None:
            payload["order_class"] = "bracket"
            if tp_price is not None:
                payload["take_profit"] = {"limit_price": str(tp_price)}
            if sl_price is not None:
                payload["stop_loss"] = {"stop_price": str(sl_price)}
        return await self._post(self._base, "/v2/orders", payload)

    async def cancel_all(self, symbol: str | None = None) -> Any:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.delete(self._base + "/v2/orders", headers=self._headers,
                               params={"symbol": symbol} if symbol else None)
            r.raise_for_status()
            return r.json() if r.text else {}
