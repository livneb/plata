"""Async ccxt wrapper for Bybit. Always testnet in MVP."""
from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import Any

import ccxt.async_support as ccxt_async

from plata.config.secrets import ScopedSecrets
from plata.config.settings import get_settings
from plata.core.observability import get_logger

_log = get_logger("bybit")


@lru_cache
def _build_client(agent: str) -> ccxt_async.bybit:
    """Build a Bybit ccxt client, secret-scoped to the calling agent."""
    settings = get_settings()
    # Prefer UI-managed credentials (encrypted in Postgres); fall back to env-scoped secrets.
    from plata.config import credentials as _creds
    api_key = _creds.get_sync("bybit_key")
    api_secret = _creds.get_sync("bybit_secret")
    if not api_key or not api_secret:
        secrets = ScopedSecrets(agent=agent, _settings=settings)
        api_key = api_key or secrets.reveal("bybit_api_key")
        api_secret = api_secret or secrets.reveal("bybit_api_secret")
    client = ccxt_async.bybit({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    if settings.bybit_testnet:
        client.set_sandbox_mode(True)
    return client


class BybitClient:
    """Thin wrapper exposing only the operations we use."""

    def __init__(self, *, agent: str):
        self._agent = agent
        self._client = _build_client(agent)

    async def close(self) -> None:
        await self._client.close()

    async def fetch_balance(self) -> dict[str, Any]:
        return await self._client.fetch_balance()

    async def fetch_positions(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        return await self._client.fetch_positions(symbols)

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return await self._client.fetch_ticker(symbol)

    async def fetch_ohlcv(
        self, symbol: str, *, timeframe: str = "1m", since: int | None = None, limit: int = 200
    ) -> list[list[float]]:
        return await self._client.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)

    async def create_market_order(
        self,
        *,
        symbol: str,
        side: str,  # "buy" | "sell"
        qty: Decimal,
        sl_price: Decimal | None = None,
        tp_price: Decimal | None = None,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"reduceOnly": reduce_only}
        if sl_price is not None:
            params["stopLoss"] = str(sl_price)
        if tp_price is not None:
            params["takeProfit"] = str(tp_price)
        order = await self._client.create_order(
            symbol=symbol,
            type="market",
            side=side,
            amount=float(qty),
            params=params,
        )
        return order

    async def cancel_all(self, symbol: str | None = None) -> Any:
        return await self._client.cancel_all_orders(symbol)
