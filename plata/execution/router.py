"""Execution venue router — picks Bybit (crypto) or Alpaca (US equities) for a proposal.

Selection rules (in order):
  1. If `proposal.venue` is explicit and starts with `bybit_` → Bybit.
  2. If `proposal.venue` is explicit and starts with `alpaca_` → Alpaca.
  3. If `proposal.instrument_class == 'stock' | 'etf'` → Alpaca.
  4. If `proposal.symbol` looks like a Bybit perp (XXXUSDT, XXXUSD, XXXBTC) → Bybit.
  5. If `proposal.symbol` is 1-5 uppercase letters (NVDA, AAPL, SPY) → Alpaca.
  6. Default → Bybit (back-compat).
"""
from __future__ import annotations

import re
from typing import Any


_STOCK_RE = re.compile(r"^[A-Z]{1,5}$")
_CRYPTO_RE = re.compile(r"^[A-Z]{2,8}(USDT|USD|USDC|BTC|ETH|EUR|JPY)$")


def venue_for(symbol: str | None, *, hint_venue: str | None = None,
              hint_class: str | None = None) -> str:
    """Return one of: 'bybit', 'alpaca'."""
    if hint_venue:
        v = hint_venue.lower()
        if v.startswith("alpaca"): return "alpaca"
        if v.startswith("bybit"):  return "bybit"
    if hint_class and hint_class.lower() in ("stock", "etf", "equity"):
        return "alpaca"
    s = (symbol or "").upper()
    if _CRYPTO_RE.match(s):
        return "bybit"
    if _STOCK_RE.match(s):
        return "alpaca"
    return "bybit"


def client_for(*, agent: str, venue: str) -> Any:
    if venue == "alpaca":
        from plata.execution.alpaca_client import AlpacaClient
        return AlpacaClient(agent=agent)
    from plata.execution.bybit_client import BybitClient
    return BybitClient(agent=agent)
