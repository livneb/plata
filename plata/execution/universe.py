"""Trading universe: symbol → venue/instrument/sector metadata.

Used by Risk Manager (sector caps), Strategist (legal symbol set),
and Executor (min qty / tick size).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True, frozen=True)
class Symbol:
    name: str
    venue: str = "bybit_testnet"
    instrument_type: str = "perp"  # perp | spot
    sector: str = "other"
    min_qty: Decimal = Decimal("0.001")
    tick_size: Decimal = Decimal("0.01")


UNIVERSE: dict[str, Symbol] = {
    # Crypto L1
    "BTCUSDT":  Symbol("BTCUSDT",  sector="crypto_l1",     min_qty=Decimal("0.001"),  tick_size=Decimal("0.1")),
    "ETHUSDT":  Symbol("ETHUSDT",  sector="crypto_l1",     min_qty=Decimal("0.01"),   tick_size=Decimal("0.05")),
    "SOLUSDT":  Symbol("SOLUSDT",  sector="crypto_l1",     min_qty=Decimal("0.1"),    tick_size=Decimal("0.01")),
    # Crypto majors
    "BNBUSDT":  Symbol("BNBUSDT",  sector="crypto_major",  min_qty=Decimal("0.01"),   tick_size=Decimal("0.01")),
    "XRPUSDT":  Symbol("XRPUSDT",  sector="crypto_major",  min_qty=Decimal("1"),      tick_size=Decimal("0.0001")),
    "ADAUSDT":  Symbol("ADAUSDT",  sector="crypto_major",  min_qty=Decimal("1"),      tick_size=Decimal("0.0001")),
    "DOGEUSDT": Symbol("DOGEUSDT", sector="crypto_meme",   min_qty=Decimal("1"),      tick_size=Decimal("0.00001")),
    # Commodities (Bybit-supported perps)
    "XAUUSDT":  Symbol("XAUUSDT",  sector="commodity",     min_qty=Decimal("0.001"),  tick_size=Decimal("0.01")),
    "XAGUSDT":  Symbol("XAGUSDT",  sector="commodity",     min_qty=Decimal("0.01"),   tick_size=Decimal("0.001")),
    # FX
    "EURUSDT":  Symbol("EURUSDT",  sector="fx",            min_qty=Decimal("1"),      tick_size=Decimal("0.00001")),
    "GBPUSDT":  Symbol("GBPUSDT",  sector="fx",            min_qty=Decimal("1"),      tick_size=Decimal("0.00001")),
    # US Equities (Alpaca, fractional shares supported)
    "AAPL":     Symbol("AAPL",     venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "MSFT":     Symbol("MSFT",     venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "NVDA":     Symbol("NVDA",     venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "GOOGL":    Symbol("GOOGL",    venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "META":     Symbol("META",     venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "AMZN":     Symbol("AMZN",     venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "TSLA":     Symbol("TSLA",     venue="alpaca_paper", instrument_type="stock", sector="us_megacap", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "AMD":      Symbol("AMD",      venue="alpaca_paper", instrument_type="stock", sector="us_semis",   min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "AVGO":     Symbol("AVGO",     venue="alpaca_paper", instrument_type="stock", sector="us_semis",   min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "COIN":     Symbol("COIN",     venue="alpaca_paper", instrument_type="stock", sector="us_crypto_adj", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "MSTR":     Symbol("MSTR",     venue="alpaca_paper", instrument_type="stock", sector="us_crypto_adj", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    # US ETFs
    "SPY":      Symbol("SPY",      venue="alpaca_paper", instrument_type="etf",   sector="us_index",   min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "QQQ":      Symbol("QQQ",      venue="alpaca_paper", instrument_type="etf",   sector="us_index",   min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "IWM":      Symbol("IWM",      venue="alpaca_paper", instrument_type="etf",   sector="us_index",   min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "GLD":      Symbol("GLD",      venue="alpaca_paper", instrument_type="etf",   sector="us_commodity", min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
    "TLT":      Symbol("TLT",      venue="alpaca_paper", instrument_type="etf",   sector="us_bonds",   min_qty=Decimal("0.01"), tick_size=Decimal("0.01")),
}


def get_symbol(name: str) -> Symbol | None:
    return UNIVERSE.get(name.upper())


def symbols_in_sector(sector: str) -> list[str]:
    return [s.name for s in UNIVERSE.values() if s.sector == sector]
