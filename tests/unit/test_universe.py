from plata.execution.universe import UNIVERSE, get_symbol, symbols_in_sector


def test_lookup_basic():
    sym = get_symbol("BTCUSDT")
    assert sym is not None
    assert sym.sector == "crypto_l1"


def test_unknown_symbol_returns_none():
    assert get_symbol("DOESNOTEXIST") is None


def test_symbols_in_sector():
    assert "ETHUSDT" in symbols_in_sector("crypto_l1")
    assert "XAUUSDT" in symbols_in_sector("commodity")


def test_universe_not_empty():
    assert len(UNIVERSE) > 5
