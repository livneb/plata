"""Map graph entities → affected trading symbols. Editable from dashboard later."""
from __future__ import annotations

from inkcliq.core.schemas import EntityRef, EntityType


STATIC_MAP: dict[tuple[str, str], list[str]] = {
    # Countries
    (EntityType.COUNTRY.value, "US"): ["BTCUSDT", "XAUUSDT", "EURUSDT"],
    (EntityType.COUNTRY.value, "CN"): ["BTCUSDT", "XAUUSDT"],
    (EntityType.COUNTRY.value, "RU"): ["BTCUSDT", "XAUUSDT", "EURUSDT"],
    (EntityType.COUNTRY.value, "IL"): ["XAUUSDT"],
    (EntityType.COUNTRY.value, "EU"): ["EURUSDT"],
    (EntityType.COUNTRY.value, "UK"): ["GBPUSDT"],
    # Topics
    (EntityType.TOPIC.value, "crypto_regulation"): ["BTCUSDT", "ETHUSDT"],
    (EntityType.TOPIC.value, "war"): ["XAUUSDT", "BTCUSDT"],
    (EntityType.TOPIC.value, "macro"): ["XAUUSDT", "EURUSDT", "BTCUSDT"],
}


def symbols_for_entities(refs: list[EntityRef]) -> list[str]:
    out: set[str] = set()
    for ref in refs:
        # Tickers map directly when listed in our universe
        if ref.type == EntityType.TICKER:
            candidate = f"{ref.id.upper()}USDT"
            out.add(candidate)
        mapped = STATIC_MAP.get((str(ref.type), ref.id))
        if mapped:
            out.update(mapped)
    return sorted(out) or ["BTCUSDT"]  # default fallback so oracle always runs
