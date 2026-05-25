"""Entity canonical-id resolver.

Different signals (and the LLM) refer to the same real-world entity with different
strings: USA / US / United States / America. This module collapses common aliases to a
single canonical id per entity type, so the graph stops creating duplicate nodes.

The function is called both at write time (in `graph_ingestion`) AND by the maintenance
endpoint that merges historical duplicates.
"""
from __future__ import annotations


# ISO-3 (the canonical form we want everywhere) keyed by every known alias.
COUNTRY_ALIASES: dict[str, str] = {
    # United States
    "US": "USA", "USA": "USA", "UNITED_STATES": "USA", "UNITED STATES": "USA",
    "AMERICA": "USA", "U.S.": "USA", "U.S.A.": "USA",
    # Israel
    "IL": "ISR", "ISR": "ISR", "ISRAEL": "ISR",
    # Iran
    "IR": "IRN", "IRN": "IRN", "IRAN": "IRN",
    # Russia
    "RU": "RUS", "RUS": "RUS", "RUSSIA": "RUS", "RUSSIAN_FEDERATION": "RUS",
    # China
    "CN": "CHN", "CHN": "CHN", "CHINA": "CHN", "PRC": "CHN",
    # Germany
    "DE": "DEU", "DEU": "DEU", "GERMANY": "DEU",
    # United Kingdom
    "UK": "GBR", "GB": "GBR", "GBR": "GBR", "BRITAIN": "GBR",
    "UNITED_KINGDOM": "GBR", "UNITED KINGDOM": "GBR", "ENGLAND": "GBR",
    # European Union
    "EU": "EUR", "EUR": "EUR", "EUROPEAN_UNION": "EUR", "EUROPEAN UNION": "EUR",
    # Major others
    "IN": "IND", "IND": "IND", "INDIA": "IND",
    "JP": "JPN", "JPN": "JPN", "JAPAN": "JPN",
    "KR": "KOR", "KOR": "KOR", "SOUTH_KOREA": "KOR", "KOREA": "KOR",
    "FR": "FRA", "FRA": "FRA", "FRANCE": "FRA",
    "ES": "ESP", "ESP": "ESP", "SPAIN": "ESP",
    "IT": "ITA", "ITA": "ITA", "ITALY": "ITA",
    "BR": "BRA", "BRA": "BRA", "BRAZIL": "BRA",
    "CA": "CAN", "CAN": "CAN", "CANADA": "CAN",
    "AU": "AUS", "AUS": "AUS", "AUSTRALIA": "AUS",
    "MX": "MEX", "MEX": "MEX", "MEXICO": "MEX",
    "SA": "SAU", "SAU": "SAU", "SAUDI_ARABIA": "SAU", "SAUDI ARABIA": "SAU",
    "AE": "ARE", "ARE": "ARE", "UAE": "ARE",
    "QA": "QAT", "QAT": "QAT", "QATAR": "QAT",
    "TR": "TUR", "TUR": "TUR", "TURKEY": "TUR", "TÜRKIYE": "TUR",
    "UA": "UKR", "UKR": "UKR", "UKRAINE": "UKR",
    "NG": "NGA", "NGA": "NGA", "NIGERIA": "NGA",
    "ID": "IDN", "IDN": "IDN", "INDONESIA": "IDN",
    "PK": "PAK", "PAK": "PAK", "PAKISTAN": "PAK",
    "EG": "EGY", "EGY": "EGY", "EGYPT": "EGY",
    "ZA": "ZAF", "ZAF": "ZAF", "SOUTH_AFRICA": "ZAF", "SOUTH AFRICA": "ZAF",
    "AF": "AFG", "AFG": "AFG", "AFGHANISTAN": "AFG",
    "LB": "LBN", "LBN": "LBN", "LEBANON": "LBN",
    "SY": "SYR", "SYR": "SYR", "SYRIA": "SYR",
    "IQ": "IRQ", "IRQ": "IRQ", "IRAQ": "IRQ",
    "SG": "SGP", "SGP": "SGP", "SINGAPORE": "SGP",
    "HK": "HKG", "HKG": "HKG", "HONG_KONG": "HKG", "HONG KONG": "HKG",
    "TW": "TWN", "TWN": "TWN", "TAIWAN": "TWN",
    "KP": "PRK", "PRK": "PRK", "NORTH_KOREA": "PRK", "NORTH KOREA": "PRK",
    "VN": "VNM", "VNM": "VNM", "VIETNAM": "VNM", "VIET_NAM": "VNM",
    "TH": "THA", "THA": "THA", "THAILAND": "THA",
}


def _norm_key(s: str) -> str:
    return (s or "").strip().upper().replace("-", "_").replace("  ", " ")


def canonical_country(name_or_id: str) -> str:
    """Return ISO-3 canonical id for a country alias. Unknown aliases pass through unchanged
    (so a typo doesn't get silently absorbed into the wrong entity)."""
    if not name_or_id:
        return name_or_id
    key = _norm_key(name_or_id)
    if key in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key]
    # Try replacing space-with-underscore too
    key_u = key.replace(" ", "_")
    if key_u in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key_u]
    return name_or_id


def canonicalize_entity(type_: str, id_: str, name: str) -> tuple[str, str]:
    """Return (canonical_id, canonical_name) for an entity.

    Currently only countries are normalized — extend per type as needed (people by full
    name + dob, tickers by Bybit symbol normalization, etc.).
    """
    t = (type_ or "").lower()
    if t == "country":
        canon = canonical_country(id_) or canonical_country(name)
        return (canon, canon)  # store name == ISO-3 for predictability
    return (id_, name)
