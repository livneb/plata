"""Entity canonical-id resolver.

Different signals (and the LLM) refer to the same real-world entity with different
strings: USA / US / United States / America → "United States". This module collapses
common aliases to a single human-readable canonical name per entity type, so the graph
stops creating duplicate nodes. ISO codes become aliases of the canonical full name.

The function is called both at write time (in `graph_ingestion`) AND by the maintenance
endpoint that merges historical duplicates (`POST /graph/normalize_aliases`).
"""
from __future__ import annotations


# Canonical human-readable country name keyed by every known alias.
# The right-hand side is what the entity node will be called; ISO-2 / ISO-3 / informal
# names all collapse into it.
COUNTRY_ALIASES: dict[str, str] = {
    # United States
    "US": "United States", "USA": "United States", "UNITED_STATES": "United States",
    "UNITED STATES": "United States", "AMERICA": "United States",
    "U.S.": "United States", "U.S.A.": "United States",
    # Israel
    "IL": "Israel", "ISR": "Israel", "ISRAEL": "Israel",
    # Iran
    "IR": "Iran", "IRN": "Iran", "IRAN": "Iran",
    # Russia
    "RU": "Russia", "RUS": "Russia", "RUSSIA": "Russia",
    "RUSSIAN_FEDERATION": "Russia", "RUSSIAN FEDERATION": "Russia",
    # China
    "CN": "China", "CHN": "China", "CHINA": "China", "PRC": "China",
    # Germany
    "DE": "Germany", "DEU": "Germany", "GERMANY": "Germany",
    # United Kingdom
    "UK": "United Kingdom", "GB": "United Kingdom", "GBR": "United Kingdom",
    "BRITAIN": "United Kingdom", "UNITED_KINGDOM": "United Kingdom",
    "UNITED KINGDOM": "United Kingdom", "ENGLAND": "United Kingdom",
    # European Union
    "EU": "European Union", "EUR": "European Union",
    "EUROPEAN_UNION": "European Union", "EUROPEAN UNION": "European Union",
    # India
    "IN": "India", "IND": "India", "INDIA": "India",
    # Japan
    "JP": "Japan", "JPN": "Japan", "JAPAN": "Japan",
    # South Korea
    "KR": "South Korea", "KOR": "South Korea",
    "SOUTH_KOREA": "South Korea", "SOUTH KOREA": "South Korea", "KOREA": "South Korea",
    # France
    "FR": "France", "FRA": "France", "FRANCE": "France",
    # Spain
    "ES": "Spain", "ESP": "Spain", "SPAIN": "Spain",
    # Italy
    "IT": "Italy", "ITA": "Italy", "ITALY": "Italy",
    # Brazil
    "BR": "Brazil", "BRA": "Brazil", "BRAZIL": "Brazil",
    # Canada
    "CA": "Canada", "CAN": "Canada", "CANADA": "Canada",
    # Australia
    "AU": "Australia", "AUS": "Australia", "AUSTRALIA": "Australia",
    # Mexico
    "MX": "Mexico", "MEX": "Mexico", "MEXICO": "Mexico",
    # Saudi Arabia
    "SA": "Saudi Arabia", "SAU": "Saudi Arabia",
    "SAUDI_ARABIA": "Saudi Arabia", "SAUDI ARABIA": "Saudi Arabia",
    # UAE
    "AE": "United Arab Emirates", "ARE": "United Arab Emirates", "UAE": "United Arab Emirates",
    # Qatar
    "QA": "Qatar", "QAT": "Qatar", "QATAR": "Qatar",
    # Turkey
    "TR": "Turkey", "TUR": "Turkey", "TURKEY": "Turkey", "TÜRKIYE": "Turkey",
    # Ukraine
    "UA": "Ukraine", "UKR": "Ukraine", "UKRAINE": "Ukraine",
    # Nigeria
    "NG": "Nigeria", "NGA": "Nigeria", "NIGERIA": "Nigeria",
    # Indonesia
    "ID": "Indonesia", "IDN": "Indonesia", "INDONESIA": "Indonesia",
    # Pakistan
    "PK": "Pakistan", "PAK": "Pakistan", "PAKISTAN": "Pakistan",
    # Egypt
    "EG": "Egypt", "EGY": "Egypt", "EGYPT": "Egypt",
    # South Africa
    "ZA": "South Africa", "ZAF": "South Africa",
    "SOUTH_AFRICA": "South Africa", "SOUTH AFRICA": "South Africa",
    # Afghanistan
    "AF": "Afghanistan", "AFG": "Afghanistan", "AFGHANISTAN": "Afghanistan",
    # Lebanon
    "LB": "Lebanon", "LBN": "Lebanon", "LEBANON": "Lebanon",
    # Syria
    "SY": "Syria", "SYR": "Syria", "SYRIA": "Syria",
    # Iraq
    "IQ": "Iraq", "IRQ": "Iraq", "IRAQ": "Iraq",
    # Singapore
    "SG": "Singapore", "SGP": "Singapore", "SINGAPORE": "Singapore",
    # Hong Kong
    "HK": "Hong Kong", "HKG": "Hong Kong",
    "HONG_KONG": "Hong Kong", "HONG KONG": "Hong Kong",
    # Taiwan
    "TW": "Taiwan", "TWN": "Taiwan", "TAIWAN": "Taiwan",
    # North Korea
    "KP": "North Korea", "PRK": "North Korea",
    "NORTH_KOREA": "North Korea", "NORTH KOREA": "North Korea",
    # Vietnam
    "VN": "Vietnam", "VNM": "Vietnam", "VIETNAM": "Vietnam", "VIET_NAM": "Vietnam",
    # Thailand
    "TH": "Thailand", "THA": "Thailand", "THAILAND": "Thailand",
}


def _norm_key(s: str) -> str:
    return (s or "").strip().upper().replace("-", "_").replace("  ", " ")


def canonical_country(name_or_id: str) -> str:
    """Return canonical human-readable country name for any alias. Unknown aliases pass
    through unchanged so a typo doesn't get silently absorbed into the wrong entity."""
    if not name_or_id:
        return name_or_id
    key = _norm_key(name_or_id)
    if key in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key]
    key_u = key.replace(" ", "_")
    if key_u in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key_u]
    return name_or_id


def canonicalize_entity(type_: str, id_: str, name: str) -> tuple[str, str]:
    """Return (canonical_id, canonical_name) for an entity.

    - Country: collapse ISO codes + informal names → full English name.
    - Other types: pass through unchanged for now (extend per type as needed).
    """
    t = (type_ or "").lower()
    if t == "country":
        # Try both id and name; whichever resolves wins. id_ usually has the LLM's choice.
        canon = canonical_country(id_)
        if canon == id_:
            canon = canonical_country(name)
        if canon:
            return (canon, canon)
        return (id_, name)
    return (id_, name)
