"""City configuration for Otodom search URLs.

Each entry maps a normalized city key (lowercase, hyphens) to the four Otodom
URL slugs plus a canonical display name used to verify that a built URL really
resolves to the expected city.

Adding a new city
-----------------
1. Go to otodom.pl, filter to the desired city, and copy the resulting URL, e.g.
   https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/zywiec/zywiec/zywiec
2. Split the four path segments after `/mieszkanie/`:
   voivodeship / powiat / gmina / city_slug
3. Add an entry below. `display_name` should be the city's canonical Polish
   name (as it appears on the site) so validation can compare against it.
"""

CITIES = {
    "katowice": {
        "voivodeship": "slaskie",
        "powiat": "katowice",
        "gmina": "katowice",
        "city_slug": "katowice",
        "display_name": "Katowice",
    },
    "bielsko-biala": {
        "voivodeship": "slaskie",
        "powiat": "bielsko--biala",
        "gmina": "bielsko--biala",
        "city_slug": "bielsko--biala",
        "display_name": "Bielsko-Biała",
    },
}

_QUERY = "?viewType=listing&by=LATEST&direction=DESC&limit=72"


def build_search_url(
    city_key: str,
    property_type: str = "apartment",
    distance_radius: int | None = None,
) -> str:
    """Build a sale-results URL for apartments or plots.

    distance_radius (km) widens the search around the city — Otodom supports
    0, 5, 10, 15, 25, 50, 75. Use it when a single city has too few listings
    on its own (typical for plots)."""
    if city_key not in CITIES:
        supported = ", ".join(sorted(CITIES.keys()))
        raise ValueError(
            f"unknown city '{city_key}' — supported: {supported}. "
            f"Add it to config/cities.py to enable."
        )
    property_paths = {"apartment": "mieszkanie", "plot": "dzialka"}
    if property_type not in property_paths:
        raise ValueError("unknown property_type '%s' — supported: %s" % (
            property_type, ", ".join(sorted(property_paths))
        ))

    c = CITIES[city_key]
    query = _QUERY
    if distance_radius is not None:
        query = f"{query}&distanceRadius={distance_radius}"
    return (
        f"https://www.otodom.pl/pl/wyniki/sprzedaz/{property_paths[property_type]}/"
        f"{c['voivodeship']}/{c['powiat']}/"
        f"{c['gmina']}/{c['city_slug']}{query}"
    )


def get_display_name(city_key: str) -> str:
    return CITIES[city_key]["display_name"]
