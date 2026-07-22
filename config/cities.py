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
}

_BASE = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie"
_QUERY = "?viewType=listing&by=LATEST&direction=DESC&limit=72"


def build_search_url(city_key: str) -> str:
    if city_key not in CITIES:
        supported = ", ".join(sorted(CITIES.keys()))
        raise ValueError(
            f"unknown city '{city_key}' — supported: {supported}. "
            f"Add it to config/cities.py to enable."
        )
    c = CITIES[city_key]
    return (
        f"{_BASE}/{c['voivodeship']}/{c['powiat']}/"
        f"{c['gmina']}/{c['city_slug']}{_QUERY}"
    )


def get_display_name(city_key: str) -> str:
    return CITIES[city_key]["display_name"]
