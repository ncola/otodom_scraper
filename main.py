import os
from dotenv import load_dotenv

load_dotenv()

from config.logging_config import setup_logger
from config.cities import build_search_url, get_display_name
from scraping.search_page import validate_search_url
from services.sync_listings import sync
from services.sync_latest import sync_latest

setup_logger()

if __name__ == "__main__":
    city = os.environ.get("CITY", "katowice")
    property_type = os.environ.get("PROPERTY_TYPE", "apartment")

    # SEARCH_URL is an optional escape hatch — lets us point at a custom URL
    # (e.g. a district) without touching cities.py. Skips URL validation.
    url = os.environ.get("SEARCH_URL")
    if url is None:
        url = build_search_url(city, property_type)
        validate_search_url(url, get_display_name(city))

    # "latest" — lightweight, runs more often, only checks the first page(s) for new offers
    # "full"   — full sync, runs daily, scrapes everything and checks for deleted offers
    mode = os.environ.get("SCRAPE_MODE", "full")

    if mode == "latest":
        max_pages = int(os.environ.get("LATEST_MAX_PAGES", "1"))
        sync_latest(url, city, max_pages=max_pages, property_type=property_type)
    else:
        sync(url, city, property_type=property_type)
