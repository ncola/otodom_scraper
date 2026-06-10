import os
from dotenv import load_dotenv

load_dotenv()

from config.logging_config import setup_logger
from services.sync_listings import sync
from services.sync_latest import sync_latest

setup_logger()

if __name__ == "__main__":
    city = os.environ.get("CITY", "katowice")
    url = os.environ.get("SEARCH_URL", "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72")

    if not city or not url:
        raise ValueError("CITY and SEARCH_URL environment variables must be set.")

    # "latest" — lightweight, runs more often, only checks the first page(s) for new offers
    # "full"   — full sync, runs daily, scrapes everything and checks for deleted offers
    mode = os.environ.get("SCRAPE_MODE", "full")

    if mode == "latest":
        max_pages = int(os.environ.get("LATEST_MAX_PAGES", "1"))
        sync_latest(url, city, max_pages=max_pages)
    else:
        sync(url, city)
