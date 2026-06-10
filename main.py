import os
from dotenv import load_dotenv

load_dotenv()

from config.logging_config import setup_logger
from services.sync_listings import sync

setup_logger()

if __name__ == "__main__":
    city = os.environ.get("CITY", "katowice")
    url = os.environ.get("SEARCH_URL", "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72")

    if not city or not url:
        raise ValueError("CITY and SEARCH_URL environment variables must be set.")

    sync(url, city)
