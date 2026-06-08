import logging
import os
import sys

scraper_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if scraper_path not in sys.path:
    sys.path.insert(0, scraper_path)

from scraper.fetch_and_parse import fetch_page


os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/test_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def test_fetch_page_returns_success_response():
    url = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice"

    response = fetch_page(url)

    assert response.status_code == 200
    logger.info("Response status code: %s for URL: %s", response.status_code, url)