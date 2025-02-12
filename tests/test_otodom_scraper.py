import unittest
import logging
import os
import sys

# ustawienie ścieżki do kodu
scraper_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if scraper_path not in sys.path:
    sys.path.insert(0, scraper_path)

from scraper.otodom_scraper import scrape
from scraper.utils import fetch_page

# konfiguracja loggera
log_file = 'logs/test_log.txt'  
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()  


def test_fetch_page():
    logger.info("Test: test_fetch_page")
    url = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice"
    response = fetch_page(url)
    assert response.status_code == 200, f"Expected status 200 but got {response.status_code}"
    logger.info(f"Response status code: {response.status_code} for URL: {url}")

def test_scrape():
    pass

def run_tests():
    logger.info("Rozpoczęcie testów.")
    
    try:
        test_fetch_page()
        test_scrape()
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
    else:
        logger.info("Wszystkie testy zakończone pomyślnie.")

if __name__ == '__main__':
    run_tests()
