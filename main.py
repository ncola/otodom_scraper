import os,sys
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.otodom_scraper import scrape_all_pages
from db.setup_and_manage_database import create_tables, insert_new_listing

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"

import requests
from urllib.robotparser import RobotFileParser


def is_allowed_to_scrape(url: str) -> bool:
    domain = '/'.join(url.split('/')[:3])
    robots_url = domain + '/robots.txt'

    try:
        response = requests.get(robots_url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
        response.raise_for_status() 
        rp = RobotFileParser()
        rp.parse(response.text.splitlines())

        return rp.can_fetch("*", url)

    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas pobierania robots.txt: {e}")
        return False

result = is_allowed_to_scrape(url_main)
print(f"Is fetching page {url_main} allowed?: {result}")

# Utwórz tabele jezeli nie istnieją
create_tables()

# Pobierz i zapisz dane ofert w bazie
offers_data = scrape_all_pages()
for offer_data in offers_data:
    insert_new_listing(offer_data)  
    print("-"*10)


