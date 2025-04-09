import os,sys
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.otodom_scraper import scrape_all_pages
from db.setup_and_manage_database import create_tables, insert_new_listing

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"

# Utwórz tabele jezeli nie istnieją
create_tables()

# Pobierz i zapisz dane ofert w bazie
offers_data = scrape_all_pages()
for offer_data in offers_data:
    insert_new_listing(offer_data)  
    print("-"*10)
