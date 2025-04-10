import os,sys
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.scraper import scrape_all_pages, is_allowed_to_scrape
from db.setup_database import create_tables, insert_new_listing
from db.insert_data import insert_new_listing

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"


def main():
    try:
        # Upewnij się, ze to dozwolone
        result = is_allowed_to_scrape(url_main)
        print(f"Is fetching page {url_main} allowed?: {result}")

        # Utwórz tabele jezeli nie istnieją
        create_tables()

        # Pobierz i zapisz dane ofert w bazie
        offers_data = scrape_all_pages()
        for offer_data in offers_data:
            insert_new_listing(offer_data)  
            print("-"*10)
    except Exception as error:
        print(f"Error: {error}")

if __name__ == "__main__": 
    main()

