import os,sys
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.scraper import scrape_all_pages, is_allowed_to_scrape
from db.setup_database import create_tables
from db.insert_data import insert_new_listing

#url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"
url= "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72"

def main():
    try:
        # Upewnij się, ze to dozwolone
        result = is_allowed_to_scrape(url)
        print(f"Is fetching page {url} allowed?: {result}")

        # Utwórz tabele jezeli nie istnieją
        create_tables()

        # Pobierz dane
        offers_data = scrape_all_pages(url)

        # Zapisz dane w bazie
        for offer_data in offers_data:
            insert_new_listing(offer_data)  
            print("-"*10)
            
    except Exception as error:
        print(f"Error main: {error}")

if __name__ == "__main__": 
    main()
