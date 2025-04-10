import os,sys
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.scraper import scrape_all_pages, is_allowed_to_scrape
from db.db_setup import create_tables
from db.db_operations import insert_new_listing, update_offers

#url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"
url= "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72"

def main():
    try:
        # Upewnij się, ze to dozwolone
        result = is_allowed_to_scrape(url)
        print(f"\nIs fetching page {url} allowed?: {result}\n")

        # Utwórz tabele jezeli nie istnieją
        create_tables()

        # Pobierz dane
        need_update_offers, new_offers_to_database = scrape_all_pages(url)

        # Zapisz dane w bazie
        print("\nRozpoczynam zapisywanie ofert w bazie.............")
        for offer_data in new_offers_to_database:
            insert_new_listing(offer_data)  
            print("-"*10)
            print("Zakończono wprowadzanie danych do bazy")
        
        # Update danych w bazie
        print("\nRozpoczynam update ofert w bazie.............")
        for offer in need_update_offers:
            update_offers(offer)
            print("Zakończono update danych w bazie")
            
    except Exception as error:
        print(f"Error main: {error}")

if __name__ == "__main__": 
    main()
