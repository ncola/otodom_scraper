import os,sys
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.scraper import is_allowed_to_scrape, scrape_offer
from scraper.fetch_and_parse import download_data_from_search_results, check_if_offer_exists, check_if_price_changed, find_closed_offers
from db.db_setup import create_tables
from db.db_operations import insert_new_listing, update_active_offers, update_deleted_offers, get_db_connection

#url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"
url= "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72"
#url= "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC"
city = 'katowice'

def main():
    conn = None
    cur = None  
    try:
        # Upewnij się, ze to dozwolone
        conn = get_db_connection()
        if conn is None:
            print("Connection to the database failed")
            return
        cur=conn.cursor()
        
        result = is_allowed_to_scrape(url)
        print(f"\nIs fetching page {url} allowed?: {result}\n")

        # Utwórz tabele jezeli nie istnieją
        create_tables(cur)

        # Pobierz dane
        print("Rozpoczynam pobieranie danych z search result....")
        all_offers_basic_from_sarching_page = download_data_from_search_results(url)
        print('-' * 40)
        print(f"Liczba znalezionych ofert: {len(all_offers_basic_from_sarching_page)}")
        print('-' * 40)

        # Sprawdz ktore oferty juz sa w bazie (i dodaj/aktualizuj cene): 
        for offer in all_offers_basic_from_sarching_page:
            id = offer.get("listing_id")
            # Jezeli dana oferta nie znajduje sie jeszcze w bazie, pobierz ja i zapisz
            if not check_if_offer_exists(offer, cur):
                offer_data = scrape_offer(offer) # pobierz znaleziona oferte w całości
                insert_new_listing(offer_data, conn, cur) # wstaw do bazy
                
                print(f"Oferta {id} jest nowa i zakończono wprowadzanie jej do bazy\n")
            # jezeli oferta sie znajduje, sprawdz czy nie zmienila sie cena
            else:
                id_db, new_price = check_if_price_changed(offer, cur)
                # Jezeli new_price to nie False tylko liczba tzn ze cena sie zmienila - update bazy
                if new_price:
                    update_active_offers(offer, conn, cur)
                else:
                    print(f"Cena w ofercie {id} ({id_db}) aktualna\n")

        # Na koncu sprawdz, czy sa jakies usuniete oferty
        deleted_offers = find_closed_offers(all_offers_basic_from_sarching_page, city,cur)
        for deletd_offer in deleted_offers:
            update_deleted_offers(deletd_offer, conn, cur)

            
    except Exception as error:
        print(f"Error main: {error}")
    finally: 
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__": 
    main()
