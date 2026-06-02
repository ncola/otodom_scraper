import os,sys, logging
from colorlog import ColoredFormatter
scraper_path = os.path.join(os.path.dirname(__file__), 'scraper')
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

from scraper.scraper import is_allowed_to_scrape, scrape_offer
from scraper.fetch_and_parse import download_data_from_search_results, check_if_offer_exists, check_if_price_changed, find_closed_offers
from db.db_setup import create_tables
from db.db_operations import insert_new_listing, update_active_offers, update_deleted_offers, get_db_connection

from dotenv import load_dotenv
load_dotenv()

from config.logging_config import setup_logger, setup_failed_offers_logger
logger = setup_logger()
failed_logger = setup_failed_offers_logger()

# ZASADY: WYSZUKIWANIE MIESZKAN NA SPRZEDAZ W DANYM MIESCIE BEZ ZADNYCH FILTROW, ZALECANE SORTOWANIE OD NAJNOWSZYCH I MAX LIMIT OFERT NA STRONE

#url= "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72"
#city = 'katowice'

def main(url, city):
    conn = None
    cur = None  
    try:
        
        conn = get_db_connection()
        if conn is None:
            logging.critical("Connection to the database failed")
            return
        cur=conn.cursor()
        
        # Upewnij się, ze to dozwolone
        result = is_allowed_to_scrape(url)
        logging.warning(f"Is fetching page {url} allowed?: {result}")

        # Utwórz tabele jezeli nie istnieją
        create_tables(cur)

        # pobierz dane
        logging.info("Rozpoczynam pobieranie podstawowych danych z wyniku wyszukiwania...")
        all_offers_basic_from_sarching_page = download_data_from_search_results(url)

        logging.debug(f"Dane z all_offers_basic_from_sarching_page: \n{'--' * 100}\n{all_offers_basic_from_sarching_page}\n {'--' * 100}\n")

        # sprawdz ktore oferty juz sa w bazie (i dodaj/aktualizuj cene): 
        a_n = 0
        b_n = 0
        logging.info("\nRozpoczynam sprawdzanie i pobieranie ofert...")
        for offer in all_offers_basic_from_sarching_page:
            id = offer.get("listing_id")
            if len(str(id)) !=8: 
                continue
            logging.debug(f"Sprawdzam oferte {id}")
            # Jezeli dana oferta nie znajduje sie jeszcze w bazie, pobierz ja i zapisz
            if not check_if_offer_exists(offer, cur):
                offer_data = scrape_offer(offer) # pobierz znaleziona oferte w całości
                if offer_data: # jezeli brak bledu wstaw dane do bazy
                    id_db = insert_new_listing(offer_data, conn, cur) # wstaw do bazy
                    a_n+=1
                    logging.info(f"Oferta {id} zapisana w bazie pod id {id_db}\n")
                else:
                    logging.warning(f"Nie udało się pobrać pełnych danych oferty {id} – pomijam.")
                    link = offer.get("link", "brak linku")
                    failed_logger.error(f"{id} | {link} | Nie udało się pobrać pełnych danych oferty")

            # jezeli oferta sie znajduje, sprawdz czy nie zmienila sie cena
            else:
                logging.info(f"Oferta {id} istnieje w bazie, sprawdzanie czy zmieniła się cena...")
                id_db, new_price, new_price_per_m = check_if_price_changed(offer, cur)
                # Jezeli new_price to nie False tylko liczba tzn ze cena sie zmienila - update bazy

                if new_price:
                    update_active_offers((id_db, new_price, new_price_per_m), conn, cur)
                    b_n+=1
                    logging.info(f"Update ceny oferty {id} w bazie zakonczony")

        logger.info("-----------------------------------------")
        logger.info(f": {a_n} ofert")
        logger.info(f"Update cen w bazie dotyczyl: {b_n} ofert")
        logger.info("-----------------------------------------")

        # Na koncu sprawdz, czy sa jakies usuniete oferty
        logging.info("Rozpoczynam sprawdzanie czy czy jakieś oferty nie zostały usunięte z otodom...")
        deleted_offers = find_closed_offers(all_offers_basic_from_sarching_page, city,cur)
        logging.info("Rozpocznynam update ofert w bazie, które zostały usunięte...")
        for deletd_offer in deleted_offers:
            update_deleted_offers(deletd_offer, conn, cur)
        
        logging.info("Zakończono")
            
    except Exception as error:
        logging.exception("Error in main fucntion:")
    finally: 
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__": 
    city="katowice"
    url="https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice/katowice/katowice?viewType=listing&by=LATEST&direction=DESC&limit=72"

    if not city or not url:
        raise ValueError("CITY and URL environment variables must be set.")
    
    main(url, city)
