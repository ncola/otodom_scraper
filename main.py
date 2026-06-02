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
        logging.info(f"scraping allowed: {result}")

        # Utwórz tabele jezeli nie istnieją
        create_tables(cur)

        # pobierz dane
        logging.info("fetching basic data from search results...")
        all_offers_basic_from_sarching_page = download_data_from_search_results(url)

        logging.debug(f"all offers from search page: {all_offers_basic_from_sarching_page}")

        a_n = 0
        b_n = 0
        logging.info("processing offers...")
        for offer in all_offers_basic_from_sarching_page:
            id = offer.get("listing_id")
            if len(str(id)) !=8: 
                continue
            logging.debug(f"checking offer {id}")
            if not check_if_offer_exists(offer, cur):
                offer_data = scrape_offer(offer)
                if offer_data:
                    id_db = insert_new_listing(offer_data, conn, cur)
                    a_n += 1
                    logging.info(f"offer {id} saved to db under id {id_db}")
                else:
                    logging.warning(f"failed to fetch offer {id}, skipping")
                    link = offer.get("link", "no link")
                    failed_logger.error(f"{id} | {link} | failed to fetch full offer data")
            else:
                logging.info(f"offer {id} already in db, checking price...")
                id_db, new_price, new_price_per_m = check_if_price_changed(offer, cur)

                if new_price:
                    update_active_offers((id_db, new_price, new_price_per_m), conn, cur)
                    b_n += 1
                    logging.info(f"offer {id} price updated")

        logger.info("-----------------------------------------")
        logger.info(f"new offers saved: {a_n}")
        logger.info(f"price updates: {b_n}")
        logger.info("-----------------------------------------")

        logging.info("checking for deleted offers...")
        deleted_offers = find_closed_offers(all_offers_basic_from_sarching_page, city, cur)
        logging.info("updating deleted offers in db...")
        for deleted_offer in deleted_offers:
            update_deleted_offers(deleted_offer, conn, cur)

        logging.info("done")

    except Exception as error:
        logging.exception("error in main function:")
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
