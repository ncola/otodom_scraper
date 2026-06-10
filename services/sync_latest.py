import logging

from scraping.client import fetch_page, is_allowed_to_scrape
from scraping.search_page import parse_offers_from_response
from services.sync_listings import _scrape_offer
from db.connection import get_fresh_connection
from db import repositories as repo
from config.logging_config import setup_failed_offers_logger

failed_logger = setup_failed_offers_logger()


def sync_latest(url: str, city: str, max_pages: int = 1):
    """Lightweight scraper for catching the newest listings.

    Fetches only the first `max_pages` pages (sorted by latest) and stops
    early once it hits an offer already in the database — meaning everything
    further back is already known. Skips the deletion check entirely.
    """
    try:
        result = is_allowed_to_scrape(url)
        logging.info(f"scraping allowed: {result}")

        new_count = 0
        updated_count = 0
        stop_early = False

        conn, cur = get_fresh_connection()
        try:
            for page in range(1, max_pages + 1):
                if stop_early:
                    break

                page_url = f"{url}&page={page}"
                logging.info(f"sync_latest: fetching page {page}/{max_pages}")
                html_response = fetch_page(page_url)
                if html_response is None:
                    logging.warning(f"failed to fetch page {page}, skipping")
                    continue

                offers = parse_offers_from_response(html_response, page=page)

                for offer in offers:
                    id = offer.listing_id
                    if len(str(id)) != 8:
                        continue

                    if not repo.check_if_offer_exists(offer, cur):
                        offer_data = _scrape_offer(offer)
                        if offer_data:
                            id_db = repo.insert_new_listing(offer_data, conn, cur)
                            new_count += 1
                            logging.info(f"offer {id} saved to db under id {id_db}")
                        else:
                            logging.warning(f"failed to fetch offer {id}, skipping")
                            failed_logger.error(f"{id} | {offer.link} | failed to fetch full offer data")
                    else:
                        logging.info(f"offer {id} already in db — assuming older offers also known, stopping early")
                        id_db, new_price, new_price_per_m = repo.check_if_price_changed(offer, cur)
                        if id_db and new_price:
                            repo.update_active_offers((id_db, new_price, new_price_per_m), conn, cur)
                            updated_count += 1
                            logging.info(f"offer {id} price updated")
                        stop_early = True
                        break

        finally:
            cur.close()
            conn.close()

        logging.info("-----------------------------------------")
        logging.info(f"sync_latest — new offers saved: {new_count}")
        logging.info(f"sync_latest — price updates: {updated_count}")
        logging.info("-----------------------------------------")

    except Exception as error:
        logging.exception(f"error in sync_latest: {error}")
        raise
