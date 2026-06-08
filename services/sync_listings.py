import logging

from domain.models import ListingBasic
from scraping.client import fetch_page, is_allowed_to_scrape
from scraping.search_page import download_data_from_search_results
from scraping.listing_page import download_data_from_listing_page, get_offer_status
from domain.normalize import transform_data
from db.connection import get_fresh_connection
from db.schema import create_tables, run_migrations
from db import repositories as repo
from config.logging_config import setup_failed_offers_logger

failed_logger = setup_failed_offers_logger()


def _scrape_offer(offer: ListingBasic) -> dict | None:
    offer_url = offer.link
    id = offer.listing_id

    for attempt in range(1, 3):
        try:
            response = fetch_page(offer_url)
            offer_data = download_data_from_listing_page(response)
            cleaned_offer_data = transform_data(offer_data)
            logging.debug(f"offer {id} fetched successfully")
            return cleaned_offer_data
        except Exception as error:
            logging.warning(f"attempt {attempt}/2 failed for offer {id}: {error}")
            if attempt == 2:
                logging.error(f"all attempts failed for offer {id}, skipping")
                return None


def _find_closed_offers(fetched_offers: list, city: str, cur) -> set:
    try:
        potentially_deleted = repo.find_potentially_deleted_offers(fetched_offers, city, cur)
        potentially_deleted_links = repo.find_offer_links(potentially_deleted, cur)

        deleted_offers = set()
        for id_from_db, offer_link in potentially_deleted_links:
            logging.debug(f"checking offer {id_from_db}: {offer_link}")
            status = get_offer_status(offer_link)
            logging.debug(f"offer {id_from_db} status: {status}")
            if status is None or 'active' not in status:
                deleted_offers.add((id_from_db, status))

        logging.info(f"offers removed from otodom: {len(deleted_offers)}")
        return deleted_offers
    except Exception as error:
        logging.exception(f"error finding closed offers: {error}")


def sync(url: str, city: str):
    try:
        result = is_allowed_to_scrape(url)
        logging.info(f"scraping allowed: {result}")

        conn, cur = get_fresh_connection()
        try:
            create_tables(cur)
            run_migrations(cur)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        logging.info("fetching basic data from search results...")
        all_offers_basic = download_data_from_search_results(url)
        logging.debug(f"all offers from search page: {all_offers_basic}")

        conn, cur = get_fresh_connection()
        try:
            new_count = 0
            updated_count = 0
            logging.info("processing offers...")

            for offer in all_offers_basic:
                id = offer.listing_id
                if len(str(id)) != 8:
                    continue
                logging.debug(f"checking offer {id}")

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
                    logging.info(f"offer {id} already in db, checking price...")
                    id_db, new_price, new_price_per_m = repo.check_if_price_changed(offer, cur)
                    if new_price:
                        repo.update_active_offers((id_db, new_price, new_price_per_m), conn, cur)
                        updated_count += 1
                        logging.info(f"offer {id} price updated")

            logging.info("-----------------------------------------")
            logging.info(f"new offers saved: {new_count}")
            logging.info(f"price updates: {updated_count}")
            logging.info("-----------------------------------------")

            logging.info("checking for deleted offers...")
            deleted_offers = _find_closed_offers(all_offers_basic, city, cur)
            logging.info("updating deleted offers in db...")
            for deleted_offer in deleted_offers:
                repo.update_deleted_offers(deleted_offer, conn, cur)

        finally:
            cur.close()
            conn.close()

        logging.info("done")

    except Exception as error:
        logging.exception("error in sync:")
        raise
