import logging

from domain.models import ListingBasic, ListingFull
from scraping.client import fetch_page, is_allowed_to_scrape
from scraping.search_page import download_data_from_search_results
from scraping.listing_page import download_data_from_listing_page, get_offer_status
from domain.normalize import transform_data
from db.connection import get_fresh_connection
from db.schema import create_tables, run_migrations
from db import repositories as repo
from config.logging_config import setup_failed_offers_logger

failed_logger = setup_failed_offers_logger()


def _scrape_offer(offer: ListingBasic) -> ListingFull | None:
    """Fetches and normalizes a single listing page.

    Returns a ListingFull DTO - a single object holding data that will later
    be split across locations, apartments_sale_listings and features tables.
    """
    offer_url = offer.link
    id = offer.listing_id

    for attempt in range(1, 3):
        try:
            response = fetch_page(offer_url)
            listing_full = download_data_from_listing_page(response)
            normalized = transform_data(listing_full)
            logging.debug(f"offer {id} fetched successfully")
            return normalized
        except Exception as error:
            logging.warning(f"attempt {attempt}/2 failed for offer {id}: {error}")
            if attempt == 2:
                logging.error(f"all attempts failed for offer {id}, skipping")
                return None


def _find_closed_offers(fetched_offers: list, city: str) -> set:
    try:
        # short-lived connection only for the two SELECTs; released before the long HTTP loop
        conn, cur = get_fresh_connection()
        try:
            potentially_deleted = repo.find_potentially_deleted_offers(fetched_offers, city, cur)
            potentially_deleted_links = repo.find_offer_links(potentially_deleted, cur)
        finally:
            cur.close()
            conn.close()

        deleted_offers = set()
        for id_from_db, offer_link in potentially_deleted_links:
            logging.debug(f"checking offer {id_from_db}: {offer_link}")
            status = get_offer_status(offer_link)
            logging.debug(f"offer {id_from_db} status: {status}")
            if status is None:
                logging.warning(f"offer {id_from_db} — could not determine status, treating as deleted")
            if status is None or 'active' not in status:
                deleted_offers.add((id_from_db, status))

        logging.info(f"offers removed from otodom: {len(deleted_offers)}")
        return deleted_offers
    except Exception as error:
        logging.exception(f"error finding closed offers: {error}")
        raise


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

        if not all_offers_basic:
            logging.error("no offers fetched from search results — aborting sync")
            return

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
                    if id_db is None:
                        logging.warning(f"offer {id} — price check failed, skipping update")
                        continue
                    if new_price:
                        repo.update_active_offers((id_db, new_price, new_price_per_m), conn, cur)
                        updated_count += 1
                        logging.info(f"offer {id} price updated")

            logging.info("-----------------------------------------")
            logging.info(f"new offers saved: {new_count}")
            logging.info(f"price updates: {updated_count}")
            logging.info("-----------------------------------------")

        finally:
            cur.close()
            conn.close()

        # HTTP-only phase — no DB connection held during the ~minutes-long status check loop
        logging.info("checking for deleted offers...")
        deleted_offers = _find_closed_offers(all_offers_basic, city)

        if deleted_offers:
            # fresh connection just for the UPDATEs; the previous one would have been idle-killed by the DB
            logging.info("updating deleted offers in db...")
            conn, cur = get_fresh_connection()
            try:
                for deleted_offer in deleted_offers:
                    repo.update_deleted_offers(deleted_offer, conn, cur)
            finally:
                cur.close()
                conn.close()

        logging.info("done")

    except Exception as error:
        logging.exception("error in sync:")
        raise
