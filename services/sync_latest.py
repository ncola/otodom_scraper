import logging

from domain.models import ListingBasic
from scraping.client import fetch_page, is_allowed_to_scrape
from scraping.search_page import parse_offers_from_response
from services.sync_listings import _scrape_offer, _strategy
from db.connection import get_fresh_connection
from db.schema import create_tables, run_migrations
from db import repositories as repo
from db import repositories_plots as plots_repo
from config.logging_config import setup_failed_offers_logger

failed_logger = setup_failed_offers_logger()


def _process_offers(offers: list[ListingBasic], conn, cur, repository=repo,
                    parser=None, normalizer=None) -> tuple[int, int, bool]:
    """Processes a list of offers from a single page.

    For each offer: saves it if new, checks price if already known.
    Stops early and returns stop_early=True on the first known offer —
    since results are sorted by latest, everything after is already in DB.

    Returns (new_count, updated_count, stop_early).
    """
    new_count = 0
    updated_count = 0

    for offer in offers:
        if len(str(offer.listing_id)) != 8:
            continue

        if not repository.check_if_offer_exists(offer, cur):
            offer_data = _scrape_offer(offer, parser, normalizer) if parser else _scrape_offer(offer)
            if offer_data:
                id_db = repository.insert_new_listing(offer_data, conn, cur)
                new_count += 1
                logging.info(f"offer {offer.listing_id} saved to db under id {id_db}")
            else:
                logging.warning(f"failed to fetch offer {offer.listing_id}, skipping")
                failed_logger.error(f"{offer.listing_id} | {offer.link} | failed to fetch full offer data")
        else:
            logging.info(f"offer {offer.listing_id} already in db — assuming older offers also known, stopping early")
            id_db, new_price, new_price_per_m = repository.check_if_price_changed(offer, cur)
            if id_db and new_price:
                repository.update_active_offers((id_db, new_price, new_price_per_m), conn, cur)
                updated_count += 1
                logging.info(f"offer {offer.listing_id} price updated")
            return new_count, updated_count, True

    return new_count, updated_count, False


def sync_latest(url: str, city: str, max_pages: int = 1, property_type: str = "apartment"):
    """Lightweight scraper for catching the newest listings.

    Fetches only the first `max_pages` pages (sorted by latest) and stops
    early once it hits an offer already in the database — meaning everything
    further back is already known. Skips the deletion check entirely.
    """
    try:
        repository, parser, normalizer, _ = _strategy(property_type)
        result = is_allowed_to_scrape(url)
        logging.info(f"scraping allowed: {result}")

        total_new = 0
        total_updated = 0

        conn, cur = get_fresh_connection()
        try:
            create_tables(cur)
            run_migrations(cur)
            if property_type == "plot":
                plots_repo.ensure_tables_exist(cur)
            conn.commit()
            for page in range(1, max_pages + 1):
                page_url = f"{url}&page={page}"
                logging.info(f"sync_latest: fetching page {page}/{max_pages}")
                html_response = fetch_page(page_url)
                if html_response is None:
                    logging.warning(f"failed to fetch page {page}, skipping")
                    continue

                offers = parse_offers_from_response(html_response, page=page)
                new_count, updated_count, stop_early = _process_offers(
                    offers, conn, cur, repository, parser, normalizer
                )
                total_new += new_count
                total_updated += updated_count

                if stop_early:
                    break

        finally:
            cur.close()
            conn.close()

        logging.info("-----------------------------------------")
        logging.info(f"sync_latest — new offers saved: {total_new}")
        logging.info(f"sync_latest — price updates: {total_updated}")
        logging.info("-----------------------------------------")

    except Exception as error:
        logging.exception(f"error in sync_latest: {error}")
        raise
