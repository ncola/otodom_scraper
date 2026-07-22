"""Database operations for ``plots_sale_listings`` only."""

import datetime
import json
import logging

from domain.models import ListingBasic, PlotListingFull
from db.repositories import check_location_table, insert_into_locations_table


def ensure_tables_exist(cur) -> None:
    """Fail before scraping when the Neon migration was not applied."""
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('plots_sale_listings', 'plots_price_history')
    """)
    present = {row[0] for row in cur.fetchall()}
    required = {'plots_sale_listings', 'plots_price_history'}
    missing = required - present
    if missing:
        raise RuntimeError(
            "missing Neon plot tables: " + ", ".join(sorted(missing))
        )


def _params(listing: PlotListingFull, location_id: int) -> dict:
    return {
        "otodom_listing_id": listing.listing_id, "offer_link": listing.offer_link,
        "source_status": listing.source_status, "active": listing.active,
        "detected_inactive_at": listing.detected_inactive_at, "title": listing.title,
        "description_text": listing.description_text, "market": listing.market,
        "advert_type": listing.advert_type, "advertiser_type": listing.advertiser_type,
        "creation_source": listing.creation_source, "creation_at": listing.creation_at,
        "modified_at": listing.modified_at, "pushed_up_at": listing.pushed_up_at,
        "exclusive_offer": listing.exclusive_offer, "area": listing.area, "price": listing.price,
        "updated_price": listing.price, "price_per_m": listing.price_per_m,
        "updated_price_per_m": listing.price_per_m, "location_id": location_id,
        "street": listing.street, "latitude": listing.latitude, "longitude": listing.longitude,
        "plot_types": listing.plot_types, "dimensions": listing.dimensions, "fence": listing.fence,
        "media_types": listing.media_types, "access_types": listing.access_types,
        "vicinity_types": listing.vicinity_types, "owner_id": listing.owner_id,
        "owner_name": listing.owner_name, "agency_id": listing.agency_id,
        "agency_name": listing.agency_name, "local_plan_url": listing.local_plan_url,
        "video_url": listing.video_url, "view3d_url": listing.view3d_url,
        "walkaround_url": listing.walkaround_url,
        "source_target": json.dumps(listing.source_target),
        "source_attributes": json.dumps(listing.source_attributes),
        "source_characteristics": json.dumps(listing.source_characteristics),
    }


def insert_new_listing(listing: PlotListingFull, conn, cur) -> int:
    try:
        insert_into_locations_table(cur, listing)
        location = check_location_table(cur, listing)
        if location is None:
            raise ValueError(f"location not found after inserting plot {listing.listing_id}")

        query = """
            INSERT INTO plots_sale_listings (
                otodom_listing_id, offer_link, source_status, active, detected_inactive_at,
                title, description_text, market, advert_type, advertiser_type, creation_source,
                creation_at, modified_at, pushed_up_at, exclusive_offer,
                area, price, updated_price, price_per_m, updated_price_per_m,
                location_id, street, latitude, longitude, plot_types, dimensions, fence,
                media_types, access_types, vicinity_types, owner_id, owner_name, agency_id,
                agency_name, local_plan_url, video_url, view3d_url, walkaround_url,
                source_target, source_attributes, source_characteristics
            ) VALUES (
                %(otodom_listing_id)s, %(offer_link)s, %(source_status)s, %(active)s, %(detected_inactive_at)s,
                %(title)s, %(description_text)s, %(market)s, %(advert_type)s, %(advertiser_type)s, %(creation_source)s,
                %(creation_at)s, %(modified_at)s, %(pushed_up_at)s, %(exclusive_offer)s,
                %(area)s, %(price)s, %(updated_price)s, %(price_per_m)s, %(updated_price_per_m)s,
                %(location_id)s, %(street)s, %(latitude)s, %(longitude)s, %(plot_types)s, %(dimensions)s, %(fence)s,
                %(media_types)s, %(access_types)s, %(vicinity_types)s, %(owner_id)s, %(owner_name)s, %(agency_id)s,
                %(agency_name)s, %(local_plan_url)s, %(video_url)s, %(view3d_url)s, %(walkaround_url)s,
                %(source_target)s::jsonb, %(source_attributes)s::jsonb, %(source_characteristics)s::jsonb
            ) RETURNING id
        """
        cur.execute(query, _params(listing, location[0]))
        listing_id = cur.fetchone()[0]
        conn.commit()
        return listing_id
    except Exception:
        conn.rollback()
        logging.exception("error inserting plot listing")
        return None


def check_if_offer_exists(offer: ListingBasic, cur) -> bool:
    cur.execute("SELECT 1 FROM plots_sale_listings WHERE otodom_listing_id = %s LIMIT 1", (offer.listing_id,))
    return cur.fetchone() is not None


def check_if_price_changed(offer: ListingBasic, cur) -> tuple:
    cur.execute("SELECT id, updated_price FROM plots_sale_listings WHERE otodom_listing_id = %s", (offer.listing_id,))
    row = cur.fetchone()
    if row is None:
        return None, False, False
    listing_id, old_price = row
    return (listing_id, offer.price, offer.price_per_m) if old_price != offer.price else (listing_id, False, False)


def update_active_offers(offer_data, conn, cur):
    try:
        listing_id, new_price, new_price_per_m = offer_data
        cur.execute("SELECT updated_price FROM plots_sale_listings WHERE id = %s", (listing_id,))
        old_price = cur.fetchone()[0]
        cur.execute("""
            UPDATE plots_sale_listings
            SET updated_price = %s, updated_price_per_m = %s, active = TRUE,
                detected_inactive_at = NULL, db_updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (new_price, new_price_per_m, listing_id))
        cur.execute("""
            INSERT INTO plots_price_history (listing_id, old_price, new_price, change_date)
            VALUES (%s, %s, %s, %s)
        """, (listing_id, old_price, new_price, datetime.datetime.now(datetime.timezone.utc)))
        conn.commit()
    except Exception:
        conn.rollback()
        logging.exception("error updating plot price")


def find_potentially_deleted_offers(fetched_offers: list[ListingBasic], city: str, cur) -> set:
    cur.execute("""
        SELECT p.id, p.otodom_listing_id
        FROM plots_sale_listings p JOIN locations l ON p.location_id = l.id
        WHERE p.active IS TRUE AND l.city = %s
    """, (city.lower(),))
    fetched_ids = {offer.listing_id for offer in fetched_offers}
    return {row[0] for row in cur.fetchall() if row[1] not in fetched_ids}


def find_offer_links(potentially_deleted: set, cur) -> set:
    if not potentially_deleted:
        return set()
    cur.execute("SELECT id, offer_link FROM plots_sale_listings WHERE id = ANY(%s)", (list(potentially_deleted),))
    return set(cur.fetchall())


def update_deleted_offers(offer_data, conn, cur):
    try:
        cur.execute("""
            UPDATE plots_sale_listings
            SET active = FALSE, detected_inactive_at = CURRENT_TIMESTAMP, db_updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (offer_data[0],))
        conn.commit()
    except Exception:
        conn.rollback()
        logging.exception("error marking plot as inactive")
