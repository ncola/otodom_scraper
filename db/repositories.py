import datetime
import logging

from domain.models import ListingBasic, ListingFull


# ---------- locations table ----------

def check_location_table(cur, listing: ListingFull):
    location_query = """
        SELECT id FROM locations
        WHERE voivodeship = %s
        AND city = %s
        AND (district = %s OR (district IS NULL AND %s IS NULL))
        ;"""
    cur.execute(location_query, (listing.voivodeship, listing.city, listing.district, listing.district))
    return cur.fetchone()


def insert_into_locations_table(cur, listing: ListingFull):
    location_result = check_location_table(cur, listing)
    if not location_result:
        logging.debug(f"location ({listing.voivodeship}, {listing.city}, {listing.district}) not in db, inserting")
        cur.execute("""
            INSERT INTO locations (voivodeship, city, district)
            VALUES (%s, %s, %s)
            RETURNING id
            ;""", (listing.voivodeship, listing.city, listing.district))
        new_id = cur.fetchone()[0]
        logging.debug(f"location inserted with id: {new_id}")
    else:
        logging.debug(f"location already in db under id: {location_result}")


# ---------- apartments_sale_listings table ----------

def insert_into_apartments_sale_listings_table(cur, listing: ListingFull) -> int:
    location_id = check_location_table(cur, listing)
    if location_id is None:
        raise ValueError(f"location not found in db for offer {listing.listing_id} — make sure it was inserted first")

    listing_query = """
        INSERT INTO apartments_sale_listings (otodom_listing_id, title, market, advert_type,
        creation_date, creation_time, pushed_up_at, exclusive_offer, creation_source, description_text,
        area, price, updated_price, price_per_m, updated_price_per_m, location_id, street, rent_amount,
        rooms_num, floor_num, heating, ownership, proper_type, construction_status, energy_certificate,
        building_build_year, building_floors_num, building_material, building_type, windows_type,
        local_plan_url, video_url, view3d_url, walkaround_url, development_id, development_title,
        owner_id, owner_name, agency_id, agency_name, offer_link, active, detected_inactive_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        ;"""

    cur.execute(listing_query, (
        listing.listing_id, listing.title, listing.market, listing.advert_type,
        listing.creation_date, listing.creation_time, listing.pushed_up_at,
        listing.exclusive_offer, listing.creation_source, listing.description_text,
        listing.area, listing.price, listing.price,       # updated_price = price on first insert
        listing.price_per_m, listing.price_per_m,         # updated_price_per_m = price_per_m on first insert
        location_id[0], listing.street, listing.rent_amount, listing.rooms_num,
        listing.floor_num, listing.heating, listing.ownership, listing.proper_type,
        listing.construction_status, listing.energy_certificate, listing.building_build_year,
        listing.building_floors_num, listing.building_material, listing.building_type,
        listing.windows_type, listing.local_plan_url, listing.video_url,
        listing.view3d_url, listing.walkaround_url, listing.development_id,
        listing.development_title, listing.owner_id, listing.owner_name,
        listing.agency_id, listing.agency_name, listing.offer_link,
        listing.active, listing.detected_inactive_at,
    ))
    return cur.fetchone()[0]


def insert_into_features_table(cur, listing: ListingFull, id: int):
    features_query = """
        INSERT INTO features (listing_id, internet, cable_television, phone, roller_shutters,
        anti_burglary_door, entryphone, monitoring, alarm, closed_area, furniture, washing_machine,
        dishwasher, fridge, stove, oven, tv, balcony, usable_room, garage, basement, garden, terrace,
        lift, two_storey, separate_kitchen, air_conditioning)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s)
        ;"""

    features_offer = list((listing.features or '').split(' '))
    features_all_possibilities = (
        'internet', 'cable_television', 'phone', 'roller_shutters',
        'anti_burglary_door', 'entryphone', 'monitoring', 'alarm',
        'closed_area', 'furniture', 'washing_machine', 'dishwasher',
        'fridge', 'stove', 'oven', 'tv', 'balcony', 'usable_room',
        'garage', 'basement', 'garden', 'terrace', 'lift', 'two_storey',
        'separate_kitchen', 'air_conditioning'
    )
    features_bools = [feature in features_offer for feature in features_all_possibilities]
    cur.execute(features_query, (id, *features_bools))


def insert_new_listing(listing: ListingFull, conn, cur) -> int:
    try:
        insert_into_locations_table(cur, listing)
        created_offer_id = insert_into_apartments_sale_listings_table(cur, listing)
        insert_into_features_table(cur, listing, created_offer_id)
        logging.debug(f"offer saved to db with id: {created_offer_id}")
        conn.commit()
        return created_offer_id
    except Exception as error:
        conn.rollback()
        logging.exception(f"error inserting new listing: {error}")


# ---------- price_history table ----------

def update_active_offers(offer_data, conn, cur):
    try:
        id, new_price, new_price_per_m = offer_data
        change_date = datetime.date.today()

        # must read old price before overwriting updated_price
        cur.execute("SELECT updated_price FROM apartments_sale_listings WHERE id = %s", (id,))
        old_price = cur.fetchone()[0]

        cur.execute("""
            UPDATE apartments_sale_listings
            SET
                updated_price = %s,
                updated_price_per_m = %s,
                db_updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            ;""", (new_price, new_price_per_m, id))
        logging.debug(f"offer {id} price updated — new price: {new_price}, new price per m2: {new_price_per_m}")

        cur.execute("""
            INSERT INTO price_history (listing_id, old_price, new_price, change_date)
            VALUES (%s, %s, %s, %s)
            """, (id, old_price, new_price, change_date))
        logging.debug(f"offer {id} price history saved: {old_price} → {new_price}")

        conn.commit()
    except Exception as error:
        conn.rollback()
        logging.exception(f"error updating active offers: {error}")


def update_deleted_offers(offer_data, conn, cur):
    try:
        update_inactive_query = """
            UPDATE apartments_sale_listings
            SET
                active = %s,
                detected_inactive_at = %s,
                db_updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            ;"""
        current_date = datetime.date.today()
        id_db = offer_data[0]
        cur.execute(update_inactive_query, (False, current_date, id_db))
        logging.debug(f"offer {id_db} marked as inactive on {current_date}")
        conn.commit()
    except Exception as error:
        logging.exception(f"error updating deleted offers: {error}")


# ---------- checks (cross-table queries used by the service layer) ----------

def check_if_offer_exists(offer: ListingBasic, cur) -> bool:
    try:
        if_exists_query = """
            SELECT id
            FROM apartments_sale_listings
            WHERE otodom_listing_id = %s AND area = %s
            LIMIT 1
            ;"""
        cur.execute(if_exists_query, (offer.listing_id, offer.area))
        result = cur.fetchone()
        if result is None:
            logging.debug(f"offer {offer.listing_id} (area: {offer.area}) not in db — will fetch")
            return False
        else:
            logging.debug(f"offer {offer.listing_id} (area: {offer.area}) already in db under id: {result}")
            return True
    except Exception as error:
        logging.exception(f"error checking if offer exists in db: {error}")
        return None


def check_if_price_changed(offer: ListingBasic, cur) -> tuple:
    try:
        id_otodom = offer.listing_id
        new_price = offer.price
        new_price_per_m = offer.price_per_m

        cur.execute("""
            SELECT id, updated_price
            FROM apartments_sale_listings
            WHERE otodom_listing_id = %s
            ;""", (id_otodom,))
        result = cur.fetchone()
        id_db, old_price = result

        logging.debug(f"offer {id_otodom} — price on site: {new_price} (per m2: {new_price_per_m})")
        logging.debug(f"offer {id_otodom} — price in db (id {id_db}): {old_price}")

        if old_price == new_price:
            logging.info(f"offer {id_otodom} — price unchanged: {old_price}")
            return id_db, False, False
        else:
            logging.debug(f"offer {id_otodom} — price changed: {old_price} → {new_price}")
            return id_db, new_price, new_price_per_m
    except Exception as error:
        logging.exception(f"error checking if price changed: {error}")


def find_potentially_deleted_offers(fetched_offers: list[ListingBasic], city: str, cur) -> set:
    all_offers_from_db_query = """
        SELECT asl.id, asl.otodom_listing_id, asl.area
        FROM apartments_sale_listings asl
        JOIN locations l ON asl.location_id = l.id
        WHERE asl.active IS TRUE
        AND l.city = %s
        ;"""
    cur.execute(all_offers_from_db_query, (city.lower(),))
    all_offers_from_db = cur.fetchall()

    ids_from_otodom = {offer.listing_id for offer in fetched_offers}

    potentially_deleted = set()
    for id_db, id_otodom_from_db, area_from_db in all_offers_from_db:
        if id_otodom_from_db not in ids_from_otodom:
            potentially_deleted.add(id_db)

    logging.info(f"potentially deleted offers: {len(potentially_deleted)}")
    return potentially_deleted


def find_offer_links(potentially_deleted: set, cur) -> set:
    check_query = """
        SELECT offer_link
        FROM apartments_sale_listings
        WHERE id = %s
        ;"""
    links = set()
    for id_from_db in potentially_deleted:
        cur.execute(check_query, (id_from_db,))
        row = cur.fetchone()
        if row is None:
            logging.warning(f"no link found for offer {id_from_db}, skipping")
            continue
        links.add((id_from_db, row[0]))
        logging.debug(f"offer {id_from_db}: {row[0]}")
    return links
