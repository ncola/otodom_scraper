from db.db_setup import get_db_connection
import datetime, logging

def check_location_table(cur, offer_data):
    location_query = """
        SELECT id FROM locations
        WHERE voivodeship = %s 
        AND city= %s 
        AND (district = %s OR (district IS NULL AND %s IS NULL))
        ;"""
    location_values = (offer_data['voivodeship'], 
                       offer_data['city'], 
                       offer_data['district'],
                       offer_data['district'])

    cur.execute(location_query, location_values)
    location_result = cur.fetchone()

    return location_result


def insert_into_locations_table(cur, offer_data):
    # sprawdzenie czy lokalizacja już istnieje w bazie danych
    location_result = check_location_table(cur, offer_data)
    # jezeli nie istnieje to dodajemy do tabeli
    if not location_result:
        location_values = (offer_data['voivodeship'], 
                           offer_data['city'], 
                           offer_data['district'])
        

        logging.debug(f"location {location_values} not in db, inserting")

        location_query = """
            INSERT INTO locations (voivodeship, city, district)
            VALUES (%s, %s, %s)
            RETURNING id
            ;"""

        cur.execute(location_query, location_values)
        new_id = cur.fetchone()[0]
        logging.debug(f"location inserted with id: {new_id}")

    else:
        logging.debug(f"location already in db under id: {location_result}")


created_offer_id = None

def insert_into_apartments_sale_listings_table(cur, offer_data):
    location_id = check_location_table(cur, offer_data)
    if location_id is None:
        raise ValueError(f"location not found in db for offer {offer_data.get('listing_id')} — make sure it was inserted first")
    listing_query = """
        INSERT INTO apartments_sale_listings (otodom_listing_id, title, market, advert_type, 
        creation_date, creation_time, pushed_up_at, exclusive_offer, creation_source, description_text, 
        area, price, updated_price, price_per_m, updated_price_per_m, location_id, street, rent_amount, 
        rooms_num, floor_num, heating, ownership, proper_type, construction_status, energy_certificate, 
        building_build_year, building_floors_num,  building_material, building_type, windows_type,  
        local_plan_url, video_url, view3d_url, walkaround_url, development_id, development_title, owner_id, owner_name, agency_id, 
        agency_name, offer_link, active, detected_inactive_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        ;"""
    
    listing_values = (offer_data['listing_id'],
                      offer_data['title'],
                      offer_data['market'],
                      offer_data['advert_type'],
                      offer_data['creation_date'],
                      offer_data['creation_time'],
                      offer_data['pushed_up_at'],
                      offer_data['exclusive_offer'],
                      offer_data['creation_source'],
                      offer_data['description_text'],
                      offer_data['area'],
                      offer_data['price'],
                      offer_data['price'],  # updated_price = price on first insert
                      offer_data['price_per_m'],
                      offer_data['price_per_m'],  # updated_price_per_m = price_per_m on first insert
                      location_id[0],
                      offer_data['street'],
                      offer_data['rent_amount'],
                      offer_data['rooms_num'],
                      offer_data['floor_num'],
                      offer_data['heating'],
                      offer_data['ownership'],
                      offer_data['proper_type'],
                      offer_data['construction_status'],
                      offer_data['energy_certificate'],
                      offer_data['building_build_year'],
                      offer_data['building_floors_num'],
                      offer_data['building_material'],
                      offer_data['building_type'],
                      offer_data['windows_type'],
                      offer_data['local_plan_url'],
                      offer_data['video_url'],
                      offer_data['view3d_url'],
                      offer_data['walkaround_url'],
                      offer_data['development_id'],
                      offer_data['development_title'],
                      offer_data['owner_id'],
                      offer_data['owner_name'],
                      offer_data['agency_id'],
                      offer_data['agency_name'],
                      offer_data['offer_link'],
                      offer_data['active'],
                      offer_data['detected_inactive_at'])
    
    cur.execute(listing_query, listing_values)
    
    created_offer_id = cur.fetchone()[0]

    return created_offer_id


def insert_into_features_table(cur, offer_data, id):
    features_query = """
        INSERT INTO features (listing_id, internet, cable_television, phone, roller_shutters, 
        anti_burglary_door, entryphone, monitoring, alarm, closed_area, furniture, washing_machine, 
        dishwasher, fridge, stove, oven, tv, balcony, usable_room, garage, basement, garden, terrace, 
        lift, two_storey, separate_kitchen, air_conditioning)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s)
        ;"""

    features_offer = list(offer_data['features'].split(' '))
    features_all_possibilities = ('internet', 'cable_television', 'phone', 'roller_shutters', 
                                  'anti_burglary_door', 'entryphone', 'monitoring', 'alarm', 
                                  'closed_area', 'furniture', 'washing_machine', 'dishwasher', 
                                  'fridge', 'stove', 'oven', 'tv', 'balcony', 'usable_room', 
                                  'garage', 'basement', 'garden', 'terrace', 'lift', 'two_storey', 
                                  'separate_kitchen', 'air_conditioning')
    
    features_bools=[feature in features_offer for feature in features_all_possibilities]
    
    features_values = (id, *features_bools)
    cur.execute(features_query, features_values)


def insert_into_photos_table(cur, offer_data, id):
    if offer_data["images"]:
        photos_query = """
            INSERT INTO photos (listing_id, photo)
            VALUES (%s, %s)
            ;"""    
        
        for photo in offer_data["images"]:
            photo_values = (id, photo)
            cur.execute(photos_query, photo_values)


def insert_new_listing(offer_data, conn, cur):
    try:
        # TABELA locations
        insert_into_locations_table(cur, offer_data)

        # TABELA apartments_sale_listings
        created_offer_id = insert_into_apartments_sale_listings_table(cur, offer_data)

        # TABELA features
        insert_into_features_table(cur, offer_data, created_offer_id)

        # TABELA photos
        #insert_into_photos_table(cur, offer_data, created_offer_id)

        logging.debug(f"offer saved to db with id: {created_offer_id}")

        conn.commit()

        return created_offer_id

    except Exception as error:
        conn.rollback()
        logging.exception(f"error inserting new listing: {error}")


def update_price_in_listings_table(offer_data, cur):
    try:
        id, new_price, new_price_per_m = offer_data

        update_price_query = """
            UPDATE apartments_sale_listings
            SET updated_price = %s, updated_price_per_m = %s
            WHERE id = %s
            ;"""

        update_price_values = (new_price, new_price_per_m, id)
        cur.execute(update_price_query, update_price_values)
        
        logging.debug(f"offer {id} price updated — new price: {new_price}, new price per m2: {new_price_per_m}")
    except Exception as error:
        logging.exception(f"error updating price in listings table: {error}")


def update_price_in_history_table(offer_data, cur):
    try:
        id, new_price, _ = offer_data

        change_date = datetime.date.today()

        old_price_query = """
            SELECT price
            FROM apartments_sale_listings
            WHERE id = %s
            ;"""
        cur.execute(old_price_query, (id,))
        old_price = cur.fetchone()[0]

        insert_history_query = """
            INSERT INTO price_history (listing_id, old_price, new_price, change_date)
            VALUES (%s, %s, %s, %s )
            RETURNING id
            ;"""
        
        update_history_values = (id, old_price, new_price, change_date)
        cur.execute(insert_history_query, update_history_values)
        id_history_table = cur.fetchone()[0]

        logging.debug(f"offer {id} price history saved with id: {id_history_table}")

    except Exception as error:
        logging.exception(f"error updating price history table: {error}")


def update_active_offers(offer_data, conn, cur):
    try:
        update_price_in_listings_table(offer_data, cur)
        update_price_in_history_table(offer_data, cur)

        conn.commit()
        
    except Exception as error:
        conn.rollback()
        logging.exception(f"error updating active offers: {error}")
 

def update_deleted_offers(offer_data, conn, cur):
    """" 
    offer_data = krotka(1. ID (nadane w bazie), 2. status ofert, które zostały usunięte z otodom)
    
    """
    try:

        update_inactive_query = """
        UPDATE apartments_sale_listings
        SET active = %s, detected_inactive_at = %s
        WHERE id = %s
        ;"""

        current_date = datetime.date.today()
        id_db = offer_data[0]
        update_inactive_values = (False, current_date, id_db)
        cur.execute(update_inactive_query, update_inactive_values)

        logging.debug(f"offer {id_db} marked as inactive on {current_date}")

        conn.commit()

    except Exception as error:
        logging.exception(f"error updating deleted offers: {error}")

