from db.db_setup import get_db_connection
import datetime


def check_location_table(cur, offer_data):
    location_query = """
        SELECT id FROM locations
        WHERE voivodeship = %s AND city= %s AND district = %s
        ;"""
    location_values = (offer_data['voivodeship'], 
                       offer_data['city'], 
                       offer_data['district'])

    cur.execute(location_query, location_values)
    location_result = cur.fetchone()

    return location_result


def insert_into_locations_table(cur, offer_data):
    # sprawdzenie czy lokalizacja już istnieje w bazie danych
    location_result = check_location_table(cur, offer_data)
    # jezeli nie istnieje to dodajemy do tabeli
    if not location_result:
        print("Czy lokalizacja znajduje się juz w locations?: NIE")

        location_query = """
            INSERT INTO locations (voivodeship, city, district)
            VALUES (%s, %s, %s)
            RETURNING id
            ;"""
        location_values = (offer_data['voivodeship'], 
                       offer_data['city'], 
                       offer_data['district'])
        
        cur.execute(location_query, location_values)
        new_id = cur.fetchone()[0]
        print(f"ID lokalizacji w tabeli locations: {new_id}")
        
    else:
        print("Czy lokalizacja znajduje się juz w locations?: TAK")

created_offer_id = None


def insert_into_apartments_sale_listings_table(cur, offer_data):
    location_id = check_location_table(cur, offer_data)
    listing_query = """
        INSERT INTO apartments_sale_listings (otodom_listing_id, title, market, advert_type, 
        creation_date, creation_time, pushed_ap_at, exclusive_offer, creation_source, description_text, 
        area, price, updated_price, price_per_m, updated_price_per_m, location_id, street, rent_amount, 
        rooms_num, floor_num, heating, ownership, proper_type, construction_status, energy_certificate, 
        building_build_year, building_floors_num,  building_material, building_type, windows_type,  
        local_plan_url, video_url, view3d_url, walkaround_url, owner_id, owner_name, agency_id, 
        agency_name, offer_link, active, closing_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        ;"""
    
    listing_values = (offer_data['listing_id'],
                      offer_data['title'],
                      offer_data['market'],
                      offer_data['advert_type'],
                      offer_data['creation_date'],
                      offer_data['creation_time'],
                      offer_data['pushed_ap_at'],
                      offer_data['exclusive_offer'],
                      offer_data['creation_source'],
                      offer_data['description_text'],
                      offer_data['area'],
                      offer_data['price'],
                      offer_data['price'], # przy pierwszym wprowadzeniu podajemy tą samą cene
                      offer_data['price_per_m'],
                      offer_data['price_per_m'], # przy pierwszym wprowadzeniu podajemy tą samą cene
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
                      offer_data['owner_id'],
                      offer_data['owner_name'],
                      offer_data['agency_id'],
                      offer_data['agency_name'],
                      offer_data['offer_link'],
                      offer_data['active'],
                      offer_data['closing_date'])
    
    cur.execute(listing_query, listing_values)
    
    created_offer_id = cur.fetchone()[0]
    print(f"ID oferty w tabeli apartments_sale_listings: {created_offer_id}")

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


def insert_new_listing(offer_data):
    conn = None
    cur = None  
    try:
        conn = get_db_connection()
        if conn is None:
            print("Connection to the databas failed")
            return
        cur=conn.cursor()

        # TABELA locations
        insert_into_locations_table(cur, offer_data)

        # TABELA apartments_sale_listings
        created_offer_id = insert_into_apartments_sale_listings_table(cur, offer_data)

        # TABELA features
        insert_into_features_table(cur, offer_data, created_offer_id)

        # TABELA photos
        insert_into_photos_table(cur, offer_data, created_offer_id)

        conn.commit()
        print("ZAKOŃCZONE")
    except Exception as error:
        print(f"Error 3: {error}")
    finally: 
        if conn:
            cur.close()
            conn.close()


def update_price_in_listings_table(data, cur):
    id = data.get("id")

    update_price_query = """
        UPDATE apartments_sale_listings
        SET updated_price = %s, updated_price_per_m = %s
        WHERE id = %s
        ;"""
    
    new_price = data.get("new_price")
    new_price_per_m = data.get("new_price_per_m")

    update_price_values = (new_price, new_price_per_m, id)
    cur.execute(update_price_query, update_price_values)
    
    print(f"UPDATE oferty {id}: nowa cena - {new_price}, nowa cena zam2 - {new_price_per_m}")


def update_price_in_history_table(data, cur):
    id = data.get("id")
    new_price = data.get("new_price")
    change_date = datetime.date.today()

    old_price_query = """
        SELECT price
        FROM apartments_sale_listings
        WHERE id = %s"""
    cur.execute(old_price_query, (id,))
    old_price = cur.fetchone()[0]

    insert_history_query = """
        INSERT INTO price_history (listing_id, old_price, new_price, change_date)
        VALUES (%s, %s, %s, %s )
        ;"""
    
    update_history_values = (id, old_price, new_price, change_date)
    cur.execute(insert_history_query, update_history_values)


def update_offers(data):
    conn = None
    cur = None  
    try:
        conn = get_db_connection()
        if conn is None:
            print("Connection to the databas failed")
            return
        cur=conn.cursor()

        update_price_in_listings_table(data, cur)
        update_price_in_history_table(data, cur)

        conn.commit()
        print("ZAKOŃCZONE")
    except Exception as error:
        print(f"Error 4: {error}")
    finally: 
        if conn:
            cur.close()
            conn.close()