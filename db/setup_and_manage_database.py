import psycopg2, os
from dotenv import load_dotenv


# Wymagany zainstalowany PostgreSQL oraz utworzona baza apartments_for_sale
# Utworzenie bazy:
# psql -U postgres
# CREATE DATABASE apartments_for_sale;


# załadowanie zmiennych środowiskowych z .env
load_dotenv()


# ustawienie połączenia z bazą danych 
def get_db_connection():
    connection = None
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        print(f"Connected to database {os.getenv('DB_NAME')}")
        return connection
    except Exception as error:
        print(f"Error while connecting to database: {error}")
    return connection



# Utworzenie tabel

def create_tables():
    conn = None
    cur = None  
    try:
        conn = get_db_connection()
        if conn is None:
            print("Connection to the databas failed")
            return
    
        cur=conn.cursor()

        # Sprawdź czy tabele juz nie istnieja
        def check_table_exists(cur, table_name):
            checking_query = """
                SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s);
                """
            cur.execute(checking_query, (table_name,)) #argumenty w zapytaniach muszą byv przekazane jako krotka lub lista
            result = cur.fetchone()
            return result[0] # true/false
        
        tables = ['locations', 'apartments_sale_listings', 'price_history', 'photos', 'features']
        
        flag = any(check_table_exists(cur, name) for name in tables)
        if not flag:
            try:
                with open('db/schema.sql', 'r') as f:
                    sql_script = f.read()
                
                sql_commands = sql_script.split(";")
                for command in sql_commands:
                    if command.strip():
                        cur.execute(command.strip())
                
                conn.commit()
                print("Tables created")
            except Exception as error:
                print(f"Error during creating tables: {error}")

    except Exception as error:
        print(f"Error 2: {error}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    

def check_location_table(cur, offer_data):
    location_query = """
        SELECT id FROM locations
        WHERE voivodeship = %s AND city= %s AND district = %s;
        """
    location_values = (offer_data['voivodeship'], 
                       offer_data['city'], 
                       offer_data['district'])

    cur.execute(location_query, location_values)
    location_result = cur.fetchone()

    return location_result

def insert_into_locations(cur, offer_data):
    # sprawdzenie czy lokalizacja już istnieje w bazie danych
    location_result = check_location_table(cur, offer_data)
    # jezeli nie istnieje to dodajemy do tabeli
    if not location_result:
        print("Czy lokalizacja znajduje się juz w locations?: NIE")

        location_query = """
            INSERT INTO locations (voivodeship, city, district)
            VALUES (%s, %s, %s)
            RETURNING id;"""
        location_values = (offer_data['voivodeship'], 
                       offer_data['city'], 
                       offer_data['district'])
        
        cur.execute(location_query, location_values)
        new_id = cur.fetchone()[0]
        print(f"ID lokalizacji w tabeli locations: {new_id}")
        
    else:
        print("Czy lokalizacja znajduje się juz w locations?: TAK")

created_offer_id = None


def insert_into_apartments_sale_listings(cur, offer_data):
    location_id = check_location_table(cur, offer_data)
    listing_query = """
        INSERT INTO apartments_sale_listings (otodom_listing_id, title, market, advert_type, creation_date, creation_time, pushed_ap_at, exclusive_offer, creation_source, description_text, area, price, updated_price, price_per_m, updated_price_per_m, location_id, street, rent_amount, rooms_num, floor_num, heating, ownership, proper_type, construction_status, energy_certificate, building_build_year, building_floors_num,  building_material, building_type, windows_type,  local_plan_url, video_url, view3d_url, walkaround_url, owner_id, owner_name, agency_id, agency_name, offer_link, active, closing_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
    
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


def insert_into_features(cur, offer_data, id):
    
    features_query = """
        INSERT INTO features (listing_id, internet, cable_television, phone, roller_shutters, anti_burglary_door, entryphone, monitoring, alarm, closed_area, furniture, washing_machine, dishwasher, fridge, stove, oven, tv, balcony, usable_room, garage, basement, garden, terrace, lift, two_storey, separate_kitchen, air_conditioning)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    features_offer = list(offer_data['features'].split(' '))
    features_all_possibilities = ('internet', 'cable_television', 'phone', 'roller_shutters', 'anti_burglary_door', 'entryphone', 'monitoring', 'alarm', 'closed_area', 'furniture', 'washing_machine', 'dishwasher', 'fridge', 'stove', 'oven', 'tv', 'balcony', 'usable_room', 'garage', 'basement', 'garden', 'terrace', 'lift', 'two_storey', 'separate_kitchen', 'air_conditioning')
    
    features_bools=[feature in features_offer for feature in features_all_possibilities]
    
    features_values = (id, *features_bools)
        
    cur.execute(features_query, features_values)


def insert_into_photos(cur, offer_data, id):
    if offer_data["images"]:
        photos_query = """
            INSERT INTO photos (listing_id, photo)
            VALUES (%s, %s)"""    
        
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
        insert_into_locations(cur, offer_data)

        # TABELA apartments_sale_listings
        created_offer_id = insert_into_apartments_sale_listings(cur, offer_data)

        # TABELA features
        insert_into_features(cur, offer_data, created_offer_id)

        # TABELA photos
        insert_into_photos(cur, offer_data, created_offer_id)


        conn.commit()
        print("ZAKOŃCZONE")
    except Exception as error:
        print(f"Error 3: {error}")
    finally: 
        if conn:
            cur.close()
            conn.close()










