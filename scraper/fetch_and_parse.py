import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests, cv2, json, time, random, logging
from bs4 import BeautifulSoup
import numpy as np
from db.db_operations import get_db_connection


def fetch_page(url: str) -> requests.Response:
    """
    Fetches the content of a webpage.

    A delay (random sleep) is added between requests to avoid being blocked by the server due to
    making too many requests in a short period

    The function sends a GET request to the specified URL with custom headers (including Polish 
    language preference). If the request is successful (status code 200), the response object 
    is returned. If the request fails (e.g. returns an error status code or raises a network-related 
    exception), an error message is printed and None is returned

    Args:
    url (str): The URL of the page to fetch

    Returns:
    requests.Response: The HTTP response object containing the content of the page

    If the request is successful (status code 200), it returns the response object.
    Otherwise, it prints an error message with the status code and returns None.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept-Language": "pl-PL,pl;q=0.9"
        }

        html_response = requests.get(url, headers=headers)
        time.sleep(random.uniform(1,2))

        if html_response.status_code == 200:
            return html_response
        else: 
            logging.error(f"Błąd HTTP podczas pobierania strony ({url}): {html_response.status_code}")
            return None
        
    except requests.exceptions.RequestException as e:
        logging.exception(f"Wyjątek przy pobieraniu strony: {e}")
        return None


def get_total_pages(html_response: requests.Response) ->int:
    """
    Parses the total number of search result pages from the HTML response of the first Otodom search page.

    This function is used internally by download_data_from_searching_page() to determine how many
    pages of listings are available for scraping. 

    Args:
        html_response (requests.Response): The HTTP response object from the first search result page

    Returns:
        int: The total number of pages available for the search. Returns 0 if parsing fails or data is missing

    Raises:
        Exception: If the response is None
    """
    try:
        if html_response is None:
            logging.error("Wystąpił błąd w pobraniu danych ze strony")
            raise Exception(f"Wystąpił błąd w pobraniu danych ze strony")
        
        soup = BeautifulSoup(html_response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if script_tag:
            json_data = json.loads(script_tag.string)

            page_count = json_data.get("props", {}).get("pageProps", {}).get("tracking", {}).get("listing", {}).get("page_count", 0)
            #result_count = json_data.get("result_count", {})
            #results_per_page = json_data.get("results_per_page", {})
            
            return page_count
        
        logging.warning("Nie udało się znaleźć tagu z danymi dla liczby stron")
        return 0
        
    except Exception as error:
        logging.exception(f"Error during getting total pages: {error}")


def download_data_from_search_results(base_url: str) -> list:
    """
    Extracts listing information from all paginated search result pages on otodom.com.

    This function iterates through all pages of search results starting from the given base URL,
    parses the embedded JSON data in each page's HTML, and collects basic information about 
    each listing (ID, area, price, price_per_m and link)

    Args:
        base_url (str): The base search URL (without the `&page=` parameter)

    Returns:
        list: A list of dictionaries, each containing:
            - listing_id (int): listing ID from otodom or None
            - area (float): area of the apartment in m2 or 0
            - price (int): Total price  or None
            - price_per_m (float): Price per m2 or None
            - link (str): URL to the individual listing or None

    Raises:
        Exception: If the first page fails to load, or if parsing fails due to a missing or incorrect script tag
        ValueError: If the script tag does not contain the expected data structure
    """
    try:
        all_offers = []

        response_first_page = fetch_page(base_url)
        if response_first_page is None:
            logging.error("Nie udało się pobrać pierwszej strony wyszukiwania, sprawdź URL")
            raise Exception("Nie udało się pobrać pierwszej strony wyszukiwania, sprawdź URL")

        page_count = get_total_pages(response_first_page)
        logging.info(f"Liczba znalezionych stron: {page_count}")
        
        for page in range(1, page_count+1):
            page_url = f"{base_url}&page={page}"
            logging.debug(f"Pobieranie strony {page} z {page_count} ({page_url})")
            
            html_response = fetch_page(page_url)
            if html_response is None:
                logging.exception(f"Nie udało się pobrać strony {page}.")
                continue

            soup = BeautifulSoup(html_response.text, 'html.parser')
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'}) 

            if not script_tag:
                logging.exception(f"Błąd przy stronie {page}: brak skryptu z danymi")
                continue

            json_data = json.loads(script_tag.string)
            offers = json_data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])
            
            if not offers:
                logging.exception(f"Brak ofert na stronie {page} url {base_url}")
                continue
            
            logging.debug(f"Liczba znalezionych ofert na stronie {page}: {len(offers)}")

            n=1
            for offer in offers: 
                # sprawdz czy nie jest to zbiorowe ogloszenie do ktorego nie mam obslugi

                listing_id = offer.get("id")
                area = round(float(offer.get("areaInSquareMeters", 0)),2)
                total_price = offer.get("totalPrice", {})
                price = total_price.get("value", None) if isinstance(total_price, dict) else None
                ppm_data = offer.get("pricePerSquareMeter", {})
                if ppm_data:
                    price_per_m = ppm_data.get("value", None)
                else: 
                    price_per_m = None
                
                link = f"https://www.otodom.pl/pl/oferta/{offer.get('slug', None)}"

                logging.debug(f"{n}.id oferty z searching page: {listing_id}, area: {area}, price: {price}, price_per_m: {price_per_m}, link: {link}")

                all_offers.append({
                    'listing_id': listing_id,
                    'area': area,
                    'price': price,
                    'price_per_m': price_per_m,
                    'link': link
                })
                n+=1

        return all_offers

    except Exception as error:
        logging.exception(f"Error during downloading data from search result: {error}")
        

def check_if_offer_exists(fetched_all_data_from_otodom: dict, cur) -> bool:
    """
    Checks whether a property listing already exists in the database based on its ID and area

    Args:
        fetched_all_data_from_otodom (dict): A dictionary containing offer data from Otodom, including 
                                        'listing_id', 'area', 'price', 'price_per_m', 'link' 
                                        (single entry from download_data_from_search_results())
        cur (cursor): Database cursor to execute queries

    Returns:
        bool: True if the offer already exists in the database, False otherwise. If an error occurs during the query,
        the function returns None
    
    Raises:
        Exception: If database query fails or any other error occurs
    """
    try:
        if_exists_query = """
            SELECT id
            FROM apartments_sale_listings
            WHERE otodom_listing_id = %s AND area = %s
            LIMIT 1
            ;"""
        
        id = fetched_all_data_from_otodom.get('listing_id')
        area = fetched_all_data_from_otodom.get('area')
        if_exists_values = (id, area)

        cur.execute(if_exists_query, if_exists_values)
        result = cur.fetchone()
        if result is None:
            logging.debug(f"Oferta {id} o metrazu {area} NIE istnieje w bazie (result: {result}), pobieramy")
            return False
        else:
            logging.debug(f"Oferta {id} o metrazu {area} istnieje w bazie pod id: {result} , sprawdzamy dalej !!!!!!!!!!!!!!!!!!!!!!!!!!")
            return True

    except Exception as error:
        logging.exception(f"Error during checking if record exists in database: {error}")
        return None


def check_if_price_changed(fetched_data_from_otodom: dict, cur) -> tuple:
    """
    Checks if the price of a given offer (that is already in database) has changed

    Args:
        fetched_data_from_otodom (dict): A dictionary containing offer data from Otodom, including 
                                        'listing_id' and 'area', 'price', 'price_per_m', 'link' 
                                        (single entry from download_data_from_search_results())
        cur (cursor): Database cursor to execute SQL queries

    Returns:
        tuple: (id, bool) - id (the one from db) of the listing and a boolean indicating if the price 
        has changed
    """
    try:
        if_changed_query = """
            SELECT id, updated_price
            FROM apartments_sale_listings
            WHERE otodom_listing_id = %s
            ;"""

        id_otodom = fetched_data_from_otodom.get('listing_id')
        new_price = fetched_data_from_otodom.get('price')
        new_price_per_m = fetched_data_from_otodom.get('price_per_m')
        if_changed_values = (id_otodom, )
        cur.execute(if_changed_query, if_changed_values)
        result = cur.fetchone()
        id_db, old_price = result

        logging.debug(f"    Dla id {id_otodom}  na stronie cena wynosi {new_price} (cena za m2: {new_price_per_m})")
        logging.debug(f"    W bazie ta oferta ma id: {id_db}  ma zapisaną cenę {old_price}\n")
        
        if old_price == new_price:
            logging.info(f"W ofercie {id_otodom} ({id_db}) cena {old_price} jest aktualna")
            return id_db, False
        else:
            logging.debug(f"W ofercie {id_otodom} ({id_db}) cena {old_price} jest nieaktualna, nowa cena wynosi {new_price}\n")
            return id_db, new_price

    except Exception as error:
        logging.exception(f"Error during checking if price changed: {error}")
        

def find_potentially_deleted_offers(fetched_all_data_from_otodom: list, city:str, cur) -> set: 
    """
    Checks if all active offers (from the same city which used in searching) from the database exist 
    in the current set of fetched offers.
    Will be used in check_offer_status()

    Args:
        fetched_all_data_from_otodom (list): List of dictionaries containing offer data from Otodom, including 
                                        'listing_id' and 'area', 'price', 'price_per_m', 'link' 
                                        (all data from download_data_from_search_results())
        city (str): City for which we are looking for apartments for sale
        cur (cursor): Database cursor to execute SQL queries

    Returns:
        set: A set of potentially deleted offer IDs from the database
    """
    
    # Sprawdź, czy wszytskie aktywne ID z bazy znajdują się w swiezo zebranych danych z całego wyszukiwania z danego miasta
    all_offers_from_db_query = """
        SELECT asl.id, asl.otodom_listing_id, asl.area
        FROM apartments_sale_listings asl
        JOIN locations l ON asl.location_id = l.id
        WHERE asl.active IS TRUE
        AND l.city = %s
        ;"""
    
    cur.execute(all_offers_from_db_query, (city.lower(),))
    all_offers_from_db = cur.fetchall()

    data_from_otodom = set()
    for offer_dict in  fetched_all_data_from_otodom:
        id_otodom_from_otodom= offer_dict["listing_id"]
        area_from_otodom = offer_dict["area"]
        offer_data = (id_otodom_from_otodom, area_from_otodom)
        data_from_otodom.add(offer_data)

    potentially_deleted = set()
    for id_db, id_otodom_from_db, area_from_db in all_offers_from_db:
        if (id_otodom_from_db, area_from_db) not in data_from_otodom:
            potentially_deleted.add(id_db)

    logging.debug("*"*100)
    logging.debug(f"Oferty z bazy danych: \n {all_offers_from_db}\n")
    logging.debug(f"Oferty z otodom: \n {data_from_otodom}")
    logging.debug("*"*100)

    logging.info(f"Potencjalnie {len(potentially_deleted)} usuniętych ofert")

    return potentially_deleted # set ID (to przypisane w bazie, a nie to z otodom), które mogły zostać usunięte (są w bazie danych ale nie ma ich w  pobranych danych z wyszukiwania)


def find_offer_link(potentially_deleted_data: set, cur) -> set:
    """
    Retrieves the links for potentially deleted offers

    Args:
        potentially_deleted_data (set): A set of potentially deleted offer IDs (from find_potentially_deleted_offers())
        cur (cursor): Database cursor to execute SQL queries

    Returns:
        set: A set of tuples containing (offer_id_from_db, offer_link)

    """
    check_potentially_deleted_query = """
        SELECT offer_link
        FROM apartments_sale_listings
        WHERE id = %s
        ;"""

    logging.debug("Potencjalnie usunięte oferty:")
    links=set()
    for id_from_db in potentially_deleted_data:
        
        cur.execute(check_potentially_deleted_query, (id_from_db,))
        offer_link = cur.fetchone()[0]

        links.add((id_from_db, offer_link))

        logging.debug(f"{id_from_db}: {offer_link}")

    return links 


def get_offer_status(offer_link: str) ->str:
    """
    Checks the status of a given offer on Otodom

    Args:
        offer_link (str): The URL of the offer to check

    Returns:
        str: The status of the offer (e.g., "active", "removed")
    """
    try:
        html_response = fetch_page(offer_link)
        if html_response is None:
            return "removed"

        soup = BeautifulSoup(html_response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'}) 
        if script_tag: 
            json_data = json.loads(script_tag.string)
            status = json_data.get("props", {}).get("pageProps", {}).get("ad", {}).get("status", None)
            
            return status
        
    except Exception as error:
        logging.exception(f"Error during getting offer status: {error}")


def find_closed_offers(data:list, city:str, cur) ->set:
    """
    Finds the offers that have been closed or removed

    Args:
        data (list): List of dictionaries containing offer data from Otodom, including 'listing_id', 
        'area', 'price', 'price_per_m', 'link' (all data from download_data_from_search_results())
        city (str): City for which we are looking for apartments for sale 
        cur (cursor): Database cursor to execute SQL queries

    Returns:
        set: A set of tuples containing (offer_id_from_db, offer_status) for closed offers
    """

    try:
        # 1. Na podstawie bazy i pobranych wlasnie danych z wyszukiwania otodom okreslamy ID ofert ktore mogly zostac usuniete
        potentially_deleted = find_potentially_deleted_offers(data, city, cur) #set (1. ID do sprawdzenia czy są aktywne)
        
        # 2. Do setu ID potencjalnie usunietcyh ofert dodajemy ich linki
        potentially_deleted_links = find_offer_link(potentially_deleted, cur)  # set krotek (1. id (to nadane w bazie) potecnjalnie usunietych z otodom ofert, 2. link do oferty)
        
        # 3. Wchodzimy w kazdy link i sprawdzamy status oferty
        deleted_offers = set()
        logging.debug("Sprawdzamy kazda potencjalnie usuniętą ofertę: \n")
        for id_from_db, offer_link in potentially_deleted_links:
            logging.debug(f"Oferta {id_from_db}, link: {offer_link}")
            status = get_offer_status(offer_link)
            logging.debug(f"status: {status}")
            if 'active' not in status:
                deleted_offers.add((id_from_db, status))
                logging.debug("Dodano do listy usuniętych ofert")
        logging.debug("\n")   

        logging.info(f"Oferty usunięte z otodom: {deleted_offers}")
        return deleted_offers #set krotek(1. ID (nadane w bazie), 2. status ofert, które zostały usunięte z otodom)

    except Exception as error:
        logging.exception(f"Error during finding closed offers: {error}")



def categorize_offers_for_db(offers_data): #jednak NOT IN USE LEFT IN CASE
    """ 
    przyjmuje all_offers z download_data_from_search_results() i dzieli je na:
    - takie, których nie ma w bazie ( -> przesyłane do scrape_all_pages() (bd trzeba zmienic nazwe))
    - takie, które są w bazie ale zmienila sie cena ( -> -> przesyłane do update_offers() )
    - takie, z którymi nie trzeba nic robić, ich nie zapisujemy bo nei sa potrzebne ( -> zostawiamy)
    """
    conn = None
    cur = None  
    try:
        conn = get_db_connection()
        if conn is None:
            print("Connection to the databas failed")
            return
        cur=conn.cursor()
        need_update_offers = []
        new_offers = []

        n=0
        for offer in offers_data:
            if not check_if_offer_exists(offer, cur):
                new_offers.append(offer)
            else:
                id, new_price, new_price_per_m = check_if_price_changed(offer, cur)
                if new_price: #jezeli jest to jakas liczba
                    need_update_offers.append({"id": id, "new_price": new_price, "new_price_per_m": new_price_per_m})
                if not new_price:
                    n+=1
        print(f"\nZnaleziono {n} ofert, które juz są w bazie i nie wymagają update ceny")

        # new_offers to lista słowników tak jak w danych wejściwoych czyli wynik download_data_from_search_results()
        # need_update_offers to lista słowników z wartsociami id, new_price i new_price_per_m

        return need_update_offers, new_offers
    except Exception as error:
        print(f"Error during categorizing offers for db: {error}")
    finally: 
        if conn:
            cur.close()
            conn.close()


def download_data_from_listing_page(html_response:requests.Response) -> dict:
    """
    Parses the HTML response, extracts the property listing data embedded in a JSON object 
    within a <script> tag, and returns it as a dictionary.

    Parameters:
        html_response (requests.Response): The HTTP response containing the HTML of the page 
        to be parsed.

    Returns:
        dict: A dictionary containing the extracted property listing data, such as title, price, 
        location, features, images, etc.

    Raises:
        Exception: If the HTML response does not contain the necessary data or is invalid.
    """
    if html_response is None:
        raise Exception(f"Wystąpił błąd w pobraniu danych ze strony")
    
    soup = BeautifulSoup(html_response.text, 'html.parser')
    script_tag = soup.find('script', {'id':'__NEXT_DATA__'})

    if script_tag:
        json_data = json.loads(script_tag.string)

        offer_data = json_data.get("props", {}).get("pageProps", {}).get("ad", {})

        # Debug: wydrukowanie tylko tej części JSON, zaczynając od ...
        #print("Struktura JSON (od props/pageProps/ad):", json.dumps(offer_data, indent=7)[:420000])

        listing_id = offer_data.get("id", None)
        listing_title = offer_data.get("title", None)
        listing_title = BeautifulSoup(listing_title, "html.parser").get_text()
        market_type = str(offer_data.get("market", None)).lower()
        advertisement_type = str(offer_data.get("advertType", None)).lower()
        creation_date = offer_data.get("createdAt", None)
        description = offer_data.get("description", None)
        description_text = BeautifulSoup(description, "html.parser").get_text()
        is_exclusive_offer = offer_data.get("exclusiveOffer", None) # True/False
        creation_source = str(offer_data.get("creationSource", None))
        promoted_at = offer_data.get("pushedUpAt", None)
        heating_type = str(offer_data.get("property", {}).get("buildingProperties", {}).get("heating", None)).lower()

        target = offer_data.get("target", {})
        # Cechy
        features_equipment = target.get("Equipment_types", None)
        features_additional_information = target.get("Extras_types", None)
        features_utilities = target.get("Media_types", None)

        area = target.get("Area", None)
        building_build_year = target.get("Build_year", None)
        building_floors_count = target.get("Building_floors_num", None)
        building_material = str(target.get("Building_material", None))

        characteristics = offer_data.get("characteristics", {})        
        ownership = None
        for characteristic in characteristics:
            if characteristic["key"] == "building_ownership":
                ownership = characteristic.get("localizedValue", None)  # ownership (Własność); cooperative_ownership (Spółdzielcze własnościowe prawo do lokalu); land_ownership (Własność gruntu); state_ownership (Własność państwowa); municipal_ownership (Własność komunalna)
                break

        building_type = str(target.get("Building_type", None))
        energy_certificate = target.get("Energy_certificate", None)        
        city = target.get("City", None)
        voivodeship = target.get("Province", None)
        construction_status = str(target.get("Construction_status", None)) #under_construction; completed; planned; ready_for_occupancy
        floor_num = str(target.get("Floor_no", None))
        price = target.get("Price", None)
        price_per_m = target.get("Price_per_m", None)
        proper_type = target.get("ProperType", None) #Mieszkanie; Dom; Działka; Komercyjna; Inny
        rent = target.get("Rent", None) #czasem ludzie wpisują '0' a czasem jest puste pole
        windows_type = str(target.get("Windows_type", None))
        security_types = str(target.get("Security_types", None))
        if isinstance(security_types, list):
            security_types = ', '.join(data for data in security_types)
        rooms_num = str(target.get("Rooms_num", None))
        
        location_data = offer_data.get("location", {}).get("address", {})
        if location_data:
            street = location_data.get("street", {}).get("name", None) if location_data.get("street") else None
        else:
            street = None
        
        reverseGeocoding_locations = offer_data.get("location", {}).get("reverseGeocoding", {}).get("locations", [])
        for data in reverseGeocoding_locations:
            district = data.get("name") if data.get("locationLevel") == "district" else None

        # Zdjęcia
        images = []
        images_html = offer_data.get("images", None)
        for element in images_html:
            image_link = element.get("medium", None)
            image_response = fetch_page(image_link)
            arr = np.asarray(bytearray(image_response.content), dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR) # konwersja na obraz
            success, encoded_image = cv2.imencode('.jpg', img) # zakodowanie obrazu na dane binarne jpg (na potrzeby postgreSQL)
            if success:
                binary_image = encoded_image.tobytes() 
                images.append(binary_image)

        # linki
        links = (offer_data.get("links", {}))
        local_plan_url = (links.get("localPlanUrl", None))
        video_url = (links.get("videoUrl", None))
        view3d_url = (links.get("view3dUrl", None))
        walkaround_url = (links.get("walkaroundUrl", None))
        
        # sprzedający
        seller = (offer_data.get("owner", {}))
        owner_id = (seller.get("id", None))
        owner_name = (seller.get("name", None))

        agency = (offer_data.get("agency", {}))
        if agency:
            agency_id = (agency.get("id", None))
            agency_name = (agency.get("name", None))
        else:
            agency_id = None
            agency_name = None

        # podstawowe informacje o ofercie
        data = {}
        data["listing_id"] = listing_id
        data["title"] = listing_title
        data["market"] = market_type
        data["advert_type"] = advertisement_type
        data["creation_date"] = creation_date
        data["pushed_ap_at"] = promoted_at
        data["exclusive_offer"] = is_exclusive_offer
        data["creation_source"] = creation_source

        #cechy mieszkania
        data["description_text"] = description_text
        data["area"] = area
        data["price"] = price
        data["price_per_m"] = price_per_m
        data["rent_amount"] = rent
        data["rooms_num"] = rooms_num
        data["floor_num"] = floor_num
        data["heating"] = heating_type
        data["ownership"] = ownership
        data["proper_type"] = proper_type
        data["construction_status"] = construction_status
        data["features_utilities"] = features_utilities
        data["features_equipment"] = features_equipment
        data["features_additional_information"] = features_additional_information
        data["energy_certificate"] = energy_certificate

        # lokalizacja
        data["voivodeship"] = voivodeship
        data["city"] = city
        data["district"] = district
        data["street"] = street

        # szczegółu budynku
        data["building_build_year"] = building_build_year
        data["building_floors_num"] = building_floors_count
        data["building_material"] = building_material
        data["building_type"] = building_type
        data["windows_type"] = windows_type
        data["security_types"] = security_types

        # linki
        data["local_plan_url"] = local_plan_url
        data["video_url"] = video_url
        data["view3d_url"] = view3d_url
        data["walkaround_url"] = walkaround_url

        # zdjęcia
        data["images"] = images
        
        # Sprzedajacy
        data["owner_id"] = owner_id
        data["owner_name"] = owner_name
        data["agency_id"] = agency_id
        data["agency_name"] = agency_name

        # Link do oferty
        data["offer_link"] = f"https://www.otodom.pl/pl/oferta/{offer_data.get('slug', '')}"

        data['active'] = True

        return data



#page = fetch_page("https://www.otodom.pl/pl/oferta/mieszkanie-2-pokojowe-48m2-katowice-koszutka-ID4wQpd")
#page = fetch_page("https://www.otodom.pl/pl/oferta/dwupoziomowe-z-duzym-ogrodkiem-4-pokoje-ID4wyXG")
#print(download_data_from_listing_page(page))