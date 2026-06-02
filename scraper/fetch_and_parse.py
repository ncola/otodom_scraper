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
            logging.error(f"http error fetching page ({url}): {html_response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logging.exception(f"request exception while fetching page: {e}")
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
            logging.error("response is None, can't get total pages")
            raise Exception("response is None, can't get total pages")

        soup = BeautifulSoup(html_response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if script_tag:
            json_data = json.loads(script_tag.string)
            page_count = json_data.get("props", {}).get("pageProps", {}).get("tracking", {}).get("listing", {}).get("page_count", 0)
            return page_count

        logging.warning("could not find __NEXT_DATA__ script tag for page count")
        return 0

    except Exception as error:
        logging.exception(f"error getting total pages: {error}")


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
            - development_id (int): id of the specific investment to which the offer belongs or None

    Raises:
        Exception: If the first page fails to load, or if parsing fails due to a missing or incorrect script tag
        ValueError: If the script tag does not contain the expected data structure
    """
    try:
        all_offers = []

        response_first_page = fetch_page(base_url)
        if response_first_page is None:
            logging.error("failed to fetch first search page — check the URL")
            raise Exception("failed to fetch first search page — check the URL")

        page_count = get_total_pages(response_first_page)
        logging.info(f"total pages found: {page_count}")
        
        for page in range(1, page_count+1):
            page_url = f"{base_url}&page={page}"
            percent = int((page / page_count) * 100)
            logging.info(f"page {page}/{page_count} ({percent}%)")
            logging.debug(f"url: {page_url}")
            
            html_response = fetch_page(page_url)
            if html_response is None:
                logging.warning(f"failed to fetch page {page}, skipping")
                continue

            soup = BeautifulSoup(html_response.text, 'html.parser')
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

            if not script_tag:
                logging.warning(f"no __NEXT_DATA__ script tag on page {page}, skipping")
                continue

            json_data = json.loads(script_tag.string)
            #all_offers_id_on_page = json_data.get("props", {}).get("pageProps", {}).get("tracking", {}).get("listing", {}).get("ad_impressions", [])

            offers = json_data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])
            
            #print("Struktura JSON (od props/pageProps/ad):", json.dumps(offers, indent=7)[:420000])

            if not offers:
                logging.warning(f"no offers found on page {page}, skipping")
                continue
            
            #logging.debug(f"Liczba znalezionych ofert na stronie {page}: {len(offers)}")

            n=1
            for offer in offers: 
                try:
                    listing_id = offer.get("id")
                    area = round(float(offer.get("areaInSquareMeters", 0)),2)
                    if area is not None:
                        area = round(float(area),2)
                    else:
                        area = 0
                    total_price = offer.get("totalPrice", {})
                    price = total_price.get("value", None) if isinstance(total_price, dict) else None
                    ppm_data = offer.get("pricePerSquareMeter", {})
                    if ppm_data:
                        price_per_m = ppm_data.get("value", None)
                    else: 
                        price_per_m = None
                    link = f"https://www.otodom.pl/pl/oferta/{offer.get('slug', None)}"

                    #sprawdz czy nie ma wiecej ofert (powiazanych)
                    other_offers = offer.get("relatedAds", None) 
                    if other_offers:
                        logging.debug(f"{n}. developer offer {listing_id} has related ads — area: {area}, price: {price}, price_per_m: {price_per_m}")
                        for related_offer in other_offers:
                            id = related_offer.get("id")
                            area = related_offer.get("areaInSquareMeters", 0)
                            if area is not None:
                                area = round(float(area),2)
                            else:
                                area = 0
                            total_price = related_offer.get("totalPrice", {})
                            price = total_price.get("value", None) if isinstance(total_price, dict) else None
                            ppm_data = related_offer.get("pricePerSquareMeter", {})
                            if ppm_data:
                                price_per_m = ppm_data.get("value", None)
                            else: 
                                price_per_m = None
                            link = f"https://www.otodom.pl/pl/oferta/{related_offer.get('slug', None)}"

                            logging.debug(f"{n}. related offer {id} — area: {area}, price: {price}, price_per_m: {price_per_m}")
                            all_offers.append({
                                'listing_id': id,
                                'area': area,
                                'price': price,
                                'price_per_m': price_per_m,
                                'link': link
                            })
                            n+=1

                    else:
                        logging.debug(f"{n}. offer {listing_id} — area: {area}, price: {price}, price_per_m: {price_per_m}")

                        all_offers.append({
                            'listing_id': listing_id,
                            'area': area,
                            'price': price,
                            'price_per_m': price_per_m,
                            'link': link
                        })
                    n+=1
                except Exception as error:
                    logging.exception(f"skipped offer {n} on page {page} (id: {listing_id}): {error}")

            logging.debug(f"offers found on page {page}: {n-1}")

        logging.debug(all_offers)
        return all_offers

    except Exception as error:
        logging.exception(f"error downloading data from search results: {error}")
        

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
            logging.debug(f"offer {id} (area: {area}) not in db — will fetch")
            return False
        else:
            logging.debug(f"offer {id} (area: {area}) already in db under id: {result}")
            return True

    except Exception as error:
        logging.exception(f"error checking if offer exists in db: {error}")
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

    ids_from_otodom = {offer_dict["listing_id"] for offer_dict in fetched_all_data_from_otodom}

    potentially_deleted = set()
    for id_db, id_otodom_from_db, area_from_db in all_offers_from_db:
        if id_otodom_from_db not in ids_from_otodom:
            potentially_deleted.add(id_db)

    logging.debug("*"*100)
    logging.debug(f"offers in db: {all_offers_from_db}")
    logging.debug(f"offer ids from otodom: {ids_from_otodom}")
    logging.debug("*"*100)
    logging.info(f"potentially deleted offers: {len(potentially_deleted)}")

    return potentially_deleted


def find_offer_link(potentially_deleted_data: set, cur) -> set:
    """
    Retrieves the links for potentially deleted offers

    Args:
        potentially_deleted_data (set): A set of potentially deleted offer IDs (from find_potentially_deleted_offers())
        cur (cursor): Database cursor to execute SQL queries

    Returns:
        set: A set of tuples containing (offer_id_from_db, offer_link)

    """
    logging.debug("fetching links for potentially deleted offers")

    if not potentially_deleted_data:
        return set()

    cur.execute(
        "SELECT id, offer_link FROM apartments_sale_listings WHERE id = ANY(%s)",
        (list(potentially_deleted_data),)
    )
    rows = cur.fetchall()

    links = set()
    fetched_ids = set()
    for id_from_db, offer_link in rows:
        fetched_ids.add(id_from_db)
        if offer_link is None:
            logging.warning(f"no link found for offer {id_from_db}, skipping")
            continue
        links.add((id_from_db, offer_link))
        logging.debug(f"offer {id_from_db}: {offer_link}")

    for id_from_db in potentially_deleted_data - fetched_ids:
        logging.warning(f"no link found for offer {id_from_db}, skipping")

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
        return "removed"


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
        logging.debug("checking status of each potentially deleted offer")
        for id_from_db, offer_link in potentially_deleted_links:
            logging.debug(f"checking offer {id_from_db}: {offer_link}")
            status = get_offer_status(offer_link)
            logging.debug(f"offer {id_from_db} status: {status}")
            if status is None or 'active' not in status:
                deleted_offers.add((id_from_db, status))
                logging.debug(f"offer {id_from_db} added to deleted list")

        logging.info(f"offers removed from otodom: {len(deleted_offers)}")
        return deleted_offers

    except Exception as error:
        logging.exception(f"error finding closed offers: {error}")


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
        raise Exception("html response is None, can't parse listing page")
    
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
        district = None
        for data in reverseGeocoding_locations:
            if data.get("locationLevel") == "district":
                district = data.get("name")
                break

        # zdjęcia
        #images = []
        #images_html = offer_data.get("images", None)
        #logging.debug(f"Znaleziono {len(images_html)} zdjęć dla oferty")
        #for n, element in enumerate(images_html, start=1):
        #    try:
        #        image_link = element.get("medium", None)
        #        image_response = fetch_page(image_link)
        #        arr = np.asarray(bytearray(image_response.content), dtype=np.uint8)
        #        img = cv2.imdecode(arr, cv2.IMREAD_COLOR) # konwersja na obraz
        #        success, encoded_image = cv2.imencode('.jpg', img) # zakodowanie obrazu na dane binarne jpg (na potrzeby postgreSQL)
        #        if success:
        #            binary_image = encoded_image.tobytes() 
        #            images.append(binary_image)
        #    except Exception as e:
        #        logging.warning(f"Error during fetching image {n} in listing {listing_id} ({image_link}): {e}")

        # linki
        links = (offer_data.get("links", {}))
        local_plan_url = (links.get("localPlanUrl", None))
        video_url = (links.get("videoUrl", None))
        view3d_url = (links.get("view3dUrl", None))
        walkaround_url = (links.get("walkaroundUrl", None))
        
        # sprzedający
        development_id = offer_data.get("developmentId", None)
        development_title = offer_data.get("developmentTitle", None)
        seller = offer_data.get("owner", {})
        owner_id = seller.get("id", None)
        owner_name = seller.get("name", None)

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
        data["pushed_up_at"] = promoted_at
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
        #data["images"] = images
        
        # sprzedajacy
        data["development_id"] = development_id
        data["development_title"] = development_title or None
        data["owner_id"] = None if owner_id == 0 else owner_id
        data["owner_name"] = owner_name
        data["agency_id"] = agency_id
        data["agency_name"] = agency_name

        # link do oferty
        data["offer_link"] = f"https://www.otodom.pl/pl/oferta/{offer_data.get('slug', '')}"

        data['active'] = True

        return data



#page = fetch_page("https://www.otodom.pl/pl/oferta/mieszkanie-2-pokojowe-48m2-katowice-koszutka-ID4wQpd")
#page = fetch_page("https://www.otodom.pl/pl/oferta/dwupoziomowe-z-duzym-ogrodkiem-4-pokoje-ID4wyXG")
#print(download_data_from_listing_page(page))