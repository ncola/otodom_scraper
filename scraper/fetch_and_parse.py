import requests, cv2, json, time, random
from bs4 import BeautifulSoup
import numpy as np


def fetch_page(url: str) -> requests.Response:
    """
    Fetches the content of a webpage in Polish language.

    A delay (random sleep) is added between requests to avoid being blocked by the server due to
    making too many requests in a short period

    Args:
    url (str): The URL of the page to fetch.

    Returns:
    requests.Response: The HTTP response object containing the content of the page.

    If the request is successful (status code 200), it returns the response object.
    Otherwise, it prints an error message with the status code and returns None.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept-Language": "pl-PL,pl;q=0.9"
    }

    response = requests.get(url, headers=headers)
    time.sleep(random.uniform(1, 3))

    if response.status_code == 200:
        return response
    else: 
        print(f"Wystąpił błąd: {response.status_code}")
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
            raise Exception(f"Wystąpił błąd w pobraniu danych ze strony")
        
        soup = BeautifulSoup(html_response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if script_tag:
            json_data = json.loads(script_tag.string)

            #page_nb = json_data.get("page_nb", {})
            page_count = json_data.get("props", {}).get("pageProps", {}).get("tracking", {}).get("listing", {}).get("page_count", 0)
            #result_count = json_data.get("result_count", {})
            #results_per_page = json_data.get("results_per_page", {})
            
            print(f"Liczba stron wyszukiwania: {page_count}")
            return page_count
        
    except Exception as error:
        print(f"Error during getting total pages: {error}")


def download_data_from_search_results(base_url: str) -> list:
    """
    Extracts listing information from all paginated search result pages on otodom.com.

    This function iterates through all pages of search results starting from the given base URL,
    parses the embedded JSON data in each page's HTML, and collects basic information about 
    each listing (ID, area, price, and link).

    Args:
        base_url (str): The base search URL (without the `&page=` parameter).

    Returns:
        list: A list of dictionaries, each containing:
            - listing_id (int): Unique ID of the listing.
            - area (float): Area of the apartment in square meters.
            - price (int or str): Total price of the apartment.
            - link (str): URL to the individual listing.

    Raises:
        Exception: If the first page fails to load or parsing fails due to missing script tag.
    """
    all_offers = []

    response_first_page = fetch_page(base_url)
    if response_first_page is None:
        raise Exception("Nie udało się pobrać pierwszej strony wyszukiwania, sprawdź URL.")

    page_count = get_total_pages(response_first_page)
    
    page_count = 10 ########################### DO TESTOW, USUNAC

    for page in range(1, page_count):
        page_url = f"{base_url}&page={page}"
        print(f"Pobieranie strony {page} z {page_count}")
        
        response = fetch_page(page_url)
        if response is None:
            print(f"Nie udało się pobrać strony {page}.")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'}) 

        if not script_tag:
            print(f"Błąd przy stronie {page}: brak skryptu z danymi.")
            continue

        json_data = json.loads(script_tag.string)
        offers = json_data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])

        for offer in offers:
            listing_id = offer.get("id")
            area = offer.get("area", 0)
            total_price = offer.get("totalPrice", {})
            price = total_price.get("value", "Brak ceny") if isinstance(total_price, dict) else "Brak ceny"
            link = f"https://www.otodom.pl/pl/oferta/{offer.get('slug', 'Brak linku')}"
            
            all_offers.append({
                'listing_id': listing_id,
                'area': area,
                'price': price,
                'link': link
            })

    return all_offers


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
        for characteristic in characteristics:
            if characteristic["key"] == "building_ownership":
                ownership = characteristic.get("localizedValue", None) # ownership (Własność); cooperative_ownership (Spółdzielcze własnościowe prawo do lokalu); land_ownership (Własność gruntu); state_ownership (Własność państwowa); municipal_ownership (Własność komunalna)
            else:
                ownership = None
        
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