import pandas as pd
import requests, cv2, json, re
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime


# DOWNLOAD DATA

def fetch_page(url: str) -> requests.Response:
    """
    Fetches the content of a webpage in Polish language.

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

    #pobranie strony
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response
    else: 
        print(f"Wystąpił błąd: {response.status_code}")
        return None


def download_data_from_searching_page(html_response: requests.Response) -> list:
    """
    Extracts listing information from the HTML content of a search results page (for otodom.com).

    Args:
    html_response (requests.Response): The response object containing the HTML content of the page.

    Returns:
    list: A list of dictionaries, each containing details about an offer such as title, price, link, 
          date of creation, and the first creation date.

    Raises:
    Exception: If the HTML response is None or if there is an issue parsing the data from the page.
    """
    if html_response is None:
        raise Exception(f"Wystąpił błąd w pobraniu danych ze strony")
    
    soup = BeautifulSoup(html_response.text, 'html.parser')
    script_tag = soup.find('script', {'id':'__NEXT_DATA__'}) 
    
    if script_tag:
        json_data = json.loads(script_tag.string)

        offers = json_data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])
        
        all_offers = []

        for offer in offers:
            listing_id = offer.get("id", None)
            title = offer.get("title", "Brak tytułu")
            total_price = offer.get("totalPrice", {})
            price = total_price.get("value", "Brak ceny") if isinstance(total_price, dict) else "Brak ceny"
            link = f"https://www.otodom.pl/pl/oferta/{offer.get('slug', 'Brak linku')}"
            creation_date = offer.get("dateCreated", "Brak daty utworzenia")
            creation_date_first = offer.get("dateCreatedFirst", "Brak daty pierwszego utworzenia")
            
            all_offers.append({
                'listing_id': listing_id,
                'title': title,
                'price': price,
                'link': link,
                'creation_date': creation_date,
                'creation_date_first': creation_date_first
            })
        
        return all_offers
    else:
        raise Exception(f"Błąd w pobraniu danych ze strony, script_tag: {script_tag}")


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


# CLEANING DATA

FLOOR_MAPPING = {
    "['ground_floor']": 0,
    "['floor_1']": 1,
    "['floor_2']": 2,
    "['floor_3']": 3,
    "['floor_4']": 4,
    "['floor_5']": 5,
    "['floor_6']": 6,
    "['floor_7']": 7,
    "['floor_8']": 8,
    "['floor_9']": 9,
    "['floor_10']": 10,
    "['floor_higher_10']": "10+"
}

OWNERSHIP_MAPPING = {
    "pełna własność": "full_ownership",
    "spółdzielcze wł. prawo do lokalu": "cooperative_ownership",
}

def clear_floor_num(data):
    return FLOOR_MAPPING.get(data, None)

def simplify_ownership(data):
    return OWNERSHIP_MAPPING.get(data, None)

def extract_rooms_num(val):
    match = re.search(r'\d+', str(val))
    return int(match.group()) if match else None

def extract_text(data):
    if data is None:
        return None
    if isinstance(data, list):
        clean = ' '.join(data).strip("[]'") 
    else:
        clean = data.strip("[]',")
    return clean

def clear_numbers(data, val='int'):
    if data is not None:
        if val == 'int':
            return int(data)
        elif val == 'float':
            return float(data)

def clean_text(text):
    if text is None:
        return None
    text = text.replace("\n", " ") 
    text = text.replace("\xa0", " ")  
    text = re.sub(r'\s+', ' ', text).strip()  
    return text


def transform_data(data):
    transformed_data = data.copy()

    transformed_data["rooms_num"] = extract_rooms_num(transformed_data.get("rooms_num"))

    transformed_data["floor_num"] = clear_floor_num(transformed_data.get("floor_num"))

    transformed_data["ownership"] = simplify_ownership(transformed_data.get("ownership"))
    
    transformed_data['construction_status'] = extract_text(transformed_data.get('construction_status'))
    transformed_data['building_material'] = extract_text(transformed_data.get('building_material'))
    transformed_data['building_type'] = extract_text(transformed_data.get('building_type'))
    transformed_data['windows_type'] = extract_text(transformed_data.get('windows_type'))

    transformed_data['security_types'] = extract_text(transformed_data.get('security_types'))
    transformed_data['features_additional_information'] = extract_text(transformed_data.get('features_additional_information'))
    transformed_data['features_equipment'] = extract_text(transformed_data.get('features_equipment'))
    transformed_data['features_utilities'] = extract_text(transformed_data.get('features_utilities'))
    transformed_data['features'] = ' '.join([
        str(transformed_data.get('features_additional_information', '')).lower(),
        str(transformed_data.get('features_equipment', '')).lower(),
        str(transformed_data.get('features_utilities', '')).lower(),
        str(transformed_data.get('security_types', '')).lower()
    ])
    if ',' in transformed_data['features']:
        transformed_data['features'] = transformed_data['features'].replace(',', ' ')
    if "'" in transformed_data['features']:
        transformed_data['features'] = transformed_data['features'].replace("'", "")
    if "cable-television" in transformed_data['features']:
        transformed_data['features'] = transformed_data['features'].replace("cable-television", "cable_television")

    del transformed_data['security_types']
    del transformed_data['features_additional_information']
    del transformed_data['features_equipment']
    del transformed_data['features_utilities']

    transformed_data['energy_certificate'] = extract_text(transformed_data.get('energy_certificate'))

    transformed_data['description_text'] = clean_text(transformed_data.get('description_text'))
    
    if transformed_data.get('creation_date'):
        creation_date = datetime.strptime(transformed_data['creation_date'], '%Y-%m-%dT%H:%M:%S%z')
        transformed_data['creation_time'] = creation_date.strftime('%H:%M')
        transformed_data['creation_date'] = creation_date.date()  
    
    transformed_data['area'] = clear_numbers(transformed_data.get('area'), val='float')
    transformed_data['price'] = clear_numbers(transformed_data.get('price'), val='int')
    transformed_data['price_per_m'] = clear_numbers(transformed_data.get('price_per_m'), val='int')

    transformed_data['closing_date'] = None
    
    return transformed_data



# SAVING DATA

def save_data_to_excel(data:dict, file_name:str="output_data/data.xlsx"):
    """
    Saves the provided data to an Excel file. If the file exists, new data is 
    appended to the existing sheet.

    Parameters:
        data (dict): A dictionary containing the data to be saved to the Excel file.
        file_name (str): The path to the Excel file where the data will be saved 
        (default is 'output_data/data.xlsx').

    Raises:
        FileNotFoundError: If the Excel file does not exist and cannot be created.
    """
    df = pd.DataFrame([data])

    try:
        with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            book = writer.book
            sheet = book['oferty']
            
            start_row = sheet.max_row + 1  # pierwsza wolna linia

            for r_idx, row in df.iterrows():
                for c_idx, value in enumerate(row):
                    sheet.cell(row=start_row + r_idx, column=c_idx + 1, value=value)

    except FileNotFoundError:
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=True, sheet_name='oferty')

    print(f"Data has been successfully saved to {file_name}")




#page = fetch_page("https://www.otodom.pl/pl/oferta/mieszkanie-2-pokojowe-48m2-katowice-koszutka-ID4wQpd")
#page = fetch_page("https://www.otodom.pl/pl/oferta/dwupoziomowe-z-duzym-ogrodkiem-4-pokoje-ID4wyXG")
#print(download_data_from_listing_page(page))