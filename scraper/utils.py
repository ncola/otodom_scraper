import pandas as pd
import requests, cv2, json
from bs4 import BeautifulSoup
import numpy as np

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
            title = offer.get("title", "Brak tytułu")
            total_price = offer.get("totalPrice", {})
            price = total_price.get("value", "Brak ceny") if isinstance(total_price, dict) else "Brak ceny"
            
            link = f"https://www.otodom.pl/pl/oferta/{offer.get('slug', 'Brak linku')}"
            date_created = offer.get("dateCreated", "Brak daty utworzenia")
            date_created_first = offer.get("dateCreatedFirst", "Brak daty pierwszego utworzenia")
            
            all_offers.append({
                'title': title,
                'price': price,
                'link': link,
                'date_created': date_created,
                'date_created_first': date_created_first
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
        #print("\n")

        listing_id = offer_data.get("id", "N/A")
        listing_title = offer_data.get("title", "N/A")
        listing_title = BeautifulSoup(listing_title, "html.parser").get_text()
        market_type = str(offer_data.get("market", "N/A")).lower()
        advertisement_type = str(offer_data.get("advertType", "N/A")).lower()
        creation_date = offer_data.get("createdAt", "N/A")
        last_modified_date = offer_data.get("modifiedAt", "N/A")
        description = offer_data.get("description", "N/A")
        description_text = BeautifulSoup(description, "html.parser").get_text()
        is_exclusive_offer = offer_data.get("exclusiveOffer", "N/A") # True/False
        creation_source = str(offer_data.get("creationSource", "N/A"))
        promoted_at = offer_data.get("pushedUpAt", "N/A")
        heating_type = str(offer_data.get("property", {}).get("buildingProperties", {}).get("heating", "N/A")).lower()
      
        # cechy w podziale na kategorie
        features_by_category = offer_data.get("featuresByCategory", [])

        categories_names_HTML = ['Media', 'Zabezpieczenia', 'Wyposażenie', 'Informacje dodatkowe']
        categories_names_variables = ['feautures_media', 'features_security', 'features_equipment', 'features_additional_information']

        for category_name, category_name_variable in zip(categories_names_HTML, categories_names_variables):
            matching_category = next((category for category in features_by_category if category.get('label') == category_name), None)
            if matching_category:
                values = matching_category.get("values", [])
                globals()[category_name_variable]  = ', '.join(values) if values else "N/A"
            else:
                globals()[category_name_variable]  = "N/A"

        features_without_category = offer_data.get("featuresWithoutCategory", "N/A")

        target = offer_data.get("target", {})
        area = target.get("Area", "N/A")
        build_year = target.get("Build_year", "N/A")
        building_floors_count = target.get("Building_floors_num", "N/A")
        building_material = str(target.get("Building_material", "N/A"))
        building_ownership = str(target.get("Building_ownership", "N/A")) # ownership (Własność); cooperative_ownership (Spółdzielcze własnościowe prawo do lokalu); land_ownership (Własność gruntu); state_ownership (Własność państwowa); municipal_ownership (Własność komunalna)
        building_type = str(target.get("Building_type", "N/A"))
        city = target.get("City", "N/A")
        construction_status = str(target.get("Construction_status", "N/A")) #under_construction; completed; planned; ready_for_occupancy
        floor_num = str(target.get("Floor_no", "N/A"))
        price = int(target.get("Price", "N/A"))
        price_per_m = float(target.get("Price_per_m", "N/A"))
        proper_type = target.get("ProperType", "N/A") #Mieszkanie; Dom; Działka; Komercyjna; Inny
        rent = target.get("Rent", "N/A") #czasem ludzie wpisują '0' a czasem jest puste pole
        windows_type = str(target.get("Windows_type", "N/A"))
        security_types = str(target.get("Security_types", "N/A"))
        if isinstance(security_types, list):
            security_types = ', '.join(data for data in security_types)
        rooms_num = str(target.get("Rooms_num", "N/A"))
        

        breadcrumbs = offer_data.get("breadcrumbs", [])
        locations = " - ".join(breadcrumb.get("locative", "") for breadcrumb in breadcrumbs)

        location_data = offer_data.get("location", {}).get("address", {})
        if location_data:
            street = location_data.get("street", {}).get("name", "N/A") if location_data.get("street") else "N/A"
            subdistrict = location_data.get("subdistrict", "N/A") if location_data.get("subdistrict") else "N/A"
            district = location_data.get("district", "N/A") if location_data.get("district") else "N/A"
            if isinstance(district, dict):
                district = ", ".join((str(data) for data in list(district.values())[1:-1])) #keys: id, code, name
            else:
                district = district
        else:
            street = "N/A"
            subdistrict = "N/A"
            district = "N/A"
        
        reverseGeocoding_locations = offer_data.get("location", {}).get("reverseGeocoding", {}).get("locations", [])
        for data in reverseGeocoding_locations:
            dzielnica = data.get("name") if data.get("locationLevel") == "district" else "N/A"

        # Zdjęcia
        images = []
        images_html = offer_data.get("images", "N/A")
        for element in images_html:
            image_link = element.get("medium", "N/A")
            image_response = fetch_page(image_link)
            arr = np.asarray(bytearray(image_response.content), dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)  
            images.append(img)

        # Linki
        links = (offer_data.get("links", {}))
        local_plan_url = (links.get("localPlanUrl", "N/A"))
        video_url = (links.get("videoUrl", "N/A"))
        view3d_url = (links.get("view3dUrl", "N/A"))
        walkaround_url = (links.get("walkaroundUrl", "N/A"))
        
        # Sprzedający
        seller = (offer_data.get("owner", {}))
        owner_id = (seller.get("id", "N/A"))
        owner_name = (seller.get("name", "N/A"))

        agency = (offer_data.get("agency", {}))
        if agency:
            agency_id = (agency.get("id", "N/A"))
            agency_name = (agency.get("name", "N/A"))
        else:
            agency_id = "N/A"
            agency_name = "N/A"

        # podstawowe informacje o ofercie
        data = {}
        data["id"] = listing_id
        data["title"] = listing_title
        data["market"] = market_type
        data["advert_type"] = advertisement_type
        data["date_created"] = creation_date
        data["date_modified"] = last_modified_date
        data["pushed_ap_at"] = promoted_at
        data["exclusive_offer"] = is_exclusive_offer
        data["creation_source"] = creation_source

        #cechy mieszkania
        data["description_text"] = description_text
        data["area"] = area
        data["price"] = price
        data["price_per_m"] = price_per_m
        data["rent"] = rent
        data["rooms_num"] = rooms_num
        data["floor_num"] = floor_num
        data["heating"] = heating_type
        data["building_ownership"] = building_ownership
        data["properType"] = proper_type
        data["construction_status"] = construction_status
        data["feautures_media"] = feautures_media
        data["features_security"] = features_security
        data["features_equipment"] = features_equipment
        data["features_additional_information"] = features_additional_information
        data["features_without_category"] = features_without_category

        # lokalizacja
        data["locations"] = locations
        data["city"] = city
        data["district"] = district
        data["subdistrict"] = subdistrict
        data["street"] = street
        data["dzielnica"] = dzielnica

        # szczegółu budynku
        data["build_year"] = build_year
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
        
        # sprzedajacy
        data["owner_id"] = owner_id
        data["owner_name"] = owner_name
        data["agency_id"] = agency_id
        data["agency_name"] = agency_name

        return data


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
