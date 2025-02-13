import pandas as pd
import requests
from bs4 import BeautifulSoup
import json


def fetch_page(url:str):
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


def download_data_from_searching_page(html_response):
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


def download_data_from_listing_page(html_response):
    if html_response is None:
        raise Exception(f"Wystąpił błąd w pobraniu danych ze strony")
    
    soup = BeautifulSoup(html_response.text, 'html.parser')
    script_tag = soup.find('script', {'id':'__NEXT_DATA__'})

    if script_tag:
        json_data = json.loads(script_tag.string)

        offer_data = json_data.get("props", {}).get("pageProps", {}).get("ad", {})

        # Debug: wydrukowanie tylko tej części JSON, zaczynając od ...
        #print("Struktura JSON (od ...):", json.dumps(offer_data, indent=7)[:420000])
        print("\n")

        id = offer_data.get("id", "Brak")
        market = offer_data.get("market", "Brak rynku")
        advertiser_type = offer_data.get("advertiserType", "Brak typu ogłoszeniodawcy")
        advert_type = offer_data.get("advertType", "Brak typu ogłoszenia")
        date_created = offer_data.get("createdAt", "Brak daty utworzenia")
        date_modified = offer_data.get("modifiedAt", "Brak daty modyfikacji")
        description = offer_data.get("description", "Brak opisu")
        description_text = BeautifulSoup(description, "html.parser").get_text()
        exclusive_offer = offer_data.get("exclusiveOffer", "Brak")
        features = offer_data.get("features", "Brak cech")
        features_str = ', '.join(features) if isinstance(features, list) else str(features)
        creation_source = offer_data.get("creationSource", "Brak")
        pushed_ap_at = offer_data.get("pushedUpAt", "Brak")
        heating = offer_data.get("property", {}).get("buildingProperties", {}).get("heating", "Brak")
      
        # cechy w podziale na kategorie
        features_by_category = offer_data.get("featuresByCategory", [])

        features_by_category = offer_data.get("featuresByCategory", [])
        categories_names_HTML = ['Media', 'Zabezpieczenia', 'Wyposażenie', 'Informacje dodatkowe']
        categories_names_variables = ['cechy_media', 'cechy_zabezpieczenia', 'cechy_wyposazenie', 'cechy_informacje_dodatkowe']

        for category_name, category_name_variable in zip(categories_names_HTML, categories_names_variables):
            matching_category = next((category for category in features_by_category if category.get('label') == category_name), None)
            if matching_category:
                values = matching_category.get("values", [])
                globals()[category_name_variable]  = ', '.join(values) if values else "Brak"
            else:
                globals()[category_name_variable]  = "Brak"


        target = offer_data.get("target", {})
        area = target.get("Area", "Brak")
        build_year = target.get("Build_year", "Brak")
        building_floors_num = target.get("Building_floors_num", "Brak")
        building_material = str(target.get("Building_material", "Brak"))
        building_ownership = str(target.get("Building_ownership", "Brak"))
        building_type = str(target.get("Building_type", "Brak"))
        city = target.get("City", "Brak")
        construction_status = str(target.get("Construction_status", "Brak"))
        floor_no = str(target.get("Floor_no", "Brak"))
        price = target.get("Price", "Brak")
        price_per_m = target.get("Price_per_m", "Brak")
        properType = target.get("ProperType", "Brak")
        rent = target.get("Rent", "Brak")
        windows_type = str(target.get("Windows_type", "Brak"))
        security_types = target.get("Security_types", "Brak")
        if isinstance(security_types, list):
            security_types = ', '.join(data for data in security_types)
        rooms_num = str(target.get("Rooms_num", "Brak"))
        

        breadcrumbs = offer_data.get("breadcrumbs", [])
        locations = " - ".join(breadcrumb.get("locativ", "") for breadcrumb in breadcrumbs)

        location_data = offer_data.get("location", {}).get("address", {})
        if location_data:
            street = location_data.get("street", {}).get("name", "Brak") if location_data.get("street") else "Brak"
            subdistrict = location_data.get("subdistrict", "Brak") if location_data.get("subdistrict") else "Brak"
            district = location_data.get("district", "Brak") if location_data.get("district") else "Brak"
            if isinstance(district, dict):
                district = ", ".join((str(data) for data in list(district.values())[:-1])) #keys: id, code, name
            else:
                district = district
        else:
            street = "Brak"
            subdistrict = "Brak"
            district = "Brak"
        
        reverseGeocoding_locations = offer_data.get("location", {}).get("reverseGeocoding", {}).get("locations", [])
        for data in reverseGeocoding_locations:
            dzielnica = data.get("name") if data.get("locationLevel") == "district" else "Brak"


        links = offer_data.get("links", {})
        local_plan_url = links.get("localPlanUrl", "Brak linku")
        video_url = links.get("videoUrl", "Brak linku")
        view3d_url = links.get("view3dUrl", "Brak linku")
        walkaround_url = links.get("walkaroundUrl", "Brak linku")
        
        seller = offer_data.get("owner", {})
        owner_id = seller.get("id", "Brak")
        owner_name = seller.get("name", "Brak")

        agency = offer_data.get("agency", {})
        if agency:
            agency_id = agency.get("id", "Brak")
            agency_name = agency.get("name", "Brak")
        else:
            agency_id = "Brak"
            agency_name = "Brak"

        
        print("Dane podstawowe oferty:")
        print(f"ID oferty: {id}")
        print(f"Rynek: {market}")
        print(f"Typ ogłoszeniodawcy: {advertiser_type}")
        print(f"Typ ogłoszenia: {advert_type}")
        print(f"Data utworzenia: {date_created}")
        print(f"Data modyfikacji: {date_modified}")
        print(f"pushed_ap_at: {pushed_ap_at}")
        print(f"exclusive_offer: {exclusive_offer}") 
        print(f"creation_source: {creation_source}")

        print("\nDane podstawowe mieszkania:")
        print(f"Opis: {description_text[:100]}...")  # Wyświetlamy tylko część opisu
        print(f"Powierzchnia: {area}")
        print(f"Cena: {price}")
        print(f"Cena za m2: {price_per_m}")
        print(f"Czynsz: {rent}")
        print(f"Liczba pokoi: {rooms_num}")
        print(f"Piętro: {floor_no}")
        print(f"Ogrzewanie: {heating}")
        print(f"Forma własności: {building_ownership}")
        print(f"ProperType: {properType}")
        print(f"Stan wykończenia: {construction_status}")
        print(f"media: {cechy_media}")
        print(f"zabezpieczenia: {cechy_zabezpieczenia}")
        print(f"wyposazenie: {cechy_wyposazenie}")
        print(f"informacje_dodatkowe: {cechy_informacje_dodatkowe}")
        print(f"Cechy: {features_str}")

        print("\nLokalizacja:")
        print(f"location: {locations}")
        print(f"Miasto: {city}")
        print(f"District: {district}")
        print(f"Subdistrict: {subdistrict}")
        print(f"Street: {street}")
        print(f"Dzielnica: {dzielnica}")

        print("\nSzczegóły budynku:")
        print(f"Rok budowy budynku: {build_year}")
        print(f"Ilość pięter w budynku: {building_floors_num}")
        print(f"Materiał budowlany: {building_material}")
        print(f"Rodzaj zabudowy: {building_type}")
        print(f"Typ okien: {windows_type}")
        print(f"Security types: {security_types}")

        print("\nLinki:")
        print(f"Local Plan URL: {local_plan_url}")
        print(f"Video URL: {video_url}")
        print(f"3D View URL: {view3d_url}")
        print(f"Walkaround URL: {walkaround_url}")

        print("\nSprzedający:")
        print(f"owner_id: {owner_id}")
        print(f"owner_name: {owner_name}")
        print(f"agency_id: {agency_id}")
        print(f"agency_name: {agency_name}")

        # podstawowe informacje o ofercie
        data = {}
        data["id"] = id
        data["market"] = market
        data["advertiser_type"] = advertiser_type
        data["advert_type"] = advert_type
        data["date_created"] = date_created
        data["date_modified"] = date_modified
        data["pushed_ap_at"] = pushed_ap_at
        data["exclusive_offer"] = exclusive_offer
        data["creation_source"] = creation_source

        #cechy mieszkania
        data["description_text"] = description_text[:1000] + "..."  # Wyświetlamy tylko część opisu
        data["area"] = area
        data["price"] = price
        data["price_per_m"] = price_per_m
        data["rent"] = rent
        data["rooms_num"] = rooms_num
        data["floor_no"] = floor_no
        data["heating"] = heating
        data["building_ownership"] = building_ownership
        data["properType"] = properType
        data["construction_status"] = construction_status
        data["features_str"] = features_str
        data["cechy_media"] = cechy_media
        data["cechy_zabezpieczenia"] = cechy_zabezpieczenia
        data["cechy_wyposazenie"] = cechy_wyposazenie
        data["cechy_informacje_dodatkowe"] = cechy_informacje_dodatkowe

        # lokalizacja
        data["locations"] = locations
        data["city"] = city
        data["district"] = district
        data["subdistrict"] = subdistrict
        data["street"] = street
        data["dzielnica"] = dzielnica

        # szczegółu budynku
        data["build_year"] = build_year
        data["building_floors_num"] = building_floors_num
        data["building_material"] = building_material
        data["building_type"] = building_type
        data["windows_type"] = windows_type
        data["security_types"] = security_types

        # linki
        data["local_plan_url"] = local_plan_url
        data["video_url"] = video_url
        data["view3d_url"] = view3d_url
        data["walkaround_url"] = walkaround_url

        # sprzedajacy
        data["owner_id"] = owner_id
        data["owner_name"] = owner_name
        data["agency_id"] = agency_id
        data["agency_name"] = agency_name

        return data


def save_data_to_excel(data, file_name="output_data/data.xlsx"):
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
