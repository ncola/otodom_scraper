import json
import logging
from bs4 import BeautifulSoup

from scraping.client import fetch_page
from domain.models import ListingFull


def _str(val) -> str | None:
    """Converts value to string, but returns None instead of the string 'None'."""
    return str(val) if val is not None else None


def download_data_from_listing_page(html_response) -> dict:
    if html_response is None:
        raise Exception("html response is None, can't parse listing page")

    soup = BeautifulSoup(html_response.text, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    if script_tag:
        json_data = json.loads(script_tag.string)
        offer_data = json_data.get("props", {}).get("pageProps", {}).get("ad", {})

        listing_id = offer_data.get("id", None)
        listing_title = offer_data.get("title", None)
        listing_title = BeautifulSoup(listing_title, "html.parser").get_text()
        market_raw = offer_data.get("market")
        market_type = market_raw.lower() if market_raw is not None else None
        advert_raw = offer_data.get("advertType")
        advertisement_type = advert_raw.lower() if advert_raw is not None else None
        creation_date = offer_data.get("createdAt", None)
        description = offer_data.get("description", None)
        description_text = BeautifulSoup(description, "html.parser").get_text()
        is_exclusive_offer = offer_data.get("exclusiveOffer", None)
        creation_source = _str(offer_data.get("creationSource"))
        promoted_at = offer_data.get("pushedUpAt", None)
        heating_raw = offer_data.get("property", {}).get("buildingProperties", {}).get("heating")
        heating_type = heating_raw.lower() if heating_raw is not None else None

        target = offer_data.get("target", {})
        features_equipment = target.get("Equipment_types", None)
        features_additional_information = target.get("Extras_types", None)
        features_utilities = target.get("Media_types", None)

        area = target.get("Area", None)
        building_build_year = target.get("Build_year", None)
        building_floors_count = target.get("Building_floors_num", None)
        building_material = _str(target.get("Building_material"))

        characteristics = offer_data.get("characteristics", {})
        ownership = None
        for characteristic in characteristics:
            if characteristic["key"] == "building_ownership":
                ownership = characteristic.get("localizedValue", None)
                break

        building_type = _str(target.get("Building_type"))
        energy_certificate = target.get("Energy_certificate", None)
        city = target.get("City", None)
        voivodeship = target.get("Province", None)

        construction_status = _str(target.get("Construction_status"))
        floor_num = _str(target.get("Floor_no"))
        price = target.get("Price", None)
        price_per_m = target.get("Price_per_m", None)
        proper_type = target.get("ProperType", None)
        rent = target.get("Rent", None)
        windows_type = _str(target.get("Windows_type"))
        security_types = _str(target.get("Security_types"))
        rooms_num = _str(target.get("Rooms_num"))

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

        links = offer_data.get("links", {})
        local_plan_url = links.get("localPlanUrl", None)
        video_url = links.get("videoUrl", None)
        view3d_url = links.get("view3dUrl", None)
        walkaround_url = links.get("walkaroundUrl", None)

        development_id = offer_data.get("developmentId", None)
        development_title = offer_data.get("developmentTitle", None)
        seller = offer_data.get("owner", {})
        owner_id = seller.get("id", None)
        owner_name = seller.get("name", None)

        agency = offer_data.get("agency", {})
        if agency:
            agency_id = agency.get("id", None)
            agency_name = agency.get("name", None)
        else:
            agency_id = None
            agency_name = None

        return ListingFull(
            listing_id=listing_id,
            offer_link=f"https://www.otodom.pl/pl/oferta/{offer_data.get('slug', '')}",
            title=listing_title,
            market=market_type,
            advert_type=advertisement_type,
            creation_date=creation_date,
            pushed_up_at=promoted_at,
            exclusive_offer=is_exclusive_offer,
            creation_source=creation_source,
            description_text=description_text,
            area=area,
            price=price,
            price_per_m=price_per_m,
            rent_amount=rent,
            rooms_num=rooms_num,
            floor_num=floor_num,
            heating=heating_type,
            ownership=ownership,
            proper_type=proper_type,
            construction_status=construction_status,
            features_utilities=features_utilities,
            features_equipment=features_equipment,
            features_additional_information=features_additional_information,
            energy_certificate=energy_certificate,
            voivodeship=voivodeship,
            city=city,
            district=district,
            street=street,
            building_build_year=building_build_year,
            building_floors_num=building_floors_count,
            building_material=building_material,
            building_type=building_type,
            windows_type=windows_type,
            security_types=security_types,
            local_plan_url=local_plan_url,
            video_url=video_url,
            view3d_url=view3d_url,
            walkaround_url=walkaround_url,
            development_id=development_id,
            development_title=development_title or None,
            owner_id=None if owner_id == 0 else owner_id,
            owner_name=owner_name,
            agency_id=agency_id,
            agency_name=agency_name,
        )


def get_offer_status(offer_link: str) -> str:
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
        logging.exception(f"error during getting offer status: {error}")
        return "removed"
