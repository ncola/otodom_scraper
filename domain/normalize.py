import re
from datetime import datetime
from domain.models import ListingFull


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


def clear_floor_num(data: str):
    if data is None:
        return None
    return FLOOR_MAPPING.get(data, None)


def simplify_ownership(data: str) -> str:
    if data is None:
        return None
    return OWNERSHIP_MAPPING.get(data, None)


def extract_rooms_num(data: str) -> int:
    if data is None:
        return None
    match = re.search(r'\d+', str(data))
    return int(match.group()) if match else None


def extract_text(data) -> str:
    if data is None:
        return None
    if isinstance(data, list):
        clean = ' '.join(data).strip("[]'")
    else:
        clean = data.strip("[]',")
    return clean


def clear_numbers(data, val: str = 'int'):
    if data is not None:
        if val == 'int':
            return int(data)
        elif val == 'float':
            return float(data)


def clean_text(text: str) -> str:
    if text is None:
        return None
    text = text.replace("\n", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def transform_data(listing: ListingFull) -> ListingFull:
    listing.rooms_num = extract_rooms_num(listing.rooms_num)
    listing.floor_num = clear_floor_num(listing.floor_num)
    listing.ownership = simplify_ownership(listing.ownership)

    listing.construction_status = extract_text(listing.construction_status)
    listing.building_material = extract_text(listing.building_material)
    listing.building_type = extract_text(listing.building_type)
    listing.windows_type = extract_text(listing.windows_type)

    features_str = ' '.join([
        extract_text(listing.features_additional_information) or '',
        extract_text(listing.features_equipment) or '',
        extract_text(listing.features_utilities) or '',
        extract_text(listing.security_types) or '',
    ]).lower()
    listing.features = (
        features_str
        .replace(',', ' ')
        .replace("'", "")
        .replace("cable-television", "cable_television")
    )

    # surowe pola scalone w features — już niepotrzebne
    listing.features_additional_information = None
    listing.features_equipment = None
    listing.features_utilities = None
    listing.security_types = None

    listing.energy_certificate = extract_text(listing.energy_certificate)
    listing.description_text = clean_text(listing.description_text)

    if listing.creation_date:
        creation_dt = datetime.strptime(listing.creation_date, '%Y-%m-%dT%H:%M:%S%z')
        listing.creation_time = creation_dt.strftime('%H:%M')
        listing.creation_date = creation_dt.date()

    listing.area = clear_numbers(listing.area, val='float')
    listing.price = clear_numbers(listing.price, val='int')
    listing.price_per_m = clear_numbers(listing.price_per_m, val='int')

    return listing
