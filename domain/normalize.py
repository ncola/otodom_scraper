import re
from datetime import datetime


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


def transform_data(data: dict) -> dict:
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
    transformed_data['features'] = (
        transformed_data['features']
        .replace(',', ' ')
        .replace("'", "")
        .replace("cable-television", "cable_television")
    )

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
    transformed_data['detected_inactive_at'] = None

    return transformed_data
