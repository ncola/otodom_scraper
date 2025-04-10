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


def clear_floor_num(data:str) ->str:
    """
    Maps the floor number from the given input data to a standard value using FLOOR_MAPPING
    """
    if data is None:
        return None
    return FLOOR_MAPPING.get(data, None)


def simplify_ownership(data: str) -> str:
    """     
    Simplifies the ownership status of a property using the OWNERSHIP_MAPPING
    """
    if data is None:
        return None
    return OWNERSHIP_MAPPING.get(data, None)


def extract_rooms_num(data: str) -> int:
    """
    Extracts the number of rooms from a string. It looks for the first numeric value in the string
    """
    if data is None:
        return None
    match = re.search(r'\d+', str(data))
    return int(match.group()) if match else None


def extract_text(data) -> str:
    """
    Extracts and cleans text from the given data. If the data is a list, it joins the list into a string
    
    Args:
    data: The input data (can be a string or a list of strings)

    Returns:
    str: The cleaned text or None if the input data is None

    If the input data is a list, the function joins the elements into a single string and removes unwanted characters
    """
    if data is None:
        return None
    if isinstance(data, list):
        clean = ' '.join(data).strip("[]'") 
    else:
        clean = data.strip("[]',")
    return clean


def clear_numbers(data, val:str='int'):
    """
    Converts the given data to either an integer or a float based on the specified value type

    Args:
    data: The data to be converted (should be a numeric string or a number)
    val (str): The type to convert to. Defaults to 'int'. Can also be 'float'

    Returns:
    int or float: The converted data (either as an integer or a float), or None if the input data is None

    If the value type is 'int', the function converts the data to an integer
    If the value type is 'float', the function converts the data to a float
    """
    if data is not None:
        if val == 'int':
            return int(data)
        elif val == 'float':
            return float(data)


def clean_text(text: str) -> str:
    """     
    Cleans the given text by removing unwanted characters, extra spaces, and line breaks
    """
    if text is None:
        return None
    text = text.replace("\n", " ") 
    text = text.replace("\xa0", " ")  
    text = re.sub(r'\s+', ' ', text).strip()  
    return text


def transform_data(data: dict) -> dict:
    """
    Transforms the input data by cleaning, simplifying, and formatting it to a standardized structure

    Args:
    data (dict): The data dictionary containing information about a property listing.

    Returns:
    dict: The transformed data dictionary with cleaned and simplified values.

    This function processes various fields in the input data, such as:
    - Extracting the number of rooms,
    - Mapping the floor number and ownership status to simplified values,
    - Cleaning text fields,
    - Combining features into one field,
    - Formatting price, area, and date values,
    """
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







