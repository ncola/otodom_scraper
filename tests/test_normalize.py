from datetime import date

from domain.normalize import (
    transform_data,
    clear_floor_num,
    simplify_ownership,
    extract_rooms_num,
    clean_text,
)
from tests.conftest import make_listing


# ---------- unit tests for helper functions ----------

def test_floor_mapping_ground():
    assert clear_floor_num("['ground_floor']") == 0

def test_floor_mapping_numbered():
    assert clear_floor_num("['floor_3']") == 3

def test_floor_mapping_higher_10():
    assert clear_floor_num("['floor_higher_10']") == "10+"

def test_floor_mapping_none():
    assert clear_floor_num(None) is None

def test_floor_mapping_unknown_value():
    assert clear_floor_num("['floor_99']") is None


def test_ownership_full():
    assert simplify_ownership("pełna własność") == "full_ownership"

def test_ownership_cooperative():
    assert simplify_ownership("spółdzielcze wł. prawo do lokalu") == "cooperative_ownership"

def test_ownership_none():
    assert simplify_ownership(None) is None


def test_extract_rooms_num_from_list_string():
    assert extract_rooms_num("['3']") == 3

def test_extract_rooms_num_plain():
    assert extract_rooms_num("2") == 2

def test_extract_rooms_num_none():
    assert extract_rooms_num(None) is None


def test_clean_text_removes_extra_whitespace():
    assert clean_text("hello   world") == "hello world"

def test_clean_text_removes_newlines():
    assert clean_text("hello\nworld") == "hello world"

def test_clean_text_none():
    assert clean_text(None) is None


# ---------- transform_data: types after normalization ----------


def test_transform_data_area_is_float():
    listing = make_listing(area=65)
    result = transform_data(listing)
    assert isinstance(result.area, float)

def test_transform_data_price_is_int():
    listing = make_listing(price=450000)
    result = transform_data(listing)
    assert isinstance(result.price, int)

def test_transform_data_price_per_m_is_int():
    listing = make_listing(price_per_m=6923)
    result = transform_data(listing)
    assert isinstance(result.price_per_m, int)

def test_transform_data_floor_mapped():
    listing = make_listing(floor_num="['floor_3']")
    result = transform_data(listing)
    assert result.floor_num == 3

def test_transform_data_rooms_extracted():
    listing = make_listing(rooms_num="['3']")
    result = transform_data(listing)
    assert result.rooms_num == 3

def test_transform_data_ownership_simplified():
    listing = make_listing(ownership="pełna własność")
    result = transform_data(listing)
    assert result.ownership == "full_ownership"

def test_transform_data_creation_date_parsed():
    listing = make_listing(creation_date="2024-01-15T10:30:00+01:00")
    result = transform_data(listing)
    assert isinstance(result.creation_date, date)
    assert result.creation_date == date(2024, 1, 15)
    assert result.creation_time == "10:30"


# ---------- transform_data: features ----------

def test_transform_data_features_merged_into_string():
    listing = make_listing(
        features_utilities=["internet", "cable-television"],
        features_equipment=["washing_machine", "fridge"],
        features_additional_information=["balcony"],
        security_types=["alarm"],
    )
    result = transform_data(listing)
    assert isinstance(result.features, str)
    assert "internet" in result.features
    assert "balcony" in result.features
    assert "alarm" in result.features

def test_transform_data_cable_television_dash_replaced():
    listing = make_listing(features_utilities=["cable-television"])
    result = transform_data(listing)
    assert "cable_television" in result.features
    assert "cable-television" not in result.features

def test_transform_data_raw_feature_fields_cleared():
    listing = make_listing(
        features_utilities=["internet"],
        features_equipment=["fridge"],
        features_additional_information=["balcony"],
        security_types=["alarm"],
    )
    result = transform_data(listing)
    assert result.features_utilities is None
    assert result.features_equipment is None
    assert result.features_additional_information is None
    assert result.security_types is None


# ---------- transform_data: real-world edge cases ----------

def test_transform_data_area_as_string():
    """area comes from Otodom as a string like '38.3', not a float."""
    listing = make_listing(area="38.3")
    result = transform_data(listing)
    assert result.area == 38.3
    assert isinstance(result.area, float)

def test_transform_data_price_none():
    """price can be null (e.g. developer hides price) — must not crash."""
    listing = make_listing(price=None)
    result = transform_data(listing)
    assert result.price is None

def test_transform_data_floor_higher_10():
    """floor_higher_10 is a real value returned by Otodom"""
    listing = make_listing(floor_num="['floor_higher_10']")
    result = transform_data(listing)
    assert result.floor_num == "10+"

def test_transform_data_building_year_as_string():
    """building_build_year comes from Otodom as string '2025', must be int"""
    listing = make_listing(building_build_year="2025")
    result = transform_data(listing)
    assert result.building_build_year == 2025
    assert isinstance(result.building_build_year, int)

def test_transform_data_building_floors_as_string():
    """building_floors_num comes from Otodom as string '7', must be int"""
    listing = make_listing(building_floors_num="7")
    result = transform_data(listing)
    assert result.building_floors_num == 7
    assert isinstance(result.building_floors_num, int)

def test_transform_data_building_year_none():
    """building_build_year is often null for secondary market — must not crash"""
    listing = make_listing(building_build_year=None)
    result = transform_data(listing)
    assert result.building_build_year is None

def test_transform_data_all_features_null():
    """Developer listings often have all feature fields null — must not crash"""
    listing = make_listing(
        features_utilities=None,
        features_equipment=None,
        features_additional_information=None,
        security_types=None,
    )
    result = transform_data(listing)
    assert isinstance(result.area, float)
    assert isinstance(result.price, int)
    assert isinstance(result.features, str)
    assert result.features_utilities is None
    assert result.features_equipment is None
    assert result.security_types is None

def test_transform_data_security_types_as_string():
    """security_types comes as string "['alarm', 'monitoring']" not a list"""
    listing = make_listing(security_types="['alarm', 'monitoring']")
    result = transform_data(listing)
    assert "alarm" in result.features
    assert "monitoring" in result.features
