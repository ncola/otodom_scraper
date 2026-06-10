"""
Tests for scraping/listing_page.py — functions that parse a single Otodom listing page.

How these tests work:
  The listing_page functions take an HTTP response object with a .text attribute (page HTML).
  To avoid making real network requests, we create a fake response using make_response().

  Otodom embeds all page data in a <script id="__NEXT_DATA__"> tag as JSON — make_listing_json()
  builds that JSON with default values based on a realistic listing from Katowice.
  To change a specific field in a test, pass it as an argument:
    make_listing_json(market="PRIMARY")
    make_listing_json(agency=None)

Test structure:
  1. Prepare input (HTML with JSON inside)
  2. Call the function being tested
  3. Check the result
"""

import json
from types import SimpleNamespace

from scraping.listing_page import download_data_from_listing_page, get_offer_status


def make_response(json_data: dict) -> SimpleNamespace:
    """Creates a fake HTTP response with data inside a __NEXT_DATA__ script tag."""
    html = f'<script id="__NEXT_DATA__">{json.dumps(json_data)}</script>'
    return SimpleNamespace(text=html)


def make_listing_json(**overrides) -> dict:
    """Builds a minimal Otodom listing-page JSON structure.
    Defaults represent a realistic flat listing from Katowice.
    Pass keyword arguments to override specific fields, e.g. make_listing_json(agency=None).
    """
    ad = {
        "id": 12345678,
        "slug": "mieszkanie-koszutka-ID12345678",
        "title": "Przestronne 3-pokojowe z balkonem",
        "market": "SECONDARY",
        "advertType": "AGENCY",
        "createdAt": "2025-03-12T09:45:00Z",
        "pushedUpAt": None,
        "exclusiveOffer": False,
        "creationSource": "posting_form",
        "description": "<p>Sprzedam mieszkanie</p>",
        "status": "active",
        "target": {
            "Area": "65.5",
            "Price": 549000,
            "Price_per_m": 8381,
            "Rent": 450,
            "Rooms_num": "['3']",
            "Floor_no": "['floor_3']",
            "Construction_status": "['ready_to_use']",
            "Building_type": "['block']",
            "Building_material": "['brick']",
            "Building_floors_num": "10",
            "Build_year": "1998",
            "Windows_type": "['plastic']",
            "Security_types": "['entryphone']",
            "Energy_certificate": "['E']",
            "ProperType": "mieszkanie",
            "City": "katowice",
            "Province": "slaskie",
            "Equipment_types": ["washing_machine", "fridge"],
            "Extras_types": ["balcony"],
            "Media_types": ["internet"],
        },
        "property": {"buildingProperties": {"heating": "URBAN"}},
        "characteristics": [
            {"key": "building_ownership", "localizedValue": "pełna własność"}
        ],
        "location": {
            "address": {"street": {"name": "ul. Słoneczna"}},
            "reverseGeocoding": {
                "locations": [{"locationLevel": "district", "name": "Koszutka"}]
            },
        },
        "links": {"localPlanUrl": "", "videoUrl": "", "view3dUrl": "", "walkaroundUrl": ""},
        "developmentId": 0,
        "developmentTitle": None,
        "owner": {"id": 9876543, "name": "Jan Kowalski"},
        "agency": {"id": 1234567, "name": "ABC Nieruchomości"},
    }
    ad.update(overrides)
    return {"props": {"pageProps": {"ad": ad}}}


# ---------- download_data_from_listing_page ----------

def test_listing_page_raises_when_response_is_none():
    """Checks that an exception is raised when response is None instead of crashing silently."""
    try:
        download_data_from_listing_page(None)
        assert False, "should have raised"
    except Exception as e:
        assert "None" in str(e)

def test_listing_page_parses_basic_fields():
    """Checks that id, title, market, advert_type, city and voivodeship are parsed correctly."""
    response = make_response(make_listing_json())
    result = download_data_from_listing_page(response)

    assert result.listing_id == 12345678
    assert result.title == "Przestronne 3-pokojowe z balkonem"
    assert result.market == "secondary"
    assert result.advert_type == "agency"
    assert result.city == "katowice"
    assert result.voivodeship == "slaskie"

def test_listing_page_strips_html_from_title():
    """Checks that HTML tags are removed from the title."""
    response = make_response(make_listing_json(title="<b>Mieszkanie</b> na sprzedaż"))
    result = download_data_from_listing_page(response)
    assert result.title == "Mieszkanie na sprzedaż"

def test_listing_page_strips_html_from_description():
    """Checks that HTML tags are removed from the description."""
    response = make_response(make_listing_json(description="<p>Sprzedam <b>mieszkanie</b></p>"))
    result = download_data_from_listing_page(response)
    assert result.description_text == "Sprzedam mieszkanie"

def test_listing_page_market_lowercased():
    """Checks that market value is lowercased (Otodom returns it in uppercase)."""
    response = make_response(make_listing_json(market="PRIMARY"))
    result = download_data_from_listing_page(response)
    assert result.market == "primary"

def test_listing_page_parses_price_and_area():
    """Checks that price and area are parsed correctly from the target object."""
    response = make_response(make_listing_json())
    result = download_data_from_listing_page(response)
    assert result.price == 549000
    assert result.area == "65.5"

def test_listing_page_parses_location():
    """Checks that street name and district are parsed correctly from the location object."""
    response = make_response(make_listing_json())
    result = download_data_from_listing_page(response)
    assert result.street == "ul. Słoneczna"
    assert result.district == "Koszutka"

def test_listing_page_parses_agency():
    """Checks that agency id and name are parsed correctly."""
    response = make_response(make_listing_json())
    result = download_data_from_listing_page(response)
    assert result.agency_id == 1234567
    assert result.agency_name == "ABC Nieruchomości"

def test_listing_page_agency_none_when_missing():
    """Checks that agency fields are None when Otodom sends no agency (private seller)."""
    response = make_response(make_listing_json(agency=None))
    result = download_data_from_listing_page(response)
    assert result.agency_id is None
    assert result.agency_name is None

def test_listing_page_owner_id_none_when_zero():
    """Checks that owner_id is stored as None when Otodom sends 0 (agency listing convention)."""
    response = make_response(make_listing_json(owner={"id": 0, "name": "Agencja"}))
    result = download_data_from_listing_page(response)
    assert result.owner_id is None

def test_listing_page_parses_ownership_from_characteristics():
    """Checks that ownership is read from the characteristics list, not the top-level fields."""
    response = make_response(make_listing_json())
    result = download_data_from_listing_page(response)
    assert result.ownership == "pełna własność"

def test_listing_page_offer_link_built_from_slug():
    """Checks that the full offer URL is constructed correctly from the slug."""
    response = make_response(make_listing_json(slug="mieszkanie-koszutka-ID12345678"))
    result = download_data_from_listing_page(response)
    assert result.offer_link == "https://www.otodom.pl/pl/oferta/mieszkanie-koszutka-ID12345678"


# ---------- get_offer_status ----------

def test_get_offer_status_returns_active(mocker):
    """Checks that 'active' status is returned when the listing page contains an active offer."""
    response = make_response(make_listing_json(status="active"))
    mocker.patch("scraping.listing_page.fetch_page", return_value=response)
    assert get_offer_status("https://www.otodom.pl/pl/oferta/mieszkanie-ID12345678") == "active"

def test_get_offer_status_returns_removed_when_response_is_none(mocker):
    """Checks that 'removed' is returned when the page fails to load (offer taken down)."""
    mocker.patch("scraping.listing_page.fetch_page", return_value=None)
    assert get_offer_status("https://www.otodom.pl/pl/oferta/mieszkanie-ID12345678") == "removed"
