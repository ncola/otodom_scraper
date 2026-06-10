"""
Tests for scraping/search_page.py — functions that parse the Otodom search results page.

How these tests work:
  The search_page functions take an HTTP response object with a .text attribute (page HTML).
  To avoid making real network requests, we create a fake response using make_response().

  Otodom embeds all page data in a <script id="__NEXT_DATA__"> tag as JSON — make_search_json()
  builds that JSON with only the fields needed for the test.

Test structure:
  1. Prepare input (HTML with JSON inside)
  2. Call the function being tested
  3. Check the result
"""
import json
from types import SimpleNamespace

from scraping.search_page import get_total_pages, parse_offers_from_response


def make_response(json_data: dict) -> SimpleNamespace:
    """Creates a fake HTTP response with data inside a __NEXT_DATA__ script tag."""
    html = f'<script id="__NEXT_DATA__">{json.dumps(json_data)}</script>'
    return SimpleNamespace(text=html)


def make_search_json(page_count=3, items=None) -> dict:
    """Builds a minimal Otodom search-page JSON structure."""
    return {
        "props": {
            "pageProps": {
                "tracking": {"listing": {"page_count": page_count}},
                "data": {"searchAds": {"items": items or []}},
            }
        }
    }


def make_offer_item(listing_id=11111111, area=55.0, price=450000, price_per_m=8181) -> dict:
    """Builds a single offer item as returned by Otodom in the search results JSON."""
    return {
        "id": listing_id,
        "areaInSquareMeters": area,
        "totalPrice": {"value": price},
        "pricePerSquareMeter": {"value": price_per_m},
        "slug": f"mieszkanie-ID{listing_id}",
        "relatedAds": None,
    }


# ---------- get_total_pages ----------

def test_get_total_pages_returns_correct_count():
    """Checks that the page count is read correctly from the JSON."""
    response = make_response(make_search_json(page_count=7))
    assert get_total_pages(response) == 7

def test_get_total_pages_returns_zero_when_no_script_tag():
    """Checks that 0 is returned when the page has no __NEXT_DATA__ script tag."""
    response = SimpleNamespace(text="<html><body>no data here</body></html>")
    assert get_total_pages(response) == 0

def test_get_total_pages_returns_zero_when_response_is_none():
    """Checks that 0 is returned instead of crashing when response is None."""
    assert get_total_pages(None) == 0

def test_get_total_pages_returns_zero_when_key_missing():
    """Checks that 0 is returned when the page_count key is absent from the JSON."""
    response = make_response({"props": {"pageProps": {}}})
    assert get_total_pages(response) == 0


# ---------- parse_offers_from_response ----------

def test_parse_offers_returns_correct_number_of_offers():
    """Checks that all offers from the page are returned."""
    items = [make_offer_item(listing_id=i) for i in [11111111, 22222222, 33333333]]
    response = make_response(make_search_json(items=items))
    result = parse_offers_from_response(response)
    assert len(result) == 3

def test_parse_offers_maps_fields_correctly():
    """Checks that all fields (id, area, price, price_per_m, link) are mapped correctly."""
    item = make_offer_item(listing_id=12345678, area=65.5, price=549000, price_per_m=8381)
    response = make_response(make_search_json(items=[item]))

    result = parse_offers_from_response(response)

    assert len(result) == 1
    offer = result[0]
    assert offer.listing_id == 12345678
    assert offer.area == 65.5
    assert offer.price == 549000
    assert offer.price_per_m == 8381
    assert offer.link == "https://www.otodom.pl/pl/oferta/mieszkanie-ID12345678"

def test_parse_offers_returns_empty_list_when_no_script_tag():
    """Checks that an empty list is returned when the page has no __NEXT_DATA__ script tag."""
    response = SimpleNamespace(text="<html><body></body></html>")
    assert parse_offers_from_response(response) == []

def test_parse_offers_returns_empty_list_when_items_empty():
    """Checks that an empty list is returned when the JSON contains no offers."""
    response = make_response(make_search_json(items=[]))
    assert parse_offers_from_response(response) == []

def test_parse_offers_area_is_rounded_to_two_decimals():
    """Checks that area is rounded to 2 decimal places."""
    item = make_offer_item(area=65.555555)
    response = make_response(make_search_json(items=[item]))
    result = parse_offers_from_response(response)
    assert result[0].area == 65.56

def test_parse_offers_price_none_when_totalPrice_missing():
    """Checks that price is None when Otodom omits totalPrice (common for developer listings)."""
    item = make_offer_item()
    item["totalPrice"] = {}
    response = make_response(make_search_json(items=[item]))
    result = parse_offers_from_response(response)
    assert result[0].price is None

def test_parse_offers_handles_related_ads():
    """Checks that a developer offer with relatedAds produces a separate ListingBasic for each sub-offer."""
    related = [
        make_offer_item(listing_id=11111111, area=40.0, price=320000),
        make_offer_item(listing_id=22222222, area=60.0, price=480000),
    ]
    item = make_offer_item(listing_id=99999999)
    item["relatedAds"] = related

    response = make_response(make_search_json(items=[item]))
    result = parse_offers_from_response(response)

    assert len(result) == 2
    assert result[0].listing_id == 11111111
    assert result[1].listing_id == 22222222
