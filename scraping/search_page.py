import json
import logging
from bs4 import BeautifulSoup

from scraping.client import fetch_page
from domain.models import ListingBasic


def validate_search_url(url: str, expected_display_name: str) -> None:
    """Fetch the search URL once and verify Otodom returns real results for the
    expected city. Raises RuntimeError with an actionable message when the URL
    fails to load, returns no results, or resolves to a different location
    (e.g. Otodom redirected because the slugs don't match)."""
    response = fetch_page(url)
    if response is None:
        raise RuntimeError(f"search URL failed to load: {url}")

    if get_total_pages(response) == 0:
        raise RuntimeError(
            f"search URL loaded but returned zero result pages — "
            f"slugs may be wrong for '{expected_display_name}'. URL: {url}"
        )

    # display name should appear somewhere in the Next.js payload for a matching city;
    # its absence usually means Otodom redirected the request to a different location
    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    payload = script_tag.string if script_tag else ""
    if expected_display_name.lower() not in payload.lower():
        raise RuntimeError(
            f"search URL loaded but its payload does not mention '{expected_display_name}' — "
            f"URL likely resolves to a different location. URL: {url}"
        )

    logging.info(f"search URL validated for '{expected_display_name}'")


def get_total_pages(html_response) -> int:
    try:
        if html_response is None:
            logging.error("response is None, can't get total pages")
            raise Exception("response is None, can't get total pages")

        soup = BeautifulSoup(html_response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if script_tag:
            json_data = json.loads(script_tag.string)
            page_count = json_data.get("props", {}).get("pageProps", {}).get("tracking", {}).get("listing", {}).get("page_count", 0)
            return page_count

        logging.warning("could not find __NEXT_DATA__ script tag for page count")
        return 0

    except Exception as error:
        logging.exception(f"error getting total pages: {error}")
        return 0


def parse_offers_from_response(html_response, page: int = 1) -> list[ListingBasic]:
    """Parses basic offer data from a single search results page into a list of ListingBasic objects.
    Extracts only the fields available on the search page (id, area, price, link) — used later
    to identify new offers and check for price changes, not as the final stored record.
    Handles both regular offers and developer listings with related ads (relatedAds).
    Returns an empty list if the page has no __NEXT_DATA__ script tag or no offers."""
    offers_out = []
    soup = BeautifulSoup(html_response.text, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    if not script_tag:
        logging.warning(f"no __NEXT_DATA__ script tag on page {page}, skipping")
        return offers_out

    json_data = json.loads(script_tag.string)
    offers = json_data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])

    if not offers:
        logging.warning(f"no offers found on page {page}, skipping")
        return offers_out

    n = 1
    for offer in offers:
        try:
            listing_id = offer.get("id")
            area = offer.get("areaInSquareMeters", 0)
            area = round(float(area), 2) if area is not None else 0
            total_price = offer.get("totalPrice", {})
            price = total_price.get("value", None) if isinstance(total_price, dict) else None
            ppm_data = offer.get("pricePerSquareMeter", {})
            price_per_m = ppm_data.get("value", None) if ppm_data else None
            link = f"https://www.otodom.pl/pl/oferta/{offer.get('slug', None)}"

            other_offers = offer.get("relatedAds", None)
            if other_offers:
                logging.debug(f"{n}. developer offer {listing_id} has related ads — area: {area}, price: {price}, price_per_m: {price_per_m}")
                for related_offer in other_offers:
                    id = related_offer.get("id")
                    area = related_offer.get("areaInSquareMeters", 0)
                    area = round(float(area), 2) if area is not None else 0
                    total_price = related_offer.get("totalPrice", {})
                    price = total_price.get("value", None) if isinstance(total_price, dict) else None
                    ppm_data = related_offer.get("pricePerSquareMeter", {})
                    price_per_m = ppm_data.get("value", None) if ppm_data else None
                    link = f"https://www.otodom.pl/pl/oferta/{related_offer.get('slug', None)}"

                    logging.debug(f"{n}. related offer {id} — area: {area}, price: {price}, price_per_m: {price_per_m}")
                    offers_out.append(ListingBasic(
                        listing_id=id,
                        area=area,
                        price=price,
                        price_per_m=price_per_m,
                        link=link
                    ))
                    n += 1
            else:
                logging.debug(f"{n}. offer {listing_id} — area: {area}, price: {price}, price_per_m: {price_per_m}")
                offers_out.append(ListingBasic(
                    listing_id=listing_id,
                    area=area,
                    price=price,
                    price_per_m=price_per_m,
                    link=link
                ))
                n += 1
        except Exception as error:
            logging.exception(f"skipped offer {n} on page {page} (id: {listing_id}): {error}")

    logging.debug(f"offers found on page {page}: {n - 1}")
    return offers_out


def download_data_from_search_results(base_url: str) -> list[ListingBasic]:
    """Fetches offers from all search result pages for a given base URL.
    Page 1 is reused from the response already fetched to get the page count, avoiding a redundant request.
    Skips pages that fail to load or return no data."""
    try:
        all_offers = []

        response_first_page = fetch_page(base_url)
        if response_first_page is None:
            logging.error("failed to fetch first search page — check the URL")
            raise Exception("failed to fetch first search page — check the URL")

        page_count = get_total_pages(response_first_page)
        if not page_count:
            raise Exception("could not determine page count — aborting search")
        logging.info(f"total pages found: {page_count}")

        all_offers.extend(parse_offers_from_response(response_first_page, page=1))

        for page in range(2, page_count + 1):
            page_url = f"{base_url}&page={page}"
            
            percent = int((page / page_count) * 100)
            logging.info(f"page {page}/{page_count} ({percent}%)")
            logging.debug(f"url: {page_url}")

            html_response = fetch_page(page_url)
            if html_response is None:
                logging.warning(f"failed to fetch page {page}, skipping")
                continue

            all_offers.extend(parse_offers_from_response(html_response, page=page))

        logging.debug(all_offers)
        return all_offers

    except Exception as error:
        logging.exception(f"error downloading data from search results: {error}")
        return []
