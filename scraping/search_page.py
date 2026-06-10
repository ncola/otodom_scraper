import json
import logging
from bs4 import BeautifulSoup

from scraping.client import fetch_page
from domain.models import ListingBasic


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


def download_data_from_search_results(base_url: str) -> list[ListingBasic]:
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

        for page in range(1, page_count + 1):
            page_url = f"{base_url}&page={page}"
            percent = int((page / page_count) * 100)
            logging.info(f"page {page}/{page_count} ({percent}%)")
            logging.debug(f"url: {page_url}")

            html_response = fetch_page(page_url)
            if html_response is None:
                logging.warning(f"failed to fetch page {page}, skipping")
                continue

            soup = BeautifulSoup(html_response.text, 'html.parser')
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

            if not script_tag:
                logging.warning(f"no __NEXT_DATA__ script tag on page {page}, skipping")
                continue

            json_data = json.loads(script_tag.string)
            offers = json_data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])

            if not offers:
                logging.warning(f"no offers found on page {page}, skipping")
                continue

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
                            all_offers.append(ListingBasic(
                                listing_id=id,
                                area=area,
                                price=price,
                                price_per_m=price_per_m,
                                link=link
                            ))
                            n += 1
                    else:
                        logging.debug(f"{n}. offer {listing_id} — area: {area}, price: {price}, price_per_m: {price_per_m}")
                        all_offers.append(ListingBasic(
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

        logging.debug(all_offers)
        return all_offers

    except Exception as error:
        logging.exception(f"error downloading data from search results: {error}")
