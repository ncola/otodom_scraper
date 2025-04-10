import requests,json
from urllib.robotparser import RobotFileParser
from scraper.utils import save_data_to_excel
from bs4 import BeautifulSoup

from scraper.fetch_and_parse import fetch_page, download_data_from_search_results, download_data_from_listing_page, categorize_offers_for_db
from scraper.transform_data import transform_data

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"


def is_allowed_to_scrape(url: str) -> bool:
    domain = '/'.join(url.split('/')[:3])
    robots_url = domain + '/robots.txt'

    try:
        response = requests.get(robots_url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
        response.raise_for_status() 
        rp = RobotFileParser()
        rp.parse(response.text.splitlines())

        return rp.can_fetch("*", url)

    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas pobierania robots.txt: {e}")
        return False


def scrape_all_pages_to_excel(url=url_main): # Not in use, left just in case
    response = fetch_page(url)
    offers = download_data_from_search_results(response)
    
    for offer in offers:
        link_offer = offer.get("link")
        response = fetch_page(link_offer)
        offer_data = download_data_from_listing_page(response)
        cleaned_offer_data = transform_data(offer_data)
        cleaned_offer_data.pop('images')
        save_data_to_excel(cleaned_offer_data, 'output_data/data_katowice.xlsx')
        

def scrape_offers(offers_to_insert):
    """
    Przekazywane sa tutaj oferty, których jeszcze nie ma w bazie
    """
    try:
        print("Rozpoczynanie pobierania znalezionych nowych ofert:")

        offers_data = []
        n=1
        for offer in offers_to_insert:
            offer_url = offer.get("link")
            id = offer.get("listing_id")

            response = fetch_page(offer_url)
            offer_data = download_data_from_listing_page(response)
            print(f"{n}. Pobrano ofertę o id {id}")
            cleaned_offer_data = transform_data(offer_data)
            offers_data.append(cleaned_offer_data)
            n+=1 
        print("Zakończono pobieranie ofert")

        return offers_data
    except Exception as error:
        print(f"Error during scraping all pages: {error}")


def scrape_all_pages(url):
    all_offers_basic = download_data_from_search_results(url)
    print('-' * 40)
    print(f"Liczba znalezionych ofert: {len(all_offers_basic)}")
    print('-' * 40)

    need_update_offers, new_offers = categorize_offers_for_db(all_offers_basic)

    print(f"Nowych ofert:{len(new_offers)}")
    print(f"Ofert, w których zmieniła się cena i wymagają update:{len(need_update_offers)}")

    new_offers_to_database = scrape_offers(new_offers)

    return need_update_offers, new_offers_to_database



