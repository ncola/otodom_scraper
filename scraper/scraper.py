import requests
from urllib.robotparser import RobotFileParser
from scraper.utils import save_data_to_excel

from scraper.fetch_and_parse import fetch_page, download_data_from_search_results, download_data_from_listing_page, categorize_offers_for_db, find_closed_offers
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


def scrape_all_pages_to_excel(url=url_main): # Not in use, left just in case, not updated
    response = fetch_page(url)
    offers = download_data_from_search_results(response)
    
    for offer in offers:
        link_offer = offer.get("link")
        response = fetch_page(link_offer)
        offer_data = download_data_from_listing_page(response)
        cleaned_offer_data = transform_data(offer_data)
        cleaned_offer_data.pop('images')
        save_data_to_excel(cleaned_offer_data, 'output_data/data_katowice.xlsx')
        

def scrape_offer(offer_to_insert: dict) -> dict:
    """
    Scrapes data from a listing page for a new offer and transforms it into a structured format for insertion into the database.

    This function takes an offer that is not yet in the database, fetches the page using the offer's URL, 
    downloads the offer's data, cleans, and transforms it into a standardized format for database insertion.

    Args:
        offer_to_insert (dict): A dictionary containing offer data from Otodom, including 
                                'listing_id' and 'area', 'price', 'price_per_m', 'link' 
                                (single entry from download_data_from_search_results())

    Returns:
        dict: A dictionary containing the cleaned and transformed data for the offer, ready to be inserted into the database.

    Raises:
        Exception: If there is an error during the scraping process, such as issues with fetching the page or transforming the data
    """

    try:
        #print("Rozpoczynanie pobierania znalezionych nowych ofert:")

        offer_url = offer_to_insert.get("link")
        id = offer_to_insert.get("listing_id")

        response = fetch_page(offer_url)
        offer_data = download_data_from_listing_page(response)
        cleaned_offer_data = transform_data(offer_data)

        print(f"Dane oferty {id} zostały pobrane")

        return cleaned_offer_data
    except Exception as error:
        print(f"Error during scraping page offer: {error}")


def scrape_all_pages(url): # jednak NOT IN USE LEFT IN CASE (zbyt duzo pamieci na raz, wole kazda oferte analizowac na biezaco)
    """
    new_offers to lista słowników tak jak w danych wejściwoych czyli wynik download_data_from_search_results() (listing_id, area, price, price_per_m, link)
    need_update_offers to lista słowników z wartsociami id, new_price i new_price_per_m
    """
    all_offers_basic = download_data_from_search_results(url)

    print('-' * 40)
    print(f"Liczba znalezionych ofert: {len(all_offers_basic)}")
    print('-' * 40)

    need_update_offers, new_offers = categorize_offers_for_db(all_offers_basic)

    deleted_offers = find_closed_offers(all_offers_basic)

    print(f"Nowych ofert:{len(new_offers)}")
    print(f"Ofert, w których zmieniła się cena i wymagają update:{len(need_update_offers)}")
    print(f"Ofert, które zostały usunięte: {len(deleted_offers)}")

    #new_offers_to_database = scrape_offers(new_offers)

    return need_update_offers, new_offers, deleted_offers


def extract_city_from_url(url: str) ->str:
    pass
