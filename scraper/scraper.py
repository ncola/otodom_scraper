import requests,json
from urllib.robotparser import RobotFileParser
from scraper.utils import save_data_to_excel
from bs4 import BeautifulSoup

from scraper.fetch_and_parse import fetch_page, download_data_from_search_results, download_data_from_listing_page
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


def scrape_all_pages(url=url_main):
    try:
        all_offers_basic = download_data_from_search_results(url)

        print('-' * 40)
        print(f"Liczba znalezionych ofert: {len(all_offers_basic)}")
        print('-' * 40)
        print("Rozpoczynanie pobierania znalezionych ofert:")

        offers_data = []
        n=1
        for offer in all_offers_basic[25:30]:
            offer_url = offer.get("link")
            id = offer.get("listing_id")

            response = fetch_page(offer_url)
            offer_data = download_data_from_listing_page(response)
            print(f"{n}. Pobrano ofertę o id {id}")
            cleaned_offer_data = transform_data(offer_data)
            offers_data.append(cleaned_offer_data)
            n+=1 

        return offers_data
    except Exception as error:
        print(f"Error during scraping all pages: {error}")


"""
DO USUNIECIA:
all_offers_basic = []
def scrape_all_pages2(url=url_main):
    try:
        response_page_count = fetch_page(url)
        page_count = get_total_pages(response_page_count)

        for page in range(1, 10): #page_count
            # zbieramy ogłoszenia ze wszystkich stron
            page_link = f"{url}&page={page}"

            print(f"Pobieranie strony {page}")
            response = fetch_page(page_link)
            basic_info_offers = download_data_from_searching_page(response)

            all_offers_basic.extend(basic_info_offers) 

        print('-'*40)
        print(f"Pobrano dane z {page_count} stron, liczba znalezionych ofert: {len(all_offers_basic)}")
        print('-'*40)

        print("Rozpoczynanie pobierania znalezionych ofert:")
        
        n=0
        for offer in all_offers_basic[45:47]:
            link_offer = offer.get("link")
            id = offer.get("listing_id")
            response = fetch_page(link_offer)
            print("-------------------")
            offer_data = download_data_from_listing_page(response)
            print(f"Pobrano ofertę o id {id}")
            cleaned_offer_data = transform_data(offer_data)
            offers_data.append(cleaned_offer_data)
            n+=1
        return offers_data
    except Exception as error:
        print(f"Error during scraping pages: {error}")


offers_data = []
def scrape_all_pages1(url=url_main):
    response = fetch_page(url)
    offers = download_data_from_searching_page(response)
    
    print(f"\nZnaleziono: {len(offers)} ofert\n")
    n=0
    for offer in offers[:2]:
        link_offer = offer.get("link")
        id = offer.get("listing_id")
        response = fetch_page(link_offer)
        print("-------------------")
        offer_data = download_data_from_listing_page(response)
        print(f"Pobrano ofertę o id {id}")
        cleaned_offer_data = transform_data(offer_data)
        offers_data.append(cleaned_offer_data)
        n+=1
    return offers_data"""

# FUNKCJA sprawdzajaca czy pobrana oferta juz istnieje w bazie (na podstawie id oferty i metrazu) 

# jezeli istnieje juz w bazie to sprawdz czy cena jest taka sama

# jezeli cena jest taka sama to nie rob nic, jezeli inna to update ceny 

# FUNCKJA updateowania ofert 

#scrape_all_page_to_excel()
#data = scrape_all_pages()
#insert_new_listing(data)    


