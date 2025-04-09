import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from utils import save_data_to_excel, fetch_page, download_data_from_searching_page, download_data_from_listing_page, transform_data
from db.setup_and_manage_database import insert_new_listing

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"

def scrape_all_pages_to_excel(url=url_main):
    response = fetch_page(url)
    offers = download_data_from_searching_page(response)
    
    for offer in offers:
        link_offer = offer.get("link")
        response = fetch_page(link_offer)
        offer_data = download_data_from_listing_page(response)
        cleaned_offer_data = transform_data(offer_data)
        cleaned_offer_data.pop('images')
        save_data_to_excel(cleaned_offer_data, 'output_data/data_katowice.xlsx')

offers_data = []
def scrape_all_pages(url=url_main):
    response = fetch_page(url)
    offers = download_data_from_searching_page(response)
    
    print(f"Znaleziono: {len(offers)} ofert")
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
    return offers_data


#scrape_all_page_to_excel()
#data = scrape_all_pages()
#insert_new_listing(data)    


