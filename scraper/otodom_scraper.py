import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from utils import save_data_to_excel, fetch_page, download_data_from_searching_page, download_data_from_listing_page

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"

def scrape_all_page_to_excel(url=url_main):
    response = fetch_page(url)
    offers = download_data_from_searching_page(response)
    
    for offer in offers:
        link_offer = offer.get("link")
        response = fetch_page(link_offer)
        offer_data = download_data_from_listing_page(response)
        save_data_to_excel(offer_data, 'data_katowice.xlsx')


def scrape_all_page(url=url_main):
    response = fetch_page(url)
    offers = download_data_from_searching_page(response)
    
    print(f"Znaleziono: {len(offers)} ofert")
    n=0
    for offer in offers:
        link_offer = offer.get("link")
        response = fetch_page(link_offer)
        print("-------------------")
        print(f"Numer oferty: {n}")
        offer_data = download_data_from_listing_page(response)
        n+=1
    return offer_data



scrape_all_page()
