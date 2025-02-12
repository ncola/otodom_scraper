import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from .utils import save_data_to_excel, fetch_page, download_data_from_searching_page, download_data_from_listing_page

url_main = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/slaskie/katowice?by=LATEST&direction=DESC"

def scrape():
    response = fetch_page(url_main)
    offers = download_data_from_searching_page(response)
    
    for offer in offers[5:20]:
        link_offer = offer.get("link")
        response = fetch_page(link_offer)
        offer_data = download_data_from_listing_page(response)
        save_data_to_excel(offer_data, 'data_katowice.xlsx')

