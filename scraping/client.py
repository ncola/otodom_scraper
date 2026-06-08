import requests
import time
import random
import logging
from urllib.robotparser import RobotFileParser


def fetch_page(url: str) -> requests.Response:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept-Language": "pl-PL,pl;q=0.9"
        }

        html_response = requests.get(url, headers=headers)
        time.sleep(random.uniform(1, 2))

        if html_response.status_code == 200:
            return html_response
        else:
            logging.error(f"http error fetching page ({url}): {html_response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logging.exception(f"request exception while fetching page: {e}")
        return None


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
        logging.warning(f"error fetching robots.txt: {e}")
        return False
