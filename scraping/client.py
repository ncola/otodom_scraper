import requests
import time
import random
import logging
from urllib.robotparser import RobotFileParser


BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

RETRY_BACKOFF_SECONDS = [2, 5, 10]


def fetch_page(url: str) -> requests.Response:
    last_status = None
    for attempt, backoff in enumerate(RETRY_BACKOFF_SECONDS, start=1):
        try:
            html_response = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
            time.sleep(random.uniform(1, 2))

            if html_response.status_code == 200:
                return html_response

            last_status = html_response.status_code
            if html_response.status_code == 403 or html_response.status_code >= 500:
                logging.warning(f"http {html_response.status_code} fetching page ({url}), attempt {attempt}/{len(RETRY_BACKOFF_SECONDS)}")
                if attempt < len(RETRY_BACKOFF_SECONDS):
                    time.sleep(backoff)
                    continue
            else:
                logging.error(f"http error fetching page ({url}): {html_response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            logging.warning(f"request exception while fetching page (attempt {attempt}/{len(RETRY_BACKOFF_SECONDS)}): {e}")
            if attempt < len(RETRY_BACKOFF_SECONDS):
                time.sleep(backoff)
                continue
            logging.exception(f"request exception while fetching page: {e}")
            return None

    logging.error(f"http error fetching page ({url}): {last_status} after {len(RETRY_BACKOFF_SECONDS)} attempts")
    return None


def is_allowed_to_scrape(url: str) -> bool:
    domain = '/'.join(url.split('/')[:3])
    robots_url = domain + '/robots.txt'

    try:
        response = requests.get(robots_url, headers=BROWSER_HEADERS, timeout=30)
        response.raise_for_status()
        rp = RobotFileParser()
        rp.parse(response.text.splitlines())
        return rp.can_fetch("*", url)

    except requests.exceptions.RequestException as e:
        logging.warning(f"error fetching robots.txt: {e}")
        return False
