"""Parser for a single Otodom plot-sale offer page."""

import json
import logging

from bs4 import BeautifulSoup

from domain.models import PlotListingFull


def _text(value):
    return BeautifulSoup(value, "html.parser").get_text() if value is not None else None


def _as_list(value):
    if value is None:
        return None
    return value if isinstance(value, list) else [value]


def _district(location: dict) -> str | None:
    for item in location.get("reverseGeocoding", {}).get("locations", []):
        if item.get("locationLevel") == "district":
            return item.get("name")
    return None


def download_data_from_plot_listing_page(html_response) -> PlotListingFull:
    if html_response is None:
        raise ValueError("html response is None, can't parse plot listing page")

    soup = BeautifulSoup(html_response.text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if script is None or not script.string:
        raise ValueError("no __NEXT_DATA__ payload on plot listing page")

    ad = json.loads(script.string).get("props", {}).get("pageProps", {}).get("ad", {})
    if not ad.get("id"):
        raise ValueError("plot listing payload has no id")

    target = ad.get("target") or {}
    attributes = ad.get("attributes") or {}
    location = ad.get("location") or {}
    address = location.get("address") or {}
    coordinates = location.get("coordinates") or {}
    owner = ad.get("owner") or {}
    agency = ad.get("agency") or {}
    links = ad.get("links") or {}

    # attributes holds the unrounded price/m2 in observed plot responses.
    price_per_m = attributes.get("price_per_m", target.get("Price_per_m"))
    fence = target.get("Fence", attributes.get("fence"))
    if isinstance(fence, list):
        fence = fence[0] if fence else None

    street_data = address.get("street") or {}
    return PlotListingFull(
        listing_id=ad["id"],
        offer_link=ad.get("url") or f"https://www.otodom.pl/pl/oferta/{ad.get('slug', '')}",
        title=_text(ad.get("title")),
        description_text=_text(ad.get("description")),
        market=ad.get("market"),
        advert_type=ad.get("advertType"),
        advertiser_type=ad.get("advertiserType"),
        creation_source=ad.get("creationSource"),
        creation_at=ad.get("createdAt"),
        modified_at=ad.get("modifiedAt"),
        pushed_up_at=ad.get("pushedUpAt"),
        exclusive_offer=ad.get("exclusiveOffer"),
        source_status=ad.get("status"),
        active=ad.get("status") == "active",
        area=target.get("Area", attributes.get("m")),
        price=target.get("Price"),
        price_per_m=price_per_m,
        voivodeship=target.get("Province"),
        city=target.get("City"),
        district=_district(location),
        street=street_data.get("name"),
        latitude=coordinates.get("latitude"),
        longitude=coordinates.get("longitude"),
        plot_types=_as_list(target.get("Type", attributes.get("type"))),
        dimensions=target.get("Dimensions", attributes.get("dimensions")),
        fence=fence,
        media_types=_as_list(target.get("Media_types", attributes.get("media_types"))),
        access_types=_as_list(target.get("Access_types", attributes.get("access_types"))),
        vicinity_types=_as_list(target.get("Vicinity_types", attributes.get("vicinity_types"))),
        owner_id=owner.get("id") or None,
        owner_name=owner.get("name"),
        agency_id=agency.get("id") or None,
        agency_name=agency.get("name"),
        local_plan_url=links.get("localPlanUrl"),
        video_url=links.get("videoUrl"),
        view3d_url=links.get("view3dUrl"),
        walkaround_url=links.get("walkaroundUrl"),
    )


def get_plot_offer_status(offer_link: str) -> str | None:
    from scraping.client import fetch_page
    try:
        response = fetch_page(offer_link)
        if response is None:
            return "removed"
        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script is None or not script.string:
            return None
        return json.loads(script.string).get("props", {}).get("pageProps", {}).get("ad", {}).get("status")
    except Exception as error:
        logging.exception(f"error during getting plot offer status: {error}")
        return "removed"
