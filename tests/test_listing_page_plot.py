import json
from types import SimpleNamespace

from domain.normalize import transform_plot_data
from scraping.listing_page_plot import download_data_from_plot_listing_page, get_plot_offer_status


def _response(ad):
    payload = {"props": {"pageProps": {"ad": ad}}}
    return SimpleNamespace(text=f'<script id="__NEXT_DATA__">{json.dumps(payload)}</script>')


def _plot_ad(**overrides):
    ad = {
        "id": 68221708,
        "slug": "dzialka-ID4CfA8",
        "url": "https://www.otodom.pl/pl/oferta/dzialka-ID4CfA8",
        "title": "Działka budowlana",
        "description": "<p>Opis <b>działki</b></p>",
        "market": "ALL", "advertType": "AGENCY", "advertiserType": "business",
        "createdAt": "2026-07-17T12:14:31Z", "modifiedAt": "2026-07-17T12:14:45Z",
        "status": "active", "target": {
            "Area": "570", "Price": 260000, "Price_per_m": 456,
            "Province": "slaskie", "City": "katowice", "Type": ["building"],
            "Dimensions": "16x32", "Fence": ["y"],
            "Media_types": ["electricity", "water"], "Access_types": ["asphalt"],
            "Vicinity_types": ["forest"],
        },
        "attributes": {"price_per_m": "456.14"}, "characteristics": [{"key": "m", "value": "570"}],
        "location": {"coordinates": {"latitude": 50.18229, "longitude": 18.980175},
                     "address": {"street": {"name": "ul. Sołtysia"}},
                     "reverseGeocoding": {"locations": [{"locationLevel": "district", "name": "Podlesie"}]}},
        "owner": {"id": 7, "name": "Owner"}, "agency": {"id": 8, "name": "Agency"},
        "links": {"localPlanUrl": "", "videoUrl": "", "view3dUrl": "", "walkaroundUrl": ""},
    }
    ad.update(overrides)
    return ad


def test_plot_parser_preserves_variable_target_fields_and_precise_price_per_m():
    listing = transform_plot_data(download_data_from_plot_listing_page(_response(_plot_ad())))
    assert listing.plot_types == ["building"]
    assert listing.media_types == ["electricity", "water"]
    assert listing.price_per_m == 456.14
    assert listing.source_target["Dimensions"] == "16x32"
    assert listing.source_characteristics == [{"key": "m", "value": "570"}]
    assert listing.latitude == 50.18229
    assert listing.description_text == "Opis działki"
    assert listing.market == "all"
    assert listing.advert_type == "agency"




def test_plot_parser_accepts_missing_optional_plot_fields():
    ad = _plot_ad(target={"Area": "900", "Price": 100000}, attributes={}, location={})
    listing = download_data_from_plot_listing_page(_response(ad))
    assert listing.plot_types is None
    assert listing.media_types is None
    assert listing.district is None


def test_plot_status_returns_removed_when_fetch_fails(monkeypatch):
    monkeypatch.setattr("scraping.client.fetch_page", lambda _: (_ for _ in ()).throw(RuntimeError("network error")))

    assert get_plot_offer_status("https://example.test/plot") == "removed"
