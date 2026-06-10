import pytest
from domain.models import ListingFull, ListingBasic


def make_listing(**kwargs) -> ListingFull:
    """Creates a ListingFull with ALL fields filled in real Otodom formats.
    Use kwargs to override specific fields — e.g. make_listing(price=None).
    """
    defaults = dict(
        listing_id=12345678,
        offer_link="https://www.otodom.pl/pl/oferta/przestronne-3-pok-balkon-katowice-koszutka-ID12345678",
        active=True,
        detected_inactive_at=None,

        # basic info
        title="Przestronne 3-pokojowe z balkonem, Koszutka",
        market="secondary",
        advert_type="agency",
        creation_date="2025-03-12T09:45:00Z",  # ISO string — as from Otodom JSON
        creation_time=None,                      # always null from Otodom, we can parse it from creation_date 
        pushed_up_at="2025-04-01T08:00:00Z",    # same ISO format as creation_date, null when not promoted
        exclusive_offer=False,
        creation_source="posting_form",
        description_text="Sprzedam mieszkanie 3-pokojowe w Koszutce. Stan bardzo dobry.",

        # area is a string from Otodom JSON, price and price_per_m are ints
        area="65.5",
        price=549000,
        price_per_m=8381,
        rent_amount=450,

        # apartment details
        rooms_num="['3']",                       # string list format
        floor_num="['floor_3']",                 # string list format
        heating="urban",
        ownership="pełna własność",
        proper_type="mieszkanie",
        construction_status="['ready_to_use']",  # string list format
        energy_certificate="['E']",

        # features — raw lists as from Otodom JSON
        features_utilities=["internet", "cable-television", "phone"],
        features_equipment=["washing_machine", "dishwasher", "fridge", "stove", "oven", "tv"],
        features_additional_information=["balcony", "basement", "lift"],
        security_types="['roller_shutters', 'anti_burglary_door', 'entryphone', 'monitoring', 'closed_area']",
        features=None,                           # filled by normalize

        # location
        voivodeship="slaskie",
        city="katowice",
        district="Koszutka",
        street="ul. Słoneczna",

        # building — strings as returned by Otodom JSON
        building_build_year="1998",
        building_floors_num="10",
        building_material="['brick']",           # string list format
        building_type="['block']",               # string list format
        windows_type="['plastic']",              # string list format

        # extra links — empty string when not provided (Otodom never sends null here)
        local_plan_url="",
        video_url="",
        view3d_url="",
        walkaround_url="",

        # seller
        development_id=0,
        development_title=None,
        owner_id=9876543,
        owner_name="Jan Kowalski",
        agency_id=1234567,
        agency_name="ABC Nieruchomości",
    )
    return ListingFull(**{**defaults, **kwargs})


@pytest.fixture
def listing_raw() -> ListingFull:
    """Complete listing before normalization."""
    return make_listing()


def listing_basic(
    listing_id=12345678,
    area=65.5,
    price=549000,
    price_per_m=8381,
    link="https://www.otodom.pl/pl/oferta/przestronne-3-pok-balkon-katowice-koszutka-ID12345678",
) -> ListingBasic:
    """Creates a ListingBasic with defaults; pass arguments to override."""
    return ListingBasic(
        listing_id=listing_id,
        area=area,
        price=price,
        price_per_m=price_per_m,
        link=link,
    )

