from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class ListingBasic:
    listing_id: int
    area: float | None
    price: int | None
    price_per_m: float | None
    link: str


@dataclass
class ListingFull:
    listing_id: int
    offer_link: str
    active: bool = True
    detected_inactive_at: date | None = None

    title: str | None = None
    market: str | None = None
    advert_type: str | None = None
    creation_date: date | None = None
    creation_time: str | None = None
    pushed_up_at: str | None = None
    exclusive_offer: bool | None = None
    creation_source: str | None = None
    description_text: str | None = None

    area: float | None = None
    price: int | None = None
    price_per_m: int | None = None
    rent_amount: int | None = None

    rooms_num: int | None = None
    floor_num: int | None = None
    heating: str | None = None
    ownership: str | None = None
    proper_type: str | None = None
    construction_status: str | None = None
    energy_certificate: str | None = None

    features_utilities: list | None = None
    features_equipment: list | None = None
    features_additional_information: list | None = None
    security_types: list | None = None

    features: str | None = None

    voivodeship: str | None = None
    city: str | None = None
    district: str | None = None
    street: str | None = None

    building_build_year: int | None = None
    building_floors_num: int | None = None
    building_material: str | None = None
    building_type: str | None = None
    windows_type: str | None = None

    local_plan_url: str | None = None
    video_url: str | None = None
    view3d_url: str | None = None
    walkaround_url: str | None = None

    development_id: int | None = None
    development_title: str | None = None
    owner_id: int | None = None
    owner_name: str | None = None
    agency_id: int | None = None
    agency_name: str | None = None


@dataclass
class PlotListingFull:
    """A plot offer, keeping Otodom's variable plot attributes losslessly."""
    listing_id: int
    offer_link: str
    active: bool = True
    detected_inactive_at: date | None = None

    title: str | None = None
    description_text: str | None = None
    market: str | None = None
    advert_type: str | None = None
    advertiser_type: str | None = None
    creation_source: str | None = None
    creation_at: str | None = None
    modified_at: str | None = None
    pushed_up_at: str | None = None
    exclusive_offer: bool | None = None
    source_status: str | None = None

    area: float | None = None
    price: float | None = None
    price_per_m: float | None = None

    voivodeship: str | None = None
    city: str | None = None
    district: str | None = None
    street: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    plot_types: list[str] | None = None
    dimensions: str | None = None
    fence: str | None = None
    media_types: list[str] | None = None
    access_types: list[str] | None = None
    vicinity_types: list[str] | None = None

    owner_id: int | None = None
    owner_name: str | None = None
    agency_id: int | None = None
    agency_name: str | None = None
    local_plan_url: str | None = None
    video_url: str | None = None
    view3d_url: str | None = None
    walkaround_url: str | None = None