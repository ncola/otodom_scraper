from datetime import date
from unittest.mock import MagicMock
import pytest
from domain.normalize import transform_data
from db.repositories import (
    update_active_offers,
    update_deleted_offers,
    insert_new_listing,
    _build_listing_params,
    _build_features_bools,
    insert_into_apartments_sale_listings_table,
    check_if_offer_exists,
    check_if_price_changed,
    find_potentially_deleted_offers,
    find_offer_links,
)
from domain.models import ListingBasic
from tests.conftest import make_listing, listing_basic


# ---------- update_active_offers ----------

def test_update_active_offers_reads_old_price_before_overwriting():
    """SELECT must come before UPDATE — otherwise old_price in history would be wrong."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (480000,)

    update_active_offers((1, 460000, 7000), conn, cur)

    sql_calls = [c[0][0].strip() for c in cur.execute.call_args_list]

    select_idx = next(i for i, sql in enumerate(sql_calls) if sql.startswith("SELECT updated_price"))
    update_idx = next(i for i, sql in enumerate(sql_calls) if sql.startswith("UPDATE apartments_sale_listings"))

    assert select_idx < update_idx, "SELECT must happen before UPDATE"


def test_update_active_offers_records_correct_old_price_in_history():
    """Price history must store the price that was in DB before the update, not the new one."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (480000,)

    update_active_offers((1, 460000, 7000), conn, cur)

    insert_call = next(
        c for c in cur.execute.call_args_list
        if "INSERT INTO price_history" in c[0][0]
    )
    params = insert_call[0][1]

    assert params['old_price'] == 480000
    assert params['new_price'] == 460000


def test_update_active_offers_commits_on_success():
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (480000,)

    update_active_offers((1, 460000, 7000), conn, cur)

    conn.commit.assert_called_once()


def test_update_active_offers_rollbacks_on_error():
    conn = MagicMock()
    cur = MagicMock()
    cur.execute.side_effect = Exception("DB error")

    update_active_offers((1, 460000, 7000), conn, cur)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


# ---------- insert_into_apartments_sale_listings_table ----------

def test_insert_listing_sets_updated_price_equal_to_price(listing_raw):
    """On first insert updated_price must mirror price — no separate value yet."""
    params = _build_listing_params(listing_raw, location_id=42)
    assert params["updated_price"] == params["price"]


def test_insert_listing_sets_updated_price_per_m_equal_to_price_per_m(listing_raw):
    params = _build_listing_params(listing_raw, location_id=42)
    assert params["updated_price_per_m"] == params["price_per_m"]


def test_insert_listing_raises_when_location_missing(listing_raw):
    cur = MagicMock()
    cur.fetchone.return_value = None  # location not found

    with pytest.raises(ValueError, match="location not found"):
        insert_into_apartments_sale_listings_table(cur, listing_raw)


# ---------- insert_into_features_table ----------

def test_insert_features_marks_present_features_as_true():
    bools = _build_features_bools("internet balcony lift garage")

    assert bools['internet'] is True
    assert bools['balcony'] is True
    assert bools['lift'] is True
    assert bools['garage'] is True


def test_insert_features_marks_absent_features_as_false():
    bools = _build_features_bools("internet")

    assert bools['balcony'] is False
    assert bools['alarm'] is False
    assert bools['tv'] is False


def test_insert_features_with_none_features_does_not_crash():
    """features=None (e.g. pre-normalization or all-null listing) must not raise."""
    bools = _build_features_bools(None)

    assert bools['internet'] is False
    assert bools['balcony'] is False


# ---------- full pipeline: raw listing -> db-ready params ----------

def test_full_pipeline_produces_db_ready_params():
    """Checks that after normalization all params have types matching the DB schema."""
    listing = make_listing()
    transform_data(listing)
    params = _build_listing_params(listing, location_id=42)

    # DATE columns
    assert isinstance(params['creation_date'], date)

    # NUMERIC / INT columns — wrong type (e.g. string) would cause DB error
    assert isinstance(params['area'], float)
    assert isinstance(params['price'], int)
    assert isinstance(params['updated_price'], int)
    assert isinstance(params['price_per_m'], int)
    assert isinstance(params['updated_price_per_m'], int)
    assert isinstance(params['rooms_num'], int)
    assert isinstance(params['building_build_year'], int)
    assert isinstance(params['building_floors_num'], int)

    # BOOLEAN columns
    assert isinstance(params['exclusive_offer'], bool)
    assert isinstance(params['active'], bool)

    # TEXT columns that get normalized from raw string list format e.g. "['brick']"
    assert params['construction_status'] == "ready_to_use"
    assert params['building_material'] == "brick"
    assert params['building_type'] == "block"
    assert params['windows_type'] == "plastic"
    assert params['ownership'] == "full_ownership"



# ---------- check_if_price_changed ----------

def test_check_if_price_changed_returns_false_flag_when_unchanged():
    cur = MagicMock()
    cur.fetchone.return_value = (7, 549000)  # id_db=7, price in DB = 549000
    offer = ListingBasic(listing_id=12345678, area=65.5, price=549000, price_per_m=8381,
                         link="https://www.otodom.pl/pl/oferta/example-ID12345678")

    id_db, price_changed, price_per_m_changed = check_if_price_changed(offer, cur)

    assert id_db == 7
    assert price_changed is False
    assert price_per_m_changed is False


def test_check_if_price_changed_returns_new_values_when_changed():
    cur = MagicMock()
    cur.fetchone.return_value = (7, 549000)
    offer = ListingBasic(listing_id=12345678, area=65.5, price=520000, price_per_m=7900, link="https://www.otodom.pl/pl/oferta/example-ID12345678")

    id_db, new_price, new_price_per_m = check_if_price_changed(offer, cur)

    assert id_db == 7
    assert new_price == 520000
    assert new_price_per_m == 7900



# ---------- find_potentially_deleted_offers ----------

def test_find_potentially_deleted_offers_detects_missing_offer():
    """Offer in DB but not in fetched list -> should be in potentially_deleted."""
    cur = MagicMock()
    cur.fetchall.return_value = [
        (10, 11111111, 55.0),  # id_db=10, otodom_id=11111111
        (20, 22222222, 60.0),  # id_db=20, otodom_id=22222222 — this one missing from site
    ]
    fetched = [ListingBasic(listing_id=11111111, area=55.0, price=400000, price_per_m=7000, link="https://www.otodom.pl/pl/oferta/example-ID11111111")]

    result = find_potentially_deleted_offers(fetched, {"katowice"}, cur)

    assert 20 in result
    assert 10 not in result


def test_find_potentially_deleted_offers_empty_when_all_present():
    cur = MagicMock()
    cur.fetchall.return_value = [(10, 11111111, 55.0)]
    fetched = [ListingBasic(listing_id=11111111, area=55.0, price=400000, price_per_m=7000,
                            link="https://www.otodom.pl/pl/oferta/example-ID11111111")]

    result = find_potentially_deleted_offers(fetched, {"katowice"}, cur)

    assert len(result) == 0


# ---------- check_if_offer_exists ----------

def test_check_if_offer_exists_returns_true_when_found():
    cur = MagicMock()
    cur.fetchone.return_value = (7,)  # DB returned a row ->offer exists

    result = check_if_offer_exists(listing_basic(), cur)

    assert result is True


def test_check_if_offer_exists_returns_false_when_not_found():
    cur = MagicMock()
    cur.fetchone.return_value = None  # no row -> offer not in DB

    result = check_if_offer_exists(listing_basic(), cur)

    assert result is False


# ---------- update_deleted_offers ----------

def test_update_deleted_offers_marks_offer_as_inactive():
    """active must be set to False — True would leave a ghost offer in dashboard queries."""
    conn = MagicMock()
    cur = MagicMock()

    update_deleted_offers((99,), conn, cur)

    sql, params = cur.execute.call_args[0]
    assert params[0] is False      # active = False
    assert params[2] == 99         # correct id


def test_update_deleted_offers_commits_on_success():
    conn = MagicMock()
    cur = MagicMock()

    update_deleted_offers((99,), conn, cur)

    conn.commit.assert_called_once()


def test_update_deleted_offers_rollbacks_on_error():
    conn = MagicMock()
    cur = MagicMock()
    cur.execute.side_effect = Exception("DB error")

    update_deleted_offers((99,), conn, cur)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


# ---------- find_offer_links ----------

def test_find_offer_links_returns_id_and_link_pairs():
    cur = MagicMock()
    cur.fetchone.return_value = ("https://www.otodom.pl/pl/oferta/example-ID99",)

    result = find_offer_links({99}, cur)

    assert (99, "https://www.otodom.pl/pl/oferta/example-ID99") in result


def test_find_offer_links_skips_missing_offers():
    """If DB has no row for an id (data inconsistency), should not crash — just skip."""
    cur = MagicMock()
    cur.fetchone.return_value = None

    result = find_offer_links({99}, cur)

    assert len(result) == 0


# ---------- insert_new_listing ----------

def test_insert_new_listing_commits_on_success(listing_raw):
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.side_effect = [
        (1,),   # check_location_table -> location exists
        (1,),   # check_location_table called again inside insert_into_apartments
        (42,),  # insert_into_apartments RETURNING id
    ]

    insert_new_listing(listing_raw, conn, cur)

    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


def test_insert_new_listing_rollbacks_on_error(listing_raw):
    conn = MagicMock()
    cur = MagicMock()
    cur.execute.side_effect = Exception("DB error")

    insert_new_listing(listing_raw, conn, cur)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
