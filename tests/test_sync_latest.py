from unittest.mock import MagicMock
from services.sync_latest import _process_offers
from tests.conftest import make_listing, listing_basic


# ---------- _process_offers ----------

def test_new_offer_is_saved(mocker):
    conn, cur = MagicMock(), MagicMock()
    offer = listing_basic()

    mocker.patch("services.sync_latest.repo.check_if_offer_exists", return_value=False)
    mocker.patch("services.sync_latest._scrape_offer", return_value=make_listing())
    mocker.patch("services.sync_latest.repo.insert_new_listing", return_value=1)

    new_count, updated_count, stop_early = _process_offers([offer], conn, cur)

    assert new_count == 1
    assert updated_count == 0
    assert stop_early is False


def test_stops_early_on_known_offer(mocker):
    """First known offer should trigger early stop — second offer must not be processed."""
    known = listing_basic(listing_id=11111111)
    new = listing_basic(listing_id=22222222)

    mocker.patch("services.sync_latest.repo.check_if_offer_exists", return_value=True)
    mocker.patch("services.sync_latest.repo.check_if_price_changed", return_value=(None, None, None))
    insert_mock = mocker.patch("services.sync_latest.repo.insert_new_listing")

    conn, cur = MagicMock(), MagicMock()
    new_count, updated_count, stop_early = _process_offers([known, new], conn, cur)

    assert stop_early is True
    insert_mock.assert_not_called()


def test_price_update_on_early_stop(mocker):
    """Price change should still be saved even when stopping early."""
    offer = listing_basic()

    mocker.patch("services.sync_latest.repo.check_if_offer_exists", return_value=True)
    mocker.patch("services.sync_latest.repo.check_if_price_changed", return_value=(1, 480000, 6900))
    update_mock = mocker.patch("services.sync_latest.repo.update_active_offers")

    conn, cur = MagicMock(), MagicMock()
    _process_offers([offer], conn, cur)

    update_mock.assert_called_once_with((1, 480000, 6900), conn, cur)


def test_failed_scrape_is_skipped(mocker):
    """If scraping fails, offer should be skipped without crashing or stopping early."""
    offer = listing_basic()

    mocker.patch("services.sync_latest.repo.check_if_offer_exists", return_value=False)
    mocker.patch("services.sync_latest._scrape_offer", return_value=None)
    insert_mock = mocker.patch("services.sync_latest.repo.insert_new_listing")

    conn, cur = MagicMock(), MagicMock()
    new_count, updated_count, stop_early = _process_offers([offer], conn, cur)

    insert_mock.assert_not_called()
    assert new_count == 0
    assert stop_early is False
