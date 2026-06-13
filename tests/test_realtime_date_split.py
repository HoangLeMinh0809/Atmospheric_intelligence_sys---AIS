import pytest

from ais_architecture_logic import compute_historical_and_realtime_dates


def test_end_date_is_split_into_historical_end_and_realtime_day():
    split = compute_historical_and_realtime_dates("2026-05-29", "2026-05-31")

    assert split.historical_end_date == "2026-05-30"
    assert split.historical_window_start_utc == "2026-05-29T00:00:00Z"
    assert split.historical_window_end_utc == "2026-05-30T23:59:59Z"
    assert split.realtime_current_date == "2026-05-31"


def test_equal_start_and_end_date_does_not_create_negative_historical_window():
    with pytest.raises(ValueError, match="end_date - 1 day"):
        compute_historical_and_realtime_dates("2026-05-31", "2026-05-31")
