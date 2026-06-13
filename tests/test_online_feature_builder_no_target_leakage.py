import pytest

from ais_architecture_logic import assert_no_future_target_columns, drop_future_target_columns


def test_future_target_columns_are_removed_from_online_feature_set():
    columns = ["pm25_mean", "wind_speed", "pm25_next_6h", "pm25_next_12h", "pm25_next_24h"]

    assert drop_future_target_columns(columns) == ["pm25_mean", "wind_speed"]


def test_future_target_columns_raise_clear_error_when_validating_online_features():
    with pytest.raises(ValueError, match="Future target columns"):
        assert_no_future_target_columns(["pm25_mean", "pm25_next_24h"])
