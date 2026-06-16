# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
import pytest

from ais_architecture_logic import assert_no_future_target_columns, drop_future_target_columns


# Kiem tra future target columns are removed from online feature set.
def test_future_target_columns_are_removed_from_online_feature_set():
    columns = ["pm25_mean", "wind_speed", "pm25_next_6h", "pm25_next_12h", "pm25_next_24h"]

    assert drop_future_target_columns(columns) == ["pm25_mean", "wind_speed"]


# Kiem tra future target columns raise clear error when validating online features.
def test_future_target_columns_raise_clear_error_when_validating_online_features():
    with pytest.raises(ValueError, match="Future target columns"):
        assert_no_future_target_columns(["pm25_mean", "pm25_next_24h"])
