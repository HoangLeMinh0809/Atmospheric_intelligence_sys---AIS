from ais_architecture_logic import (
    assert_no_future_target_columns,
    compute_historical_and_realtime_dates,
    select_asof_context,
)
from unittest.mock import Mock


def test_end_to_end_logical_realtime_flow_uses_cassandra_latest_without_future_leakage():
    split = compute_historical_and_realtime_dates("2026-05-29", "2026-05-31")
    assert split.historical_end_date == "2026-05-30"
    assert split.realtime_current_date == "2026-05-31"

    batch_hours = ["2026-05-29T00:00:00Z", "2026-05-30T23:00:00Z"]
    assert all(not item.startswith("2026-05-31") for item in batch_hours)

    base_time = "2026-05-31T14:00:00Z"
    era5 = select_asof_context(
        [{"hour": "2026-05-31T13:00:00Z", "wind_speed": 3.2}, {"hour": "2026-05-31T15:00:00Z", "wind_speed": 9.9}],
        "hour",
        base_time,
    )
    hysplit = select_asof_context(
        [{"hour": "2026-05-31T14:00:00Z", "dominant_cluster": 2}, {"hour": "2026-05-31T16:00:00Z", "dominant_cluster": 9}],
        "hour",
        base_time,
    )
    s5p = select_asof_context([{"date": "2026-05-30", "s5p_no2_mean": 1.1}, {"date": "2026-06-01", "s5p_no2_mean": 9.9}], "date", "2026-05-31")

    feature_state = {
        "location_id": "hanoi",
        "base_time": base_time,
        "base_hour": base_time,
        "pm25_mean": 44.0,
        "wind_speed": era5["wind_speed"],
        "dominant_cluster": hysplit["dominant_cluster"],
        "s5p_no2_mean": s5p["s5p_no2_mean"],
        "feature_schema_hash": "schema-hash",
    }
    assert_no_future_target_columns(feature_state.keys())

    cassandra_feature_state = {}
    cassandra_forecast_latest = {}
    cassandra_feature_state[(feature_state["location_id"], feature_state["base_hour"])] = feature_state

    read_iceberg_serving_features = Mock(side_effect=AssertionError("Iceberg serving features must not be realtime input"))
    prediction_model = Mock(return_value={6: 46.0, 12: 49.0, 24: 52.0})

    realtime_input = cassandra_feature_state[("hanoi", base_time)]
    predictions = prediction_model(realtime_input)
    cassandra_forecast_latest["hanoi"] = {
        "location_id": "hanoi",
        "base_hour": base_time,
        "pm25_6h": predictions[6],
        "pm25_12h": predictions[12],
        "pm25_24h": predictions[24],
    }
    ui_latest = cassandra_forecast_latest["hanoi"]

    read_iceberg_serving_features.assert_not_called()
    prediction_model.assert_called_once_with(realtime_input)
    assert set(ui_latest) >= {"pm25_6h", "pm25_12h", "pm25_24h"}
    assert ui_latest["base_hour"] == "2026-05-31T14:00:00Z"
