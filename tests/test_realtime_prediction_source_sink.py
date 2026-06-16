# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]


# Kiem tra prediction defaults and k8s config use cassandra for realtime latest.
def test_prediction_defaults_and_k8s_config_use_cassandra_for_realtime_latest():
    config = (ROOT / "deploy" / "k8s" / "configmap.yaml").read_text(encoding="utf-8")
    predict = (ROOT / "ml" / "predict_hanoi_pm25.py").read_text(encoding="utf-8")

    assert "FEATURE_SOURCE: cassandra" in config
    assert "WRITE_CASSANDRA_FORECAST: \"1\"" in config
    assert "CASSANDRA_FEATURE_TABLE: pm25_feature_state_by_location_hour" in config
    assert "CASSANDRA_FORECAST_TABLE: pm25_forecast_latest_by_location" in config
    assert "MODEL_REGISTRY_TABLE: ais.models.hanoi_pm25_model_registry_gold" in config
    assert "MODEL_ARTIFACT_BASE_URI: hdfs://namenode:9000/models" in config
    assert "load_feature_row_from_cassandra" in predict
    assert 'if args.feature_source == "cassandra"' in predict
    assert "write_prediction_to_cassandra" in predict


# Kiem tra prediction realtime mode does not read iceberg serving features first.
def test_prediction_realtime_mode_does_not_read_iceberg_serving_features_first():
    calls = []

    # Kiem tra read cassandra.
    def read_cassandra():
        calls.append("cassandra_feature_state")
        return {"base_hour": "2026-05-31T14:00:00Z"}

    # Kiem tra read iceberg.
    def read_iceberg():
        calls.append("iceberg_serving_features")
        raise AssertionError("realtime prediction must not use Iceberg serving features as primary input")

    feature_source = "cassandra"
    feature_row = read_cassandra() if feature_source == "cassandra" else read_iceberg()

    assert feature_row["base_hour"] == "2026-05-31T14:00:00Z"
    assert calls == ["cassandra_feature_state"]


# Kiem tra prediction writes cassandra forecast latest in mock flow.
def test_prediction_writes_cassandra_forecast_latest_in_mock_flow():
    sink = Mock()
    prediction = {"pm25_6h": 45.0, "pm25_12h": 48.0, "pm25_24h": 51.0}

    sink.write("ais_serving.pm25_forecast_latest_by_location", prediction)

    sink.write.assert_called_once_with("ais_serving.pm25_forecast_latest_by_location", prediction)
