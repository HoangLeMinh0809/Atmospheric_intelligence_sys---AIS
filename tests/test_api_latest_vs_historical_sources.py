from ais_architecture_logic import resolve_latest_source
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_forecast_latest_without_date_resolves_to_cassandra():
    assert resolve_latest_source(date=None, artifact="forecast") == "cassandra"


def test_forecast_with_date_resolves_to_historical_cache():
    assert resolve_latest_source(date="2026-05-30", artifact="forecast") == "historical_cache"


def test_heatmap_latest_without_date_resolves_to_cassandra():
    assert resolve_latest_source(date=None, artifact="heatmap") == "cassandra"


def test_heatmap_with_date_resolves_to_historical_cache():
    assert resolve_latest_source(date="2026-05-30", artifact="heatmap") == "historical_cache"


def test_visualization_api_latest_vs_historical_contract_is_encoded():
    api = (ROOT / "serving" / "visualization_api" / "main.py").read_text(encoding="utf-8")

    assert "if date is None and cassandra_forecast_enabled()" in api
    assert "return JSONResponse(load_cassandra_forecast(location_id))" in api
    assert "live_heatmap_does_not_accept_date" in api
    assert 'find_layer(load_manifest(date), "pm25_heatmap"' in api
