from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_event_contract_and_streaming_guards_are_present():
    producer = (ROOT / "ingest" / "kafka_utils.py").read_text(encoding="utf-8")
    streaming = (ROOT / "spark_jobs" / "streaming_bronze_utils.py").read_text(encoding="utf-8")

    for field in ("schema_version", "available_at", "quality_flags", "trace", "payload"):
        assert f'"{field}"' in producer
    assert '"ais-dlq"' in producer
    assert "withWatermark" in streaming
    assert "dropDuplicates" in streaming
    assert "MERGE INTO" in streaming
    assert "& ~late_condition" in streaming


def test_statistics_ui_uses_real_visualization_api_and_is_linked_from_map():
    app = (ROOT / "ui" / "src" / "App.jsx").read_text(encoding="utf-8")
    dashboard = (ROOT / "ui" / "src" / "pages" / "StatisticsDashboard.jsx").read_text(encoding="utf-8")
    map_dashboard = (ROOT / "ui" / "src" / "pages" / "AirQualityMapDashboard.jsx").read_text(encoding="utf-8")

    assert '"#/statistics"' in app
    assert 'href="#/statistics"' in map_dashboard
    for api_call in ("getForecastLatest", "getLiveHeatmapLatest", "getPM25TimeseriesLatest", "getStationsLatest"):
        assert api_call in dashboard
    assert "/mock/" not in dashboard
