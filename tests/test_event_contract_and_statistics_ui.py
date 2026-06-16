# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


# Kiem tra event contract and streaming guards are present.
def test_event_contract_and_streaming_guards_are_present():
    producer = (ROOT / "ingest" / "kafka_utils.py").read_text(encoding="utf-8")
    streaming = (ROOT / "spark_jobs" / "streaming_bronze_utils.py").read_text(encoding="utf-8")

    for field in ("schema_version", "available_at", "quality_flags", "trace", "payload"):
        assert f'"{field}"' in producer
    assert '"ais-dlq"' in producer
    assert "withWatermark" in streaming
    assert "dropDuplicates" in streaming
    # Dung MERGE de upsert vao bang dich ma khong mat ban ghi cu.
    assert "MERGE INTO" in streaming
    assert "& ~late_condition" in streaming


# Kiem tra statistics ui uses real visualization api and is linked from map.
def test_statistics_ui_uses_real_visualization_api_and_is_linked_from_map():
    app = (ROOT / "ui" / "src" / "App.jsx").read_text(encoding="utf-8")
    dashboard = (ROOT / "ui" / "src" / "pages" / "StatisticsDashboard.jsx").read_text(encoding="utf-8")
    map_dashboard = (ROOT / "ui" / "src" / "pages" / "AirQualityMapDashboard.jsx").read_text(encoding="utf-8")

    assert '"#/statistics"' in app
    assert 'href="#/statistics"' in map_dashboard
    for api_call in ("getForecastLatest", "getLiveHeatmapLatest", "getPM25TimeseriesLatest", "getStationsLatest"):
        assert api_call in dashboard
    assert "/mock/" not in dashboard
