# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


# Kiem tra spark submit and shared config default to utc.
def test_spark_submit_and_shared_config_default_to_utc():
    hanoi_config = (ROOT / "spark_jobs" / "hanoi_config.py").read_text(encoding="utf-8")
    compose_submit = (ROOT / "scripts" / "submit_spark.sh").read_text(encoding="utf-8")
    k8s_submit = (ROOT / "scripts" / "submit_spark_k8s.sh").read_text(encoding="utf-8")

    assert 'SPARK_SQL_SESSION_TIMEZONE = os.getenv("SPARK_SQL_SESSION_TIMEZONE", "UTC")' in hanoi_config
    assert "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TIMEZONE:-UTC}" in compose_submit
    assert "spark.sql.session.timeZone=\\${SPARK_SQL_SESSION_TIMEZONE:-UTC}" in k8s_submit


# Kiem tra weather processing prefers utc epoch over local time string.
def test_weather_processing_prefers_utc_epoch_over_local_time_string():
    weather_ingest = (ROOT / "ingest" / "ingest_weather.py").read_text(encoding="utf-8")
    weather_streaming = (ROOT / "spark_jobs" / "weather_streaming.py").read_text(encoding="utf-8")
    online_builder = (ROOT / "spark_jobs" / "online_pm25_feature_builder.py").read_text(encoding="utf-8")

    assert "window.start_utc.astimezone(WEATHER_SOURCE_TIMEZONE)" in weather_ingest
    assert "event_time_utc < window_start or event_time_utc > window_end" in weather_ingest
    assert "timestamp_seconds(time_epoch)" in weather_streaming
    assert "to_utc_timestamp(to_timestamp(time, 'yyyy-MM-dd HH:mm'), tz_id)" in weather_streaming
    assert "timestamp_seconds(time_epoch)" in online_builder


# Kiem tra era5 partitions use utc hour.
def test_era5_partitions_use_utc_hour():
    era5_silver = (ROOT / "spark_jobs" / "era5_surface_hanoi_silver.py").read_text(encoding="utf-8")

    assert '"hour": hour_utc_naive' in era5_silver
    assert '"year": hour_utc_naive.year' in era5_silver
    assert "local_hour" not in era5_silver
