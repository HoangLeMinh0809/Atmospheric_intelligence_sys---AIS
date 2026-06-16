# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from ais_architecture_logic import filter_records_asof


# Kiem tra online builder filters openaq and weather to base time or earlier.
def test_online_builder_filters_openaq_and_weather_to_base_time_or_earlier():
    base_time = "2026-05-31T14:00:00Z"
    openaq_rows = [
        {"event_time": "2026-05-31T13:00:00Z", "pm25": 42.0},
        {"event_time": "2026-05-31T13:30:00Z", "pm25": 43.0},
        {"event_time": "2026-05-31T14:00:00Z", "pm25": 44.0},
        {"event_time": "2026-05-31T14:30:00Z", "pm25": 99.0},
    ]
    weather_rows = [
        {"event_time": "2026-05-31T13:00:00Z", "temp_c": 31.0},
        {"event_time": "2026-05-31T14:00:00Z", "temp_c": 32.0},
        {"event_time": "2026-05-31T15:00:00Z", "temp_c": 40.0},
    ]

    scoped_openaq = filter_records_asof(openaq_rows, "event_time", base_time)
    scoped_weather = filter_records_asof(weather_rows, "event_time", base_time)

    assert [row["event_time"] for row in scoped_openaq] == [
        "2026-05-31T13:00:00Z",
        "2026-05-31T13:30:00Z",
        "2026-05-31T14:00:00Z",
    ]
    assert [row["event_time"] for row in scoped_weather] == [
        "2026-05-31T13:00:00Z",
        "2026-05-31T14:00:00Z",
    ]
    assert max(row["event_time"] for row in scoped_openaq) <= base_time
    assert max(row["event_time"] for row in scoped_weather) <= base_time
