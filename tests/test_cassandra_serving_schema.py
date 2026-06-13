import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _column_names(cql: str, table_name: str) -> set[str]:
    match = re.search(rf"CREATE TABLE IF NOT EXISTS .*\.{table_name} \((.*?)\n\)\s*(?:WITH|;)", cql, re.DOTALL)
    assert match, f"Missing CREATE TABLE for {table_name}"
    names = set()
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("PRIMARY KEY"):
            continue
        names.add(line.split()[0])
    return names


def test_feature_state_schema_contains_online_serving_fields():
    cql = (ROOT / "scripts" / "ensure_cassandra_online_schema.sh").read_text(encoding="utf-8")
    columns = _column_names(cql, "pm25_feature_state_by_location_hour")

    required = {
        "location_id",
        "base_time",
        "data_watermark",
        "feature_version",
        "feature_schema_hash",
        "pm25_now",
        "pm25_mean",
        "weather_time",
        "era5_time",
        "hysplit_time",
        "satellite_date",
        "era5_staleness_hours",
        "hysplit_staleness_hours",
        "s5p_staleness_days",
        "maiac_staleness_days",
        "updated_at",
    }
    assert required <= columns


def test_forecast_latest_schema_contains_latest_query_fields():
    cql = (ROOT / "scripts" / "ensure_cassandra_online_schema.sh").read_text(encoding="utf-8")
    columns = _column_names(cql, "pm25_forecast_latest_by_location")

    required = {
        "location_id",
        "base_hour",
        "model_version",
        "feature_version",
        "pm25_6h",
        "pm25_12h",
        "pm25_24h",
        "risk_6h",
        "risk_12h",
        "risk_24h",
        "updated_at",
    }
    assert required <= columns
