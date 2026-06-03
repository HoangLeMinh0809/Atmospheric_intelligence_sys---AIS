from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from hanoi_config import (
    HDFS_NAMENODE,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    TABLES,
    get_visualization_cache_base_uri,
    get_visualization_cluster_labels,
    get_visualization_config,
    get_visualization_horizons,
    get_visualization_region_bbox,
)


def build_spark(app_name: str) -> SparkSession:
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"))
    )
    return builder.getOrCreate()


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-time", default=os.getenv("BASE_TIME", ""))
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--horizons", default=os.getenv("VIS_HORIZONS", ""))
    parser.add_argument("--grid-resolution-deg", type=float, default=_env_float("VIS_GRID_RESOLUTION_DEG", 0.0))
    parser.add_argument("--product-version", default=os.getenv("VIS_PRODUCT_VERSION", ""))
    parser.add_argument("--schema-version", default=os.getenv("VIS_SCHEMA_VERSION", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--dry-run", default=os.getenv("DRY_RUN", "0"))


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else default


def as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_base_time(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def end_of_date(value: str | None) -> datetime | None:
    if not value:
        return None
    day = datetime.fromisoformat(value.strip()).replace(tzinfo=timezone.utc)
    return day.replace(hour=23, minute=0, second=0, microsecond=0)


def visualization_runtime(args: argparse.Namespace) -> dict[str, Any]:
    cfg = get_visualization_config()
    horizons = [int(v.strip()) for v in args.horizons.split(",") if v.strip()] if args.horizons else get_visualization_horizons()
    return {
        "bbox": get_visualization_region_bbox(),
        "cluster_labels": get_visualization_cluster_labels(),
        "cache_base_uri": get_visualization_cache_base_uri(),
        "freshness_max_minutes": int(cfg.get("freshness_max_minutes", 180)),
        "forward_plume_required": bool(cfg.get("forward_plume_required", False)),
        "grid_resolution_deg": float(args.grid_resolution_deg or cfg.get("grid_resolution_deg", 0.1)),
        "horizons": horizons,
        "obs_history_hours": int(cfg.get("observation_history_hours", 48)),
        "max_trajectories": int(os.getenv("VIS_MAX_TRAJECTORIES", cfg.get("max_trajectories", 150))),
        "max_points_per_trajectory": int(os.getenv("VIS_MAX_POINTS_PER_TRAJECTORY", cfg.get("max_points_per_trajectory", 100))),
        "max_geojson_features": int(os.getenv("VIS_MAX_GEOJSON_FEATURES", cfg.get("max_geojson_features", 5000))),
        "product_version": args.product_version or str(cfg.get("product_version", "windy_v1")),
        "schema_version": args.schema_version or str(cfg.get("schema_version", "1")),
    }


def risk_value(pm25: float | None) -> str:
    if pm25 is None:
        return "unknown"
    if pm25 < 35:
        return "low"
    if pm25 < 75:
        return "medium"
    if pm25 < 150:
        return "high"
    return "very_high"


def risk_expr(col: str) -> F.Column:
    return (
        F.when(F.col(col).isNull(), F.lit("unknown"))
        .when(F.col(col) < F.lit(35), F.lit("low"))
        .when(F.col(col) < F.lit(75), F.lit("medium"))
        .when(F.col(col) < F.lit(150), F.lit("high"))
        .otherwise(F.lit("very_high"))
    )


def run_id(prefix: str, base_time: datetime | None, product_version: str) -> str:
    stamp = iso_z(base_time or utc_now()) or iso_z(utc_now())
    payload = f"{prefix}:{stamp}:{product_version}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def apply_date_range(df: DataFrame, time_col: str, start_date: str, end_date: str) -> DataFrame:
    if start_date:
        df = df.filter(F.to_date(F.col(time_col)) >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date(F.col(time_col)) <= F.to_date(F.lit(end_date)))
    return df


def read_table_if_exists(spark: SparkSession, table_name: str) -> DataFrame | None:
    try:
        return spark.read.table(table_name)
    except Exception as exc:
        print(f"visualization_common table_missing_or_unreadable={table_name} error={type(exc).__name__}: {exc}")
        return None


def latest_row(df: DataFrame, time_col: str, filters: list[F.Column] | None = None) -> dict[str, Any] | None:
    if filters:
        for condition in filters:
            df = df.filter(condition)
    rows = df.orderBy(F.col(time_col).desc()).limit(1).collect()
    return rows[0].asDict(recursive=True) if rows else None


def latest_row_asof(
    df: DataFrame,
    time_col: str,
    asof_time: datetime | None,
    filters: list[F.Column] | None = None,
) -> dict[str, Any] | None:
    if asof_time is not None:
        df = df.filter(F.col(time_col) <= F.lit(asof_time.replace(tzinfo=None)))
    return latest_row(df, time_col, filters=filters)


def fallback_forecast_values(latest_pm25: float | None, trend_per_6h: float | None = None) -> dict[int, float]:
    base = float(latest_pm25 or 0.0)
    trend = max(min(float(trend_per_6h or 0.0), 18.0), -18.0)
    diurnal = {6: 3.0, 12: -4.0, 24: 1.5}
    values = {}
    for horizon in [6, 12, 24]:
        projected = base + trend * (horizon / 6.0) + diurnal[horizon]
        values[horizon] = max(0.0, projected)
    return values


def station_trend_per_6h(station_df: DataFrame | None, base_time: datetime, lookback_hours: int = 24) -> float:
    if station_df is None:
        return 0.0
    start_time = base_time - timedelta(hours=lookback_hours)
    rows = (
        station_df.filter(station_df.pm25.isNotNull())
        .filter(station_df.hour >= start_time)
        .filter(station_df.hour <= base_time)
        .groupBy("hour")
        .agg(F.avg("pm25").alias("pm25_value"))
        .orderBy("hour")
        .collect()
    )
    if len(rows) < 2:
        return 0.0
    first = float(rows[0]["pm25_value"] or 0.0)
    last = float(rows[-1]["pm25_value"] or 0.0)
    hours = max((rows[-1]["hour"] - rows[0]["hour"]).total_seconds() / 3600.0, 1.0)
    return (last - first) / hours * 6.0


def grid_cells(bbox: dict[str, float], resolution: float) -> list[dict[str, float | str]]:
    cells = []
    lat = bbox["south"]
    while lat < bbox["north"] - 1e-9:
        lon = bbox["west"]
        while lon < bbox["east"] - 1e-9:
            lat_min = round(lat, 6)
            lat_max = round(min(lat + resolution, bbox["north"]), 6)
            lon_min = round(lon, 6)
            lon_max = round(min(lon + resolution, bbox["east"]), 6)
            cells.append(
                {
                    "cell_id": f"{lat_min:.3f}_{lon_min:.3f}",
                    "lat": round((lat_min + lat_max) / 2, 6),
                    "lon": round((lon_min + lon_max) / 2, 6),
                    "lat_min": lat_min,
                    "lat_max": lat_max,
                    "lon_min": lon_min,
                    "lon_max": lon_max,
                }
            )
            lon += resolution
        lat += resolution
    return cells


def point_geojson(lon: float | None, lat: float | None) -> str | None:
    if lon is None or lat is None:
        return None
    return json.dumps({"type": "Point", "coordinates": [float(lon), float(lat)]}, separators=(",", ":"))


def polygon_geojson(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> str:
    coords = [
        [lon_min, lat_min],
        [lon_max, lat_min],
        [lon_max, lat_max],
        [lon_min, lat_max],
        [lon_min, lat_min],
    ]
    return json.dumps({"type": "Polygon", "coordinates": [coords]}, separators=(",", ":"))


def line_geojson(points: list[dict[str, Any]]) -> str:
    coords = []
    for point in points:
        lon = point.get("lon")
        lat = point.get("lat")
        alt = point.get("alt_m")
        if lon is None or lat is None:
            continue
        if alt is None:
            coords.append([float(lon), float(lat)])
        else:
            coords.append([float(lon), float(lat), float(alt)])
    return json.dumps({"type": "LineString", "coordinates": coords}, separators=(",", ":"))


def feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def hdfs_write_text(spark: SparkSession, uri: str, payload: str) -> None:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(conf)
    parent = path.getParent()
    if parent is not None:
        fs.mkdirs(parent)
    out = fs.create(path, True)
    try:
        out.write(bytearray(payload.encode("utf-8")))
    finally:
        out.close()


def payload_checksum(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def cache_uri(*parts: str) -> str:
    clean = [part.strip("/") for part in parts if part]
    return "/".join([get_visualization_cache_base_uri(), *clean])


def write_product(df: DataFrame, table: str, dry_run: bool) -> int:
    count = df.count()
    if dry_run:
        return count
    df.writeTo(table).overwritePartitions()
    return count


def distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_tables() -> dict[str, str]:
    return TABLES.copy()
