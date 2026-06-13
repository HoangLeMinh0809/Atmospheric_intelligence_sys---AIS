from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

for candidate in [
    Path(__file__).resolve().parents[1] / "ml",
    Path("/opt/ais/ml"),
    Path("/opt/ml"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hanoi_config import HDFS_NAMENODE, ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_table_names  # noqa: E402
from train_hanoi_pm25 import BOOLEAN_FEATURES, FEATURE_COLUMNS, FEATURE_SCHEMA_HASH, INTEGER_FEATURES  # noqa: E402

ONLINE_INTEGER_FEATURES = set(INTEGER_FEATURES) | {"chance_of_rain"}


DEFAULT_KEYSPACE = "ais_serving"
DEFAULT_TARGET_TABLE = "pm25_feature_state_by_location_hour"

OPENAQ_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("provider", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("datetime_utc", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("coverage_pct", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
    ]
)

WEATHER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("time", StringType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("dewpoint_c", DoubleType(), True),
        StructField("condition_code", IntegerType(), True),
        StructField("wind_kph", DoubleType(), True),
        StructField("wind_degree", IntegerType(), True),
        StructField("pressure_mb", DoubleType(), True),
        StructField("precip_mm", DoubleType(), True),
        StructField("vis_km", DoubleType(), True),
        StructField("uv", DoubleType(), True),
        StructField("will_it_rain", IntegerType(), True),
        StructField("chance_of_rain", IntegerType(), True),
        StructField("ingest_time", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build latest online PM2.5 feature state from realtime Bronze")
    parser.add_argument("--base-time", default=os.getenv("BASE_TIME", os.getenv("BASE_HOUR", "")))
    parser.add_argument("--lookback-hours", type=int, default=int(os.getenv("ONLINE_FEATURE_LOOKBACK_HOURS", "72")))
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--location-name", default=os.getenv("LOCATION_NAME", "Hanoi"))
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "hanoi_pm25_core_v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    parser.add_argument("--dataset-version", default=os.getenv("DATASET_VERSION", "hanoi_pm25_v1"))
    parser.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", DEFAULT_KEYSPACE))
    parser.add_argument("--target-table", default=os.getenv("CASSANDRA_FEATURE_TABLE", DEFAULT_TARGET_TABLE))
    parser.add_argument("--dry-run", default=os.getenv("DRY_RUN", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def allow_kafka_fallback() -> bool:
    return as_bool(os.getenv("ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK", "1"))


def parse_base_time(raw: str) -> datetime:
    value = (raw or "").strip()
    if not value:
        now = datetime.now(timezone.utc)
        simulation_date = os.getenv("REALTIME_SIMULATION_DATE", "").strip()
        if simulation_date:
            parsed_date = datetime.fromisoformat(simulation_date).date()
            return datetime(
                parsed_date.year,
                parsed_date.month,
                parsed_date.day,
                now.hour,
                tzinfo=timezone.utc,
            )
        return now.replace(minute=0, second=0, microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def build_spark() -> SparkSession:
    packages = os.getenv("SPARK_JARS_PACKAGES")
    if packages is None:
        packages = (
            "org.apache.hadoop:hadoop-client:3.3.4,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
        )
    builder = (
        SparkSession.builder.appName("OnlinePM25FeatureBuilder")
        .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", HDFS_NAMENODE))
        .config("spark.hadoop.dfs.client.use.datanode.hostname", os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"))
        .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra"))
        .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
    )
    if packages.strip():
        builder = builder.config("spark.jars.packages", packages.strip())
    return builder.getOrCreate()


def table_or_empty(spark: SparkSession, table: str) -> DataFrame | None:
    try:
        return spark.table(table)
    except Exception as exc:
        print(f"job=online_pm25_feature_builder warning=table_unavailable table={table} message={exc}")
        return None


def collect_one(df: DataFrame) -> dict[str, Any]:
    rows = df.limit(1).collect()
    return rows[0].asDict() if rows else {}


def kafka_batch(spark: SparkSession, topic: str, schema: StructType) -> DataFrame | None:
    if not allow_kafka_fallback():
        print(
            "job=online_pm25_feature_builder "
            f"warning=kafka_batch_fallback_disabled topic={topic} "
            "hint=set ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK=1 to enable"
        )
        return None
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        return None
    try:
        return (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(F.from_json(F.col("json_str"), schema).alias("data"))
            .select("data.*")
        )
    except Exception as exc:
        print(f"job=online_pm25_feature_builder warning=kafka_batch_unavailable topic={topic} message={exc}")
        return None


def default_value(name: str) -> Any:
    if name == "season":
        return "unknown"
    if name in BOOLEAN_FEATURES:
        return False
    if name in ONLINE_INTEGER_FEATURES:
        return 0
    return 0.0


def season_for(month: int) -> str:
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


def as_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return datetime(int(value.year), int(value.month), int(value.day))
    return None


def latest_context(spark: SparkSession, base_time: datetime, feature_version: str) -> dict[str, Any]:
    tables = get_table_names()
    base_naive = base_time.replace(tzinfo=None)
    df = table_or_empty(spark, tables["serving_features_gold"])
    context = {}
    if df is not None:
        context.update(collect_one(
            df.filter(F.col("feature_version") == F.lit(feature_version))
            .filter(F.col("base_hour") <= F.lit(base_naive))
            .orderBy(F.col("base_hour").desc())
        ))

    # Daily satellite cadence: latest product/date at or before base date.
    s5p = table_or_empty(spark, tables["sentinel5p_silver"])
    if s5p is not None:
        s5p_norm = (
            s5p
            .withColumn("product_norm", F.upper(F.col("product")))
            .withColumn("product_norm", F.when(F.col("product_norm") == "AER", F.lit("AER_AI")).otherwise(F.col("product_norm")))
            .filter(F.col("date") <= F.to_date(F.lit(base_naive)))
            .filter((F.col("overpass_time_utc").isNull()) | (F.col("overpass_time_utc") <= F.lit(base_naive)))
            .select("product_norm", "date", "overpass_time_utc", "value_mean", "valid_pct")
        )
        w = Window.partitionBy("product_norm").orderBy(F.col("date").desc_nulls_last(), F.col("overpass_time_utc").desc_nulls_last())
        latest = s5p_norm.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
        s5p_row = collect_one(latest.agg(
            F.max(F.when(F.col("product_norm") == "NO2", F.col("value_mean"))).alias("s5p_no2_mean"),
            F.max(F.when(F.col("product_norm") == "CO", F.col("value_mean"))).alias("s5p_co_mean"),
            F.max(F.when(F.col("product_norm") == "SO2", F.col("value_mean"))).alias("s5p_so2_mean"),
            F.max(F.when(F.col("product_norm") == "O3", F.col("value_mean"))).alias("s5p_o3_mean"),
            F.max(F.when(F.col("product_norm") == "AER_AI", F.col("value_mean"))).alias("s5p_aer_ai_mean"),
            F.max(F.when(F.col("product_norm") == "NO2", F.col("valid_pct"))).alias("s5p_no2_valid_pct"),
            F.max(F.when(F.col("product_norm") == "AER_AI", F.col("valid_pct"))).alias("s5p_aer_ai_valid_pct"),
            F.max("date").alias("_s5p_date"),
        ))
        context.update({k: v for k, v in s5p_row.items() if v is not None})

    maiac = table_or_empty(spark, tables["maiac_silver"])
    if maiac is not None:
        maiac_row = collect_one(
            maiac
            .filter(F.col("date") <= F.to_date(F.lit(base_naive)))
            .orderBy(F.col("date").desc_nulls_last())
            .select(
                "aod_047_mean",
                "aod_055_mean",
                "aod_mean",
                "aod_max",
                F.col("valid_pct").alias("aod_valid_pct"),
                F.col("date").alias("_maiac_date"),
            )
        )
        context.update({k: v for k, v in maiac_row.items() if v is not None})

    # Hourly cadence: latest ERA5/HYSPLIT rows at or before base_time.
    era5 = table_or_empty(spark, tables["era5_surface_silver"])
    if era5 is not None:
        era5_cols = [
            "wind_u10",
            "wind_v10",
            "wind_speed",
            "wind_dir",
            "pbl_height_m",
            "low_pbl",
            "surface_pressure",
            "temperature_2m_c",
            "dewpoint_2m_c",
            "total_precipitation_mm",
        ]
        selected_cols = [F.col(name) for name in era5_cols if name in era5.columns]
        if selected_cols:
            era5_row = collect_one(
                era5
                .filter(F.col("hour") <= F.lit(base_naive))
                .orderBy(F.col("hour").desc_nulls_last())
                .select(*selected_cols, F.col("hour").alias("_era5_time"))
            )
            context.update({k: v for k, v in era5_row.items() if v is not None})

    traj = table_or_empty(spark, tables["trajectory_hourly_silver"])
    if traj is not None:
        traj_row = collect_one(
            traj
            .filter(F.col("hour") <= F.lit(base_naive))
            .orderBy(F.col("hour").desc_nulls_last())
            .select(
                F.col("dominant_cluster").cast("int").alias("dominant_cluster"),
                F.col("n_traj").cast("int").alias("n_traj"),
                F.col("source_lat").cast("double").alias("traj_source_lat"),
                F.col("source_lon").cast("double").alias("traj_source_lon"),
                F.col("path_no2_mean").cast("double").alias("traj_path_no2_mean"),
                F.col("path_aer_mean").cast("double").alias("traj_path_aer_mean"),
                F.col("path_no2_aer_ratio").cast("double").alias("traj_path_no2_aer_ratio"),
                F.col("hour").alias("_hysplit_time"),
            )
        )
        context.update({k: v for k, v in traj_row.items() if v is not None})

    gradient = table_or_empty(spark, tables["openaq_gradient_silver"])
    if gradient is not None:
        gradient_row = collect_one(
            gradient
            .filter(F.col("hour") <= F.lit(base_naive))
            .orderBy(F.col("hour").desc_nulls_last())
            .select(
                "pm25_grad_n",
                "pm25_grad_s",
                "pm25_grad_e",
                "pm25_grad_w",
                "pm25_spatial_std",
                "pm25_grad_mag",
            )
        )
        context.update({k: v for k, v in gradient_row.items() if v is not None})

    # Do not allow training labels into online inference even if a context table contains them.
    for target_col in ("pm25_next_6h", "pm25_next_12h", "pm25_next_24h"):
        context.pop(target_col, None)

    return context


def latest_openaq_stats(spark: SparkSession, base_time: datetime, lookback_hours: int) -> dict[str, Any]:
    tables = get_table_names()
    df = table_or_empty(spark, tables["openaq_bronze"])
    from_kafka = False
    if df is None:
        df = kafka_batch(spark, os.getenv("OPENAQ_KAFKA_TOPIC", "openaq-hourly"), OPENAQ_SCHEMA)
        if df is None:
            return {}
        from_kafka = True
        df = (
            df.withColumn("event_time", F.to_timestamp("datetime_utc"))
            .withColumn("sensor_id", F.col("sensor_id").cast("string"))
            .withColumn("coverage_pct", F.col("coverage_pct").cast("double"))
        )
    start = base_time - timedelta(hours=lookback_hours)
    scoped = (
        df.filter(F.col("parameter") == F.lit("pm25"))
        .filter(F.col("event_time") <= F.lit(base_time.replace(tzinfo=None)))
        .filter(F.col("event_time") >= F.lit(start.replace(tzinfo=None)))
        .withColumn("base_hour", F.date_trunc("hour", F.col("event_time")))
    )
    hourly = (
        scoped.groupBy("base_hour")
        .agg(
            F.avg("value").alias("pm25_mean"),
            F.expr("percentile_approx(value, 0.5)").alias("pm25_median"),
            F.countDistinct("sensor_id").cast("int").alias("station_count"),
            F.avg("coverage_pct").alias("coverage_avg"),
            F.max("event_time").alias("data_watermark"),
        )
        .orderBy(F.col("base_hour").desc())
    )
    latest_rows = hourly.limit(1).collect()
    if not latest_rows and not from_kafka:
        df = kafka_batch(spark, os.getenv("OPENAQ_KAFKA_TOPIC", "openaq-hourly"), OPENAQ_SCHEMA)
        if df is None:
            return {}
        df = (
            df.withColumn("event_time", F.to_timestamp("datetime_utc"))
            .withColumn("sensor_id", F.col("sensor_id").cast("string"))
            .withColumn("coverage_pct", F.col("coverage_pct").cast("double"))
        )
        scoped = (
            df.filter(F.col("parameter") == F.lit("pm25"))
            .filter(F.col("event_time") <= F.lit(base_time.replace(tzinfo=None)))
            .filter(F.col("event_time") >= F.lit(start.replace(tzinfo=None)))
            .withColumn("base_hour", F.date_trunc("hour", F.col("event_time")))
        )
        hourly = (
            scoped.groupBy("base_hour")
            .agg(
                F.avg("value").alias("pm25_mean"),
                F.expr("percentile_approx(value, 0.5)").alias("pm25_median"),
                F.countDistinct("sensor_id").cast("int").alias("station_count"),
                F.avg("coverage_pct").alias("coverage_avg"),
                F.max("event_time").alias("data_watermark"),
            )
            .orderBy(F.col("base_hour").desc())
        )
        latest_rows = hourly.limit(1).collect()
    if not latest_rows:
        return {}
    latest = latest_rows[0].asDict()
    history = {row["base_hour"]: row["pm25_mean"] for row in hourly.collect() if row["pm25_mean"] is not None}

    def lag(hours: int) -> float | None:
        return history.get(base_time.replace(tzinfo=None) - timedelta(hours=hours))

    values_3h = [value for hour, value in history.items() if hour >= base_time.replace(tzinfo=None) - timedelta(hours=3)]
    values_6h = [value for hour, value in history.items() if hour >= base_time.replace(tzinfo=None) - timedelta(hours=6)]
    values_24h = [value for hour, value in history.items() if hour >= base_time.replace(tzinfo=None) - timedelta(hours=24)]
    latest.update(
        {
            "pm25_lag_1h": lag(1),
            "pm25_lag_3h": lag(3),
            "pm25_lag_6h": lag(6),
            "pm25_lag_12h": lag(12),
            "pm25_lag_24h": lag(24),
            "pm25_roll_mean_3h": sum(values_3h) / len(values_3h) if values_3h else None,
            "pm25_roll_mean_6h": sum(values_6h) / len(values_6h) if values_6h else None,
            "pm25_roll_mean_24h": sum(values_24h) / len(values_24h) if values_24h else None,
            "pm25_roll_max_24h": max(values_24h) if values_24h else None,
            "pm25_roll_std_24h": float(math.sqrt(sum((v - (sum(values_24h) / len(values_24h))) ** 2 for v in values_24h) / len(values_24h))) if values_24h else None,
        }
    )
    return latest


def latest_weather_stats(spark: SparkSession, base_time: datetime) -> dict[str, Any]:
    tables = get_table_names()
    df = table_or_empty(spark, tables["weather_bronze"])
    from_kafka = False
    if df is None:
        df = kafka_batch(spark, os.getenv("WEATHER_KAFKA_TOPIC", "weather_history"), WEATHER_SCHEMA)
        if df is None:
            return {}
        from_kafka = True
        df = df.withColumn("event_time", F.to_timestamp("time", "yyyy-MM-dd HH:mm"))
    rows = (
        df.filter(F.col("event_time") <= F.lit(base_time.replace(tzinfo=None)))
        .orderBy(F.col("event_time").desc())
        .limit(1)
        .collect()
    )
    if not rows and not from_kafka:
        df = kafka_batch(spark, os.getenv("WEATHER_KAFKA_TOPIC", "weather_history"), WEATHER_SCHEMA)
        if df is not None:
            rows = (
                df.withColumn("event_time", F.to_timestamp("time", "yyyy-MM-dd HH:mm"))
                .filter(F.col("event_time") <= F.lit(base_time.replace(tzinfo=None)))
                .orderBy(F.col("event_time").desc())
                .limit(1)
                .collect()
            )
    if not rows:
        return {}
    row = rows[0].asDict()
    wind_kph = row.get("wind_kph")
    wind_degree = row.get("wind_degree")
    radians = math.radians(float(wind_degree or 0))
    wind_speed = float(wind_kph or 0) / 3.6
    return {
        "weather_time": row.get("event_time"),
        "vis_km": row.get("vis_km"),
        "uv": row.get("uv"),
        "condition_code": row.get("condition_code"),
        "is_day": row.get("is_day"),
        "will_it_rain": row.get("will_it_rain"),
        "chance_of_rain": row.get("chance_of_rain"),
        "wind_speed": wind_speed,
        "wind_dir": float(wind_degree or 0),
        "wind_u10": -wind_speed * math.sin(radians),
        "wind_v10": -wind_speed * math.cos(radians),
        "surface_pressure": row.get("pressure_mb"),
        "temperature_2m_c": row.get("temp_c"),
        "dewpoint_2m_c": row.get("dewpoint_c"),
        "total_precipitation_mm": row.get("precip_mm"),
    }


def build_row(args: argparse.Namespace, context: dict[str, Any], openaq: dict[str, Any], weather: dict[str, Any], base_time: datetime) -> dict[str, Any]:
    base_hour = base_time.replace(tzinfo=None)
    row = {name: context.get(name, default_value(name)) for name in FEATURE_COLUMNS}
    row.update({k: v for k, v in openaq.items() if k in FEATURE_COLUMNS and v is not None})
    row.update({k: v for k, v in weather.items() if k in FEATURE_COLUMNS and v is not None})

    base_naive = base_time.replace(tzinfo=None)
    row.update(
        {
            "hour_of_day": int(base_time.hour),
            "day_of_week": int(base_time.weekday()),
            "month": int(base_time.month),
            "season": season_for(base_time.month),
            "is_weekend": base_time.weekday() >= 5,
            "hour_sin": math.sin(2 * math.pi * base_time.hour / 24),
            "hour_cos": math.cos(2 * math.pi * base_time.hour / 24),
            "dow_sin": math.sin(2 * math.pi * base_time.weekday() / 7),
            "dow_cos": math.cos(2 * math.pi * base_time.weekday() / 7),
            "month_sin": math.sin(2 * math.pi * base_time.month / 12),
            "month_cos": math.cos(2 * math.pi * base_time.month / 12),
            "is_rush_hour": base_time.hour in {7, 8, 17, 18},
        }
    )

    s5p_time = as_timestamp(context.get("_s5p_date"))
    maiac_time = as_timestamp(context.get("_maiac_date"))
    era5_time = as_timestamp(context.get("_era5_time") or context.get("base_hour"))
    hysplit_time = as_timestamp(context.get("_hysplit_time") or context.get("base_hour"))
    satellite_time = s5p_time or maiac_time or context.get("base_hour")
    data_watermark = openaq.get("data_watermark") or weather.get("weather_time") or base_naive

    row.update(
        {
            "location_id": args.location_id,
            "feature_version": args.feature_version,
            "base_hour": base_hour,
            "base_time": base_naive,
            "location_name": args.location_name,
            "feature_set_name": args.feature_set_name,
            "dataset_version": args.dataset_version,
            "schema_hash": FEATURE_SCHEMA_HASH,
            "feature_schema_hash": FEATURE_SCHEMA_HASH,
            "created_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "loaded_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "data_watermark": data_watermark,
            "pm25_now": openaq.get("pm25_mean") or row.get("pm25_mean"),
            "openaq_time": openaq.get("data_watermark"),
            "weather_time": weather.get("weather_time"),
            "era5_time": era5_time,
            "hysplit_time": hysplit_time,
            "satellite_date": satellite_time,
            "s5p_staleness_days": (base_naive.date() - s5p_time.date()).days if hasattr(s5p_time, "date") else None,
            "maiac_staleness_days": (base_naive.date() - maiac_time.date()).days if hasattr(maiac_time, "date") else None,
            "era5_staleness_hours": int((base_naive - era5_time).total_seconds() / 3600) if era5_time else None,
            "hysplit_staleness_hours": int((base_naive - hysplit_time).total_seconds() / 3600) if hysplit_time else None,
            "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
    )
    for name in ONLINE_INTEGER_FEATURES:
        if row.get(name) is not None:
            row[name] = int(row[name])
    for name in BOOLEAN_FEATURES:
        if row.get(name) is not None:
            row[name] = bool(row[name])
    for name in FEATURE_COLUMNS:
        if name not in ONLINE_INTEGER_FEATURES and name not in BOOLEAN_FEATURES and name != "season" and row.get(name) is not None:
            row[name] = float(row[name])
    return row


def output_schema() -> StructType:
    fields = [
        StructField("location_id", StringType(), False),
        StructField("feature_version", StringType(), False),
        StructField("base_hour", TimestampType(), False),
        StructField("base_time", TimestampType(), True),
        StructField("location_name", StringType(), True),
        StructField("feature_set_name", StringType(), True),
        StructField("dataset_version", StringType(), True),
        StructField("schema_hash", StringType(), True),
        StructField("feature_schema_hash", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("loaded_at", TimestampType(), True),
        StructField("data_watermark", TimestampType(), True),
        StructField("pm25_now", DoubleType(), True),
        StructField("openaq_time", TimestampType(), True),
        StructField("weather_time", TimestampType(), True),
        StructField("era5_time", TimestampType(), True),
        StructField("hysplit_time", TimestampType(), True),
        StructField("satellite_date", TimestampType(), True),
        StructField("s5p_staleness_days", IntegerType(), True),
        StructField("maiac_staleness_days", IntegerType(), True),
        StructField("era5_staleness_hours", IntegerType(), True),
        StructField("hysplit_staleness_hours", IntegerType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
    for name in FEATURE_COLUMNS:
        if name in BOOLEAN_FEATURES:
            fields.append(StructField(name, BooleanType(), True))
        elif name in ONLINE_INTEGER_FEATURES:
            fields.append(StructField(name, IntegerType(), True))
        elif name == "season":
            fields.append(StructField(name, StringType(), True))
        else:
            fields.append(StructField(name, DoubleType(), True))
    return StructType(fields)


def main() -> None:
    args = parse_args()
    dry_run = as_bool(args.dry_run)
    base_time = parse_base_time(args.base_time)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        context = latest_context(spark, base_time, args.feature_version)
        openaq = latest_openaq_stats(spark, base_time, args.lookback_hours)
        weather = latest_weather_stats(spark, base_time)
        if not openaq:
            raise RuntimeError(f"No OpenAQ realtime Bronze rows found at or before base_time={base_time.isoformat()}")

        row = build_row(args, context, openaq, weather, base_time)
        out = spark.createDataFrame([row], schema=output_schema())
        count = out.count()
        print(
            "job=online_pm25_feature_builder "
            f"base_time={base_time.isoformat()} "
            f"watermark={row.get('data_watermark')} "
            f"keyspace={args.keyspace} target_table={args.target_table} "
            f"output_count={count} dry_run={int(dry_run)}"
        )
        if dry_run:
            out.show(truncate=False)
            print("job=online_pm25_feature_builder status=dry_run_success")
            return
        (
            out.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(keyspace=args.keyspace, table=args.target_table)
            .save()
        )
        print(f"job=online_pm25_feature_builder status=written cassandra_write_count={count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
