# File nay: tao feature, training table hoac serving table cho bai toan PM2.5.
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from hanoi_config import apply_asof_time, parse_asof_time

# Keep feature selection aligned with training.
# This import works in-repo and inside the Spark runtime image (/opt/ais).
for candidate in [
    Path(__file__).resolve().parents[1] / "ml",
    Path("/opt/ais/ml"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from train_hanoi_pm25 import FEATURE_COLUMNS  # noqa: E402


DEFAULT_SOURCE_TABLE = "ais.features.hanoi_pm25_master_hourly_gold"
DEFAULT_TARGET_TABLE = "ais.features.hanoi_pm25_serving_features_gold"
LEAKAGE_COLUMNS = {"pm25_next_6h", "pm25_next_12h", "pm25_next_24h"}
FEATURE_COLUMN_TYPES = {
    "pm25_median": "DOUBLE",
    "pm25_mean": "DOUBLE",
    "station_count": "INT",
    "coverage_avg": "DOUBLE",
    "vis_km": "DOUBLE",
    "uv": "DOUBLE",
    "condition_code": "INT",
    "is_day": "INT",
    "will_it_rain": "INT",
    "chance_of_rain": "INT",
    "wind_u10": "DOUBLE",
    "wind_v10": "DOUBLE",
    "wind_speed": "DOUBLE",
    "wind_dir": "DOUBLE",
    "pbl_height_m": "DOUBLE",
    "low_pbl": "BOOLEAN",
    "surface_pressure": "DOUBLE",
    "temperature_2m_c": "DOUBLE",
    "dewpoint_2m_c": "DOUBLE",
    "total_precipitation_mm": "DOUBLE",
    "s5p_no2_mean": "DOUBLE",
    "s5p_co_mean": "DOUBLE",
    "s5p_so2_mean": "DOUBLE",
    "s5p_o3_mean": "DOUBLE",
    "s5p_aer_ai_mean": "DOUBLE",
    "s5p_no2_valid_pct": "DOUBLE",
    "s5p_aer_ai_valid_pct": "DOUBLE",
    "aod_047_mean": "DOUBLE",
    "aod_055_mean": "DOUBLE",
    "aod_mean": "DOUBLE",
    "aod_max": "DOUBLE",
    "aod_valid_pct": "DOUBLE",
    "pm25_grad_n": "DOUBLE",
    "pm25_grad_s": "DOUBLE",
    "pm25_grad_e": "DOUBLE",
    "pm25_grad_w": "DOUBLE",
    "pm25_spatial_std": "DOUBLE",
    "pm25_grad_mag": "DOUBLE",
    "dominant_cluster": "INT",
    "n_traj": "INT",
    "traj_source_lat": "DOUBLE",
    "traj_source_lon": "DOUBLE",
    "traj_path_no2_mean": "DOUBLE",
    "traj_path_aer_mean": "DOUBLE",
    "traj_path_no2_aer_ratio": "DOUBLE",
    "hour_of_day": "INT",
    "day_of_week": "INT",
    "month": "INT",
    "season": "STRING",
    "is_weekend": "BOOLEAN",
    "hour_sin": "DOUBLE",
    "hour_cos": "DOUBLE",
    "dow_sin": "DOUBLE",
    "dow_cos": "DOUBLE",
    "month_sin": "DOUBLE",
    "month_cos": "DOUBLE",
    "is_rush_hour": "BOOLEAN",
    "pm25_lag_1h": "DOUBLE",
    "pm25_lag_3h": "DOUBLE",
    "pm25_lag_6h": "DOUBLE",
    "pm25_lag_12h": "DOUBLE",
    "pm25_lag_24h": "DOUBLE",
    "pm25_roll_mean_3h": "DOUBLE",
    "pm25_roll_mean_6h": "DOUBLE",
    "pm25_roll_mean_24h": "DOUBLE",
    "pm25_roll_max_24h": "DOUBLE",
    "pm25_roll_std_24h": "DOUBLE",
}
METADATA_COLUMN_TYPES = {
    "year": "INT",
    "month_partition": "INT",
    "spark_processed_at": "TIMESTAMP",
}


# Chuyen flag dang chuoi nhu 1/true/yes thanh boolean.
def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build PM2.5 serving feature gold table (K8s-ready)")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--asof-time", default=os.getenv("ASOF_TIME", os.getenv("SIMULATED_NOW", os.getenv("BASE_TIME", ""))))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "hanoi_pm25_core_v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    parser.add_argument("--dataset-version", default=os.getenv("DATASET_VERSION", ""))
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--location-name", default=os.getenv("LOCATION_NAME", "Hanoi"))
    parser.add_argument("--dry-run", default=os.getenv("DRY_RUN", "0"))
    return parser.parse_args()


# Khoi tao SparkSession voi Iceberg catalog, warehouse va HDFS config.
def build_spark() -> SparkSession:
    catalog = os.getenv("ICEBERG_CATALOG", "ais")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "")
    hdfs_namenode = (
        os.getenv("HDFS_NAMENODE")
        or os.getenv("HDFS_DEFAULT_FS")
        or os.getenv("HADOOP_DEFAULT_FS")
        or "hdfs://namenode:9000"
    )
    packages = os.getenv("SPARK_JARS_PACKAGES", "").strip()
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")

    builder = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder.appName("HanoiPM25ServingFeaturesGold")
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
    )
    if warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_namenode).config(
        "spark.hadoop.dfs.client.use.datanode.hostname",
        os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


# Loc du lieu theo khoang ngay start/end duoc yeu cau.
def apply_date_range(df: DataFrame, time_col: str, start_date: str, end_date: str) -> DataFrame:
    if start_date:
        df = df.filter(F.to_date(F.col(time_col)) >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date(F.col(time_col)) <= F.to_date(F.lit(end_date)))
    return df


# Tinh schema hash cho payload cho du lieu/du doan PM2.5.
def schema_hash_for(feature_cols: list[str]) -> str:
    payload = json.dumps({"features": feature_cols}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Kiem tra tinh dung dan cua du lieu/du doan PM2.5.
def validate_schema(df: DataFrame, expected_features: list[str]) -> dict:
    missing = [c for c in expected_features if c not in df.columns]
    unexpected_leakage = [c for c in sorted(LEAKAGE_COLUMNS) if c in df.columns]

    # Only consider null ratio for expected columns that exist.
    null_ratios = {}
    if expected_features and not missing:
        exprs = [F.mean(F.col(c).isNull().cast("double")).alias(c) for c in expected_features]
        row = df.select(*exprs).first()
        if row is not None:
            null_ratios = {c: float(row[c]) if row[c] is not None else None for c in expected_features}

    return {
        "missing": missing,
        "unexpected_leakage": unexpected_leakage,
        "null_ratios": null_ratios,
    }


# Tao bang serving features toi thieu, schema on dinh cho buoc infer/giong lai Cassandra state.
def ensure_table(spark: SparkSession, table_name: str) -> None:
    catalog = os.getenv("ICEBERG_CATALOG", "ais")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.features")
    # Create a minimal compatible schema; Iceberg will evolve if needed.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            base_hour TIMESTAMP,
            location_id STRING,
            location_name STRING,
            feature_version STRING,
            feature_set_name STRING,
            dataset_version STRING,
            schema_hash STRING,
            created_at TIMESTAMP
        )
        USING ICEBERG
        TBLPROPERTIES ('format-version'='2')
        """
    )
    existing = set(spark.table(table_name).columns)
    for column, dtype in {**FEATURE_COLUMN_TYPES, **METADATA_COLUMN_TYPES}.items():
        if column not in existing:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {column} {dtype}")


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()

    catalog = os.getenv("ICEBERG_CATALOG", "ais")
    source_table = os.getenv("SOURCE_TABLE", DEFAULT_SOURCE_TABLE)
    target_table = os.getenv("TARGET_TABLE", DEFAULT_TARGET_TABLE)

    full_refresh = as_bool(args.full_refresh)
    dry_run = as_bool(args.dry_run)

    spark = build_spark()

    feature_cols = list(FEATURE_COLUMNS)
    expected_cols = ["hour"] + feature_cols
    run_schema_hash = schema_hash_for(feature_cols)

    created_at = datetime.now(timezone.utc)

    try:
        # Doc bang nguon tu Iceberg truoc khi bien doi du lieu.
        df = spark.read.table(source_table)
        df = apply_date_range(df, "hour", args.start_date, args.end_date)
        df = apply_asof_time(df, "hour", parse_asof_time(args.asof_time))

        # base_hour = hour (required by TODO)
        df = df.withColumnRenamed("hour", "base_hour")

        # Select only training FEATURE_COLUMNS + serving metadata.
        missing_input = [c for c in expected_cols if c != "hour" and c not in df.columns and c != "base_hour"]
        if missing_input:
            raise ValueError(f"Missing columns in source table {source_table}: {missing_input}")

        select_exprs = [F.col("base_hour")] + [F.col(c) for c in feature_cols]
        out = df.select(*select_exprs)

        # Drop leakage if present (defensive) and enforce it is gone.
        for col in LEAKAGE_COLUMNS:
            if col in out.columns:
                out = out.drop(col)

        out = (
            out.withColumn("feature_version", F.lit(args.feature_version))
            .withColumn("feature_set_name", F.lit(args.feature_set_name))
            .withColumn("dataset_version", F.lit(args.dataset_version))
            .withColumn("schema_hash", F.lit(run_schema_hash))
            .withColumn("location_id", F.lit(args.location_id))
            .withColumn("location_name", F.lit(args.location_name))
            .withColumn("created_at", F.lit(created_at))
            .withColumn("spark_processed_at", F.current_timestamp())
            .withColumn("year", F.year("base_hour").cast("int"))
            .withColumn("month_partition", F.month("base_hour").cast("int"))
        )

        # Validate schema matches FEATURE_COLUMNS (i.e. all features exist, no leakage).
        validation = validate_schema(out, feature_cols)
        if validation["missing"]:
            raise ValueError(f"Schema validation failed. Missing features: {validation['missing']}")
        if validation["unexpected_leakage"]:
            raise ValueError(f"Schema validation failed. Leakage columns present: {validation['unexpected_leakage']}")

        input_count = df.count()
        output_count = out.count()
        bounds = out.agg(F.min("base_hour").alias("min_base_hour"), F.max("base_hour").alias("max_base_hour")).first()

        print(
            "job=hanoi_pm25_serving_features_gold "
            f"input_count={input_count} "
            f"output_count={output_count} "
            f"min_base_hour={bounds['min_base_hour'] if bounds else None} "
            f"max_base_hour={bounds['max_base_hour'] if bounds else None} "
            f"feature_version={args.feature_version} "
            f"schema_hash={run_schema_hash} "
            f"dry_run={int(dry_run)}"
        )

        if dry_run:
            # Print a small QC summary.
            null_preview = {k: v for k, v in list(validation["null_ratios"].items())[:10]}
            print(f"job=hanoi_pm25_serving_features_gold null_ratio_preview={null_preview}")
            print("job=hanoi_pm25_serving_features_gold status=dry_run_success")
            return

        ensure_table(spark, target_table)

        if full_refresh:
            out.writeTo(target_table).overwritePartitions()
            print("job=hanoi_pm25_serving_features_gold status=written mode=overwritePartitions")
            return

        # Idempotent upsert by (base_hour, location_id, feature_version).
        out.createOrReplaceTempView("src")
        spark.sql(
            f"""
                MERGE INTO {target_table} t
            USING src s
            ON t.base_hour = s.base_hour
              AND t.location_id = s.location_id
              AND t.feature_version = s.feature_version
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        print("job=hanoi_pm25_serving_features_gold status=written mode=merge")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
