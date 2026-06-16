# File nay: tao feature, training table hoac serving table cho bai toan PM2.5.
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for candidate in [
    Path(__file__).resolve().parents[1] / "ml",
    Path("/opt/ais/ml"),
    Path("/opt/ml"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hanoi_config import HDFS_NAMENODE, ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_table_names  # noqa: E402
from train_hanoi_pm25 import FEATURE_COLUMNS, FEATURE_SCHEMA_HASH  # noqa: E402


DEFAULT_KEYSPACE = "ais_serving"
DEFAULT_TARGET_TABLE = "pm25_feature_state_by_location_hour"


# Chuyen flag dang chuoi nhu 1/true/yes thanh boolean.
def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish PM2.5 model-ready serving features to Cassandra")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--base-hour", default=os.getenv("BASE_HOUR", ""))
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--location-name", default=os.getenv("LOCATION_NAME", "Hanoi"))
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "hanoi_pm25_core_v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    parser.add_argument("--dataset-version", default=os.getenv("DATASET_VERSION", ""))
    parser.add_argument("--source-table", default=os.getenv("SERVING_FEATURE_TABLE", ""))
    parser.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", DEFAULT_KEYSPACE))
    parser.add_argument("--target-table", default=os.getenv("CASSANDRA_FEATURE_TABLE", DEFAULT_TARGET_TABLE))
    parser.add_argument("--latest-only", default=os.getenv("CASSANDRA_FEATURE_LATEST_ONLY", "0"))
    parser.add_argument("--dry-run", default=os.getenv("DRY_RUN", "0"))
    return parser.parse_args()


# Khoi tao SparkSession voi Iceberg catalog, warehouse va HDFS config.
def build_spark() -> SparkSession:
    packages = os.getenv("SPARK_JARS_PACKAGES")
    if packages is None:
        packages = (
            "org.apache.hadoop:hadoop-client:3.3.4,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
        )
    packages = packages.strip()
    cassandra_host = os.getenv("CASSANDRA_HOST", "cassandra").strip() or "cassandra"
    cassandra_port = os.getenv("CASSANDRA_PORT", "9042").strip() or "9042"
    builder = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder.appName("PM25ServingFeaturesToCassandra")
        .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", HDFS_NAMENODE))
        .config("spark.hadoop.dfs.client.use.datanode.hostname", os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"))
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.cassandra.connection.port", cassandra_port)
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


# Chuan hoa va loc moc thoi gian cho du lieu/du doan PM2.5.
def apply_time_filters(df, args: argparse.Namespace):
    if args.base_hour:
        df = df.filter(F.col("base_hour") == F.to_timestamp(F.lit(args.base_hour)))
    if args.start_date:
        df = df.filter(F.to_date("base_hour") >= F.to_date(F.lit(args.start_date)))
    if args.end_date:
        df = df.filter(F.to_date("base_hour") <= F.to_date(F.lit(args.end_date)))
    return df


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    tables = get_table_names()
    source_table = args.source_table or tables["serving_features_gold"]
    dry_run = as_bool(args.dry_run)
    latest_only = as_bool(args.latest_only)
    loaded_at = datetime.now(timezone.utc)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.table(source_table)
        if "base_hour" not in df.columns and "hour" in df.columns:
            df = df.withColumnRenamed("hour", "base_hour")
        df = apply_time_filters(df, args)
        df = df.filter(F.col("base_hour").isNotNull())

        if latest_only and not args.base_hour:
            latest = df.agg(F.max("base_hour").alias("base_hour")).first()["base_hour"]
            if latest is not None:
                df = df.filter(F.col("base_hour") == F.lit(latest))

        missing = [name for name in FEATURE_COLUMNS if name not in df.columns]
        if missing:
            raise RuntimeError(f"Missing model feature columns in {source_table}: {missing}")

        out = (
            df.select(
                F.lit(args.location_id).alias("location_id"),
                F.lit(args.feature_version).alias("feature_version"),
                F.col("base_hour"),
                F.lit(args.location_name).alias("location_name"),
                F.lit(args.feature_set_name).alias("feature_set_name"),
                F.lit(args.dataset_version).alias("dataset_version"),
                F.lit(FEATURE_SCHEMA_HASH).alias("schema_hash"),
                F.coalesce(F.col("created_at"), F.current_timestamp()).alias("created_at") if "created_at" in df.columns else F.current_timestamp().alias("created_at"),
                F.lit(loaded_at).alias("loaded_at"),
                *[F.col(name) for name in FEATURE_COLUMNS],
            )
            .dropDuplicates(["location_id", "feature_version", "base_hour"])
        )

        count = out.count()
        bounds = out.agg(F.min("base_hour").alias("min_base_hour"), F.max("base_hour").alias("max_base_hour")).first()
        print(
            "job=pm25_serving_features_to_cassandra "
            f"source_table={source_table} keyspace={args.keyspace} target_table={args.target_table} "
            f"output_count={count} min_base_hour={bounds['min_base_hour'] if bounds else None} "
            f"max_base_hour={bounds['max_base_hour'] if bounds else None} latest_only={int(latest_only)} dry_run={int(dry_run)}"
        )

        if dry_run:
            out.limit(5).show(truncate=False)
            print("job=pm25_serving_features_to_cassandra status=dry_run_success")
            return

        (
            out.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(keyspace=args.keyspace, table=args.target_table)
            .save()
        )
        print("job=pm25_serving_features_to_cassandra status=written")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
