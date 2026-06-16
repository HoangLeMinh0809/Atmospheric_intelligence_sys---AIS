# File nay: xu ly du lieu lakehouse hoac tac vu Spark tien ich.
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

HDFS_NAMENODE = (
    os.getenv("HDFS_NAMENODE")
    or os.getenv("HDFS_DEFAULT_FS")
    or os.getenv("HADOOP_DEFAULT_FS")
    or "hdfs://namenode:9000"
).rstrip("/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", f"{HDFS_NAMENODE}/warehouse/iceberg")
TABLES = [
    "weather.weather_history_bronze",
    "air_quality.openaq_hourly_bronze",
    "satellite.sentinel5p_summary_bronze",
    "satellite.maiac_summary_bronze",
]


# Khoi tao SparkSession voi Iceberg catalog, warehouse va HDFS config.
def build_spark() -> SparkSession:
    return (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("AIS_IcebergMaintenance")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )


# Chay bo thu tuc bao tri Iceberg co ban cho cac bang bronze lon: rewrite, expire snapshots, remove orphan files.
def run_maintenance(spark: SparkSession, retention_hours: int) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=retention_hours)).strftime("%Y-%m-%d %H:%M:%S")

    for table_suffix in TABLES:
        fq_table = f"{ICEBERG_CATALOG}.{table_suffix}"
        if not spark.catalog.tableExists(fq_table):
            print(f"[SKIP] Table does not exist: {fq_table}")
            continue

        print(f"[RUN] rewrite_data_files: {fq_table}")
        try:
            spark.sql(f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{table_suffix}')")
        except Exception as exc:
            print(f"[WARN] rewrite_data_files failed for {fq_table}: {exc}")

        print(f"[RUN] expire_snapshots: {fq_table} older_than={cutoff}")
        try:
            spark.sql(
                f"CALL {ICEBERG_CATALOG}.system.expire_snapshots(table => '{table_suffix}', older_than => TIMESTAMP '{cutoff}')"
            )
        except Exception as exc:
            print(f"[WARN] expire_snapshots failed for {fq_table}: {exc}")

        print(f"[RUN] remove_orphan_files: {fq_table} older_than={cutoff}")
        try:
            spark.sql(
                f"CALL {ICEBERG_CATALOG}.system.remove_orphan_files(table => '{table_suffix}', older_than => TIMESTAMP '{cutoff}')"
            )
        except Exception as exc:
            print(f"[WARN] remove_orphan_files failed for {fq_table}: {exc}")


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Iceberg maintenance procedures")
    parser.add_argument("--retention-hours", type=int, default=168)
    return parser.parse_args()


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_maintenance(spark, retention_hours=max(1, int(args.retention_hours)))
    spark.stop()


if __name__ == "__main__":
    main()
