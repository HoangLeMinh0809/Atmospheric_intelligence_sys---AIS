# File nay: xu ly du lieu lakehouse hoac tac vu Spark tien ich.
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp

HDFS_NAMENODE = (
    os.getenv("HDFS_NAMENODE")
    or os.getenv("HDFS_DEFAULT_FS")
    or os.getenv("HADOOP_DEFAULT_FS")
    or "hdfs://namenode:9000"
).rstrip("/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", f"{HDFS_NAMENODE}/warehouse/iceberg")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "ais_serving")

DATASETS = {
    "weather": {
        "iceberg_table": f"{ICEBERG_CATALOG}.weather.weather_history_bronze",
        "cassandra_table": "weather_hourly_by_province_day",
        "time_col": "event_time",
        "key_col": "event_id",
    },
    "openaq": {
        "iceberg_table": f"{ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze",
        "cassandra_table": "openaq_hourly_by_city_parameter_day",
        "time_col": "event_time",
        "key_col": "event_id",
    },
}


# Khoi tao SparkSession voi Iceberg catalog, warehouse va HDFS config.
def build_spark() -> SparkSession:
    return (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("AIS_ReconcileServing")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile Iceberg historical vs Cassandra serving")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--tolerance", type=float, default=0.95)
    parser.add_argument("--datasets", type=str, default="weather,openaq")
    return parser.parse_args()


# Dem ban ghi moi gan day cho serving state Cassandra.
def count_recent(df, time_col: str, key_col: str, window_start_utc: datetime) -> int:
    window_start_text = window_start_utc.strftime("%Y-%m-%d %H:%M:%S")
    return (
        # Chi doi soat cua so gan day de bat kip do tre serving thay vi quet toan bo bang.
        df.where(col(time_col) >= to_timestamp(lit(window_start_text)))
        .select(key_col)
        .dropna()
        .distinct()
        .count()
    )


# Doi soat du lieu giua hai he thong cho serving state Cassandra.
def reconcile_dataset(
    spark: SparkSession,
    dataset: str,
    lookback_hours: int,
    tolerance: float,
) -> None:
    cfg = DATASETS[dataset]
    window_start = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    if not spark.catalog.tableExists(cfg["iceberg_table"]):
        raise RuntimeError(f"Iceberg table missing for dataset={dataset}: {cfg['iceberg_table']}")

    # Iceberg dai dien cho su that lich su; Cassandra dai dien cho state phuc vu online hien tai.
    iceberg_df = spark.read.table(cfg["iceberg_table"])
    cassandra_df = (
        # Doc serving state tu Cassandra de doi chieu hoac phuc vu online.
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=cfg["cassandra_table"], keyspace=CASSANDRA_KEYSPACE)
        .load()
    )

    # Dem theo key distinct de tranh sai so do duplicate row hoac overwrite trong serving store.
    iceberg_count = count_recent(
        iceberg_df,
        time_col=cfg["time_col"],
        key_col=cfg["key_col"],
        window_start_utc=window_start,
    )
    cassandra_count = count_recent(
        cassandra_df,
        time_col=cfg["time_col"],
        key_col=cfg["key_col"],
        window_start_utc=window_start,
    )

    # Ti le nay cho biet Cassandra co bi roi mat du lieu so voi nguon Iceberg trong cua so gan day hay khong.
    ratio = 1.0 if iceberg_count == 0 else (cassandra_count / iceberg_count)
    print(
        f"dataset={dataset} window_hours={lookback_hours} "
        f"iceberg={iceberg_count} cassandra={cassandra_count} ratio={ratio:.4f}"
    )

    if ratio + 1e-9 < tolerance:
        raise RuntimeError(
            f"Reconciliation failed for {dataset}: cassandra/iceberg ratio {ratio:.4f} < tolerance {tolerance:.4f}"
        )


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    selected = [item.strip() for item in args.datasets.split(",") if item.strip()]

    # Validate ten dataset som de job fail ro rang thay vi chay nua chung moi loi.
    for ds in selected:
        if ds not in DATASETS:
            raise SystemExit(f"Unsupported dataset: {ds}. Supported: {','.join(sorted(DATASETS))}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Chay doi soat tung dataset doc lap de log duoc so lieu rieng cho weather va OpenAQ.
        for ds in selected:
            reconcile_dataset(
                spark=spark,
                dataset=ds,
                lookback_hours=max(1, int(args.lookback_hours)),
                tolerance=float(args.tolerance),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
