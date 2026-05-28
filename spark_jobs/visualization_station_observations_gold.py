from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hanoi_config import (
    HDFS_NAMENODE,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_table_names,
    get_visualization_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build visualization station observation point layer")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--dry-run", nargs="?", const="1", default=os.getenv("DRY_RUN", "0"))
    parser.add_argument("--source-table", default=os.getenv("OPENAQ_STATION_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("VIS_STATION_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VisualizationStationObservationsGold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.visualization")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            observation_id STRING,
            visualization_run_id STRING,
            product_version STRING,
            schema_version STRING,
            observation_time TIMESTAMP,
            station_id STRING,
            station_name STRING,
            location_id STRING,
            city STRING,
            lat DOUBLE,
            lon DOUBLE,
            pm25 DOUBLE,
            risk STRING,
            coverage_pct DOUBLE,
            unit STRING,
            provider STRING,
            source STRING,
            geometry_geojson STRING,
            generated_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("hour") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("hour") <= F.to_date(F.lit(end_date)))
    return df


def risk_expr(pm25_col):
    return (
        F.when(pm25_col.isNull(), F.lit("unknown"))
        .when(pm25_col <= F.lit(12.0), F.lit("good"))
        .when(pm25_col <= F.lit(35.4), F.lit("moderate"))
        .when(pm25_col <= F.lit(55.4), F.lit("unhealthy_sensitive"))
        .when(pm25_col <= F.lit(150.4), F.lit("unhealthy"))
        .when(pm25_col <= F.lit(250.4), F.lit("very_unhealthy"))
        .otherwise(F.lit("hazardous"))
    )


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")
    df.createOrReplaceTempView("station_observation_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING station_observation_updates s
        ON t.observation_id = s.observation_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def build_output(spark: SparkSession, source_table: str, cfg: dict):
    source = spark.table(source_table)
    base = (
        source
        .filter(F.col("pm25").isNotNull())
        .filter(F.col("hour").isNotNull())
        .withColumn("station_id", F.coalesce(F.col("sensor_id").cast("string"), F.col("location_id").cast("string")))
        .withColumn("station_name", F.coalesce(F.col("location_name"), F.col("station_id")))
        .withColumn("lat", F.col("latitude").cast("double"))
        .withColumn("lon", F.col("longitude").cast("double"))
    )
    valid = base.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    latest_hour = valid.agg(F.max("hour").alias("latest_hour")).first()["latest_hour"]
    if latest_hour is None:
        empty = valid.limit(0).select(
            F.lit(None).cast("string").alias("observation_id"),
            F.lit(None).cast("string").alias("visualization_run_id"),
            F.lit(None).cast("string").alias("product_version"),
            F.lit(None).cast("string").alias("schema_version"),
            F.lit(None).cast("timestamp").alias("observation_time"),
            F.lit(None).cast("string").alias("station_id"),
            F.lit(None).cast("string").alias("station_name"),
            F.lit(None).cast("string").alias("location_id"),
            F.lit(None).cast("string").alias("city"),
            F.lit(None).cast("double").alias("lat"),
            F.lit(None).cast("double").alias("lon"),
            F.lit(None).cast("double").alias("pm25"),
            F.lit(None).cast("string").alias("risk"),
            F.lit(None).cast("double").alias("coverage_pct"),
            F.lit(None).cast("string").alias("unit"),
            F.lit(None).cast("string").alias("provider"),
            F.lit(None).cast("string").alias("source"),
            F.lit(None).cast("string").alias("geometry_geojson"),
            F.lit(None).cast("timestamp").alias("generated_at"),
            F.lit(None).cast("int").alias("year"),
            F.lit(None).cast("int").alias("month"),
            F.lit(None).cast("int").alias("day"),
        )
        return base, empty

    w = Window.partitionBy("station_id").orderBy(F.col("hour").desc(), F.col("coverage_pct").desc_nulls_last())
    latest = (
        valid.filter(F.col("hour") == F.lit(latest_hour))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
    )
    run_id = F.concat_ws("_", F.lit("station"), F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
    geometry = F.to_json(
        F.struct(
            F.lit("Point").alias("type"),
            F.array(F.col("lon"), F.col("lat")).alias("coordinates"),
        )
    )
    output = (
        latest
        .withColumn("observation_time", F.col("hour"))
        .withColumn("observation_id", F.sha2(F.concat_ws("|", F.col("station_id"), F.col("hour").cast("string")), 256))
        .withColumn("visualization_run_id", run_id)
        .withColumn("product_version", F.lit(str(cfg["product_version"])))
        .withColumn("schema_version", F.lit(str(cfg["schema_version"])))
        .withColumn("location_id", F.lit("hanoi"))
        .withColumn("risk", risk_expr(F.col("pm25").cast("double")))
        .withColumn("geometry_geojson", geometry)
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("year", F.year("observation_time"))
        .withColumn("month", F.month("observation_time"))
        .withColumn("day", F.dayofmonth("observation_time"))
        .select(
            "observation_id",
            "visualization_run_id",
            "product_version",
            "schema_version",
            "observation_time",
            "station_id",
            "station_name",
            "location_id",
            "city",
            "lat",
            "lon",
            F.col("pm25").cast("double").alias("pm25"),
            "risk",
            F.col("coverage_pct").cast("double").alias("coverage_pct"),
            "unit",
            "provider",
            "source",
            "geometry_geojson",
            "generated_at",
            "year",
            "month",
            "day",
        )
    )
    return base, output


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    cfg = get_visualization_config()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_table = args.source_table or tables["openaq_station_silver"]
    target_table = args.target_table or tables["visualization_station_observations_gold"]
    ensure_table(spark, target_table)

    source = spark.table(source_table)
    source = apply_date_range(source, args.start_date, args.end_date)
    source.createOrReplaceTempView("station_source_window")
    base, output = build_output(spark, "station_source_window", cfg)

    input_count = base.count()
    missing_coordinate_count = base.filter(F.col("lat").isNull() | F.col("lon").isNull()).count()
    output_count = output.count()
    duplicate_count = output.groupBy("observation_id").count().filter(F.col("count") > 1).count()

    print(f"input_count={input_count}")
    print(f"missing_coordinate_count={missing_coordinate_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print("status=ok" if duplicate_count == 0 else "status=duplicate_observation_id")

    if output_count == 0:
        print("No valid station rows to write")
    elif not as_bool(args.dry_run):
        merge_iceberg(spark, output, target_table, full_refresh=as_bool(args.full_refresh))
        print(f"Saved: {target_table}")
    else:
        print("Dry run: skipped write")
    spark.stop()


if __name__ == "__main__":
    main()
