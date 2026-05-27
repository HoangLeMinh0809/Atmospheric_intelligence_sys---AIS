from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_hanoi_center,
    get_table_names,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OpenAQ spatial gradient silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OpenAQSpatialGradientSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.features")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            hour TIMESTAMP,
            pm25_grad_n DOUBLE,
            pm25_grad_s DOUBLE,
            pm25_grad_e DOUBLE,
            pm25_grad_w DOUBLE,
            pm25_spatial_std DOUBLE,
            pm25_grad_mag DOUBLE,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("hour") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("hour") <= F.to_date(F.lit(end_date)))
    return df


def compute_spatial_gradient(station, center_lat: float, center_lon: float):
    offset = 0.25

    valid = station.filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("pm25").isNotNull()
    )
    hour_stats = valid.groupBy("hour").agg(
        F.countDistinct("location_id").alias("location_count"),
        F.stddev_samp("pm25").alias("pm25_spatial_std_raw"),
    )

    targets = F.array(
        F.struct(F.lit("n").alias("target"), F.lit(center_lat + offset).alias("target_lat"), F.lit(center_lon).alias("target_lon")),
        F.struct(F.lit("s").alias("target"), F.lit(center_lat - offset).alias("target_lat"), F.lit(center_lon).alias("target_lon")),
        F.struct(F.lit("e").alias("target"), F.lit(center_lat).alias("target_lat"), F.lit(center_lon + offset).alias("target_lon")),
        F.struct(F.lit("w").alias("target"), F.lit(center_lat).alias("target_lat"), F.lit(center_lon - offset).alias("target_lon")),
    )

    expanded = (
        valid.select("hour", "location_id", "latitude", "longitude", "pm25")
        .withColumn("target_point", F.explode(targets))
        .select(
            "hour",
            "location_id",
            "latitude",
            "longitude",
            "pm25",
            F.col("target_point.target").alias("target"),
            F.col("target_point.target_lat").alias("target_lat"),
            F.col("target_point.target_lon").alias("target_lon"),
        )
        .withColumn(
            "distance",
            F.sqrt(
                F.pow(F.col("latitude") - F.col("target_lat"), F.lit(2.0))
                + F.pow(F.col("longitude") - F.col("target_lon"), F.lit(2.0))
            ),
        )
    )

    nearest_window = Window.partitionBy("hour", "target").orderBy(F.col("distance").asc())
    idw = (
        expanded.withColumn("rank", F.row_number().over(nearest_window))
        .filter(F.col("rank") <= 3)
        .withColumn("weight", F.lit(1.0) / F.greatest(F.col("distance"), F.lit(1e-6)))
        .groupBy("hour", "target")
        .agg((F.sum(F.col("weight") * F.col("pm25")) / F.sum("weight")).alias("pm25_idw"))
        .groupBy("hour")
        .pivot("target", ["n", "s", "e", "w"])
        .agg(F.first("pm25_idw"))
    )

    enough_locations = F.col("location_count") >= 3
    grad_mag = F.sqrt(
        F.pow(F.col("n") - F.col("s"), F.lit(2.0))
        + F.pow(F.col("e") - F.col("w"), F.lit(2.0))
    )

    return (
        hour_stats.join(idw, on="hour", how="left")
        .select(
            F.col("hour"),
            F.when(enough_locations, F.col("n")).cast("double").alias("pm25_grad_n"),
            F.when(enough_locations, F.col("s")).cast("double").alias("pm25_grad_s"),
            F.when(enough_locations, F.col("e")).cast("double").alias("pm25_grad_e"),
            F.when(enough_locations, F.col("w")).cast("double").alias("pm25_grad_w"),
            F.when(enough_locations, F.col("pm25_spatial_std_raw")).cast("double").alias("pm25_spatial_std"),
            F.when(enough_locations, grad_mag).cast("double").alias("pm25_grad_mag"),
            F.current_timestamp().alias("spark_processed_at"),
            F.year("hour").cast("int").alias("year"),
            F.month("hour").cast("int").alias("month"),
        )
    )


def build_output_df(spark: SparkSession, source_table: str, start_date: str, end_date: str):
    station = spark.table(source_table)
    station = apply_date_range(station, start_date, end_date)
    station = station.filter(F.col("hour").isNotNull())

    duplicate_count_row = (
        station.groupBy("hour", "location_id", "sensor_id")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_count"))
        .first()
    )
    duplicate_count = int((duplicate_count_row["duplicate_count"] if duplicate_count_row else 0) or 0)

    center = get_hanoi_center()
    output = compute_spatial_gradient(station, center["lat"], center["lon"])
    return station, output, duplicate_count


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df.createOrReplaceTempView("openaq_gradient_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING openaq_gradient_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def log_metrics(station_df, output_df, duplicate_count: int) -> None:
    input_count = station_df.count()
    output_count = output_df.count()
    bounds = station_df.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    null_ratio = output_df.agg(
        F.avg(F.when(F.col("pm25_grad_mag").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pm25_grad_mag"),
        F.avg(F.when(F.col("pm25_spatial_std").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pm25_spatial_std"),
    ).first()
    coverage = output_df.agg(
        F.min("pm25_grad_mag").alias("pm25_grad_mag_min"),
        F.max("pm25_grad_mag").alias("pm25_grad_mag_max"),
    ).first()

    print(f"input_count={input_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"pm25_grad_mag_min={coverage['pm25_grad_mag_min'] if coverage else None}")
    print(f"pm25_grad_mag_max={coverage['pm25_grad_mag_max'] if coverage else None}")
    print(
        "null_ratio="
        f"{{'pm25_grad_mag': {null_ratio['pm25_grad_mag'] if null_ratio else None}, "
        f"'pm25_spatial_std': {null_ratio['pm25_spatial_std'] if null_ratio else None}}}"
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["openaq_station_silver"])
    target_table = os.getenv("ICEBERG_TABLE", tables["openaq_gradient_silver"])
    full_refresh = as_bool(args.full_refresh)

    ensure_table(spark, target_table)
    station_df, output_df, duplicate_count = build_output_df(
        spark,
        source_table=source_table,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    log_metrics(station_df, output_df, duplicate_count)
    merge_iceberg(spark, output_df, target_table, full_refresh=full_refresh)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
