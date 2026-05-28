from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from hanoi_config import (
    HDFS_NAMENODE,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_table_names,
    get_visualization_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build visualization forward plume probability grid")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--dry-run", nargs="?", const="1", default=os.getenv("DRY_RUN", "0"))
    parser.add_argument("--source-table", default=os.getenv("HYSPLIT_TRAJ_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("VIS_PLUME_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VisualizationForwardPlumeProbabilityGold")
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
            visualization_run_id STRING,
            product_version STRING,
            schema_version STRING,
            base_time TIMESTAMP,
            valid_time TIMESTAMP,
            horizon_h INT,
            cell_id STRING,
            lat DOUBLE,
            lon DOUBLE,
            lat_min DOUBLE,
            lat_max DOUBLE,
            lon_min DOUBLE,
            lon_max DOUBLE,
            particle_count BIGINT,
            total_particle_count BIGINT,
            probability DOUBLE,
            available BOOLEAN,
            unavailable_reason STRING,
            source_run_count INT,
            source_method STRING,
            geometry_geojson STRING,
            generated_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (horizon_h, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("timestamp") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("timestamp") <= F.to_date(F.lit(end_date)))
    return df


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")
    df.createOrReplaceTempView("forward_plume_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING forward_plume_updates s
        ON t.base_time = s.base_time
           AND t.horizon_h = s.horizon_h
           AND t.cell_id = s.cell_id
           AND t.product_version = s.product_version
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def unavailable_output(spark: SparkSession, cfg: dict, horizons: list[int]):
    schema = StructType(
        [
            StructField("visualization_run_id", StringType()),
            StructField("product_version", StringType()),
            StructField("schema_version", StringType()),
            StructField("base_time", TimestampType()),
            StructField("valid_time", TimestampType()),
            StructField("horizon_h", IntegerType()),
            StructField("cell_id", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("lat_min", DoubleType()),
            StructField("lat_max", DoubleType()),
            StructField("lon_min", DoubleType()),
            StructField("lon_max", DoubleType()),
            StructField("particle_count", LongType()),
            StructField("total_particle_count", LongType()),
            StructField("probability", DoubleType()),
            StructField("available", BooleanType()),
            StructField("unavailable_reason", StringType()),
            StructField("source_run_count", IntegerType()),
            StructField("source_method", StringType()),
            StructField("geometry_geojson", StringType()),
            StructField("generated_at", TimestampType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("day", IntegerType()),
        ]
    )
    empty = spark.createDataFrame([], schema)
    horizons_df = spark.createDataFrame([(int(h),) for h in horizons], ["horizon_h"])
    base = (
        horizons_df
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("base_time", F.date_trunc("hour", F.col("generated_at")))
        .withColumn("valid_time", F.expr("base_time + make_interval(0, 0, 0, 0, horizon_h, 0, 0)"))
        .withColumn("visualization_run_id", F.concat_ws("_", F.lit("plume_unavailable"), F.date_format(F.col("generated_at"), "yyyyMMddHHmmss")))
        .withColumn("product_version", F.lit(str(cfg["product_version"])))
        .withColumn("schema_version", F.lit(str(cfg["schema_version"])))
        .withColumn("cell_id", F.concat(F.lit("unavailable_h"), F.col("horizon_h").cast("string")))
        .withColumn("lat", F.lit(None).cast("double"))
        .withColumn("lon", F.lit(None).cast("double"))
        .withColumn("lat_min", F.lit(None).cast("double"))
        .withColumn("lat_max", F.lit(None).cast("double"))
        .withColumn("lon_min", F.lit(None).cast("double"))
        .withColumn("lon_max", F.lit(None).cast("double"))
        .withColumn("particle_count", F.lit(0).cast("bigint"))
        .withColumn("total_particle_count", F.lit(0).cast("bigint"))
        .withColumn("probability", F.lit(0.0).cast("double"))
        .withColumn("available", F.lit(False))
        .withColumn("unavailable_reason", F.lit("forward_hysplit_missing"))
        .withColumn("source_run_count", F.lit(0).cast("int"))
        .withColumn("source_method", F.lit("hysplit_forward_particle_grid"))
        .withColumn("geometry_geojson", F.lit(None).cast("string"))
        .withColumn("year", F.year("base_time"))
        .withColumn("month", F.month("base_time"))
        .withColumn("day", F.dayofmonth("base_time"))
        .select(empty.columns)
    )
    return base


def build_output(spark: SparkSession, source_table: str, cfg: dict):
    resolution = float(cfg.get("grid_resolution_deg", 0.1))
    horizons = [int(h) for h in cfg.get("horizons_hours", [0, 6, 12, 24]) if int(h) in {6, 12, 24}]
    source = (
        spark.table(source_table)
        .filter(F.col("direction") == F.lit("forward"))
        .filter(F.col("traj_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("age_h").isin(horizons))
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    )
    if source.limit(1).count() == 0:
        if bool(cfg.get("forward_plume_required", False)):
            raise RuntimeError("No forward HYSPLIT rows found and forward plume is required")
        return source, unavailable_output(spark, cfg, horizons)

    points = (
        source
        .withColumn("horizon_h", F.col("age_h").cast("int"))
        .withColumn("base_time", F.expr("timestamp - make_interval(0, 0, 0, 0, horizon_h, 0, 0)"))
        .withColumn("valid_time", F.col("timestamp"))
        .withColumn("lat_min", F.floor(F.col("lat") / F.lit(resolution)) * F.lit(resolution))
        .withColumn("lon_min", F.floor(F.col("lon") / F.lit(resolution)) * F.lit(resolution))
        .withColumn("lat_max", F.col("lat_min") + F.lit(resolution))
        .withColumn("lon_max", F.col("lon_min") + F.lit(resolution))
        .withColumn("cell_id", F.concat_ws("_", F.col("horizon_h"), F.format_number(F.col("lat_min"), 4), F.format_number(F.col("lon_min"), 4)))
    )
    totals = (
        points.groupBy("base_time", "horizon_h")
        .agg(
            F.count("*").cast("bigint").alias("total_particle_count"),
            F.countDistinct("traj_id").cast("int").alias("source_run_count"),
        )
    )
    grid = (
        points.groupBy("base_time", "horizon_h", "cell_id", "lat_min", "lat_max", "lon_min", "lon_max")
        .agg(
            F.count("*").cast("bigint").alias("particle_count"),
            F.max("valid_time").alias("valid_time"),
        )
        .join(totals, on=["base_time", "horizon_h"], how="inner")
        .withColumn("probability", F.col("particle_count") / F.col("total_particle_count"))
        .withColumn("lat", (F.col("lat_min") + F.col("lat_max")) / F.lit(2.0))
        .withColumn("lon", (F.col("lon_min") + F.col("lon_max")) / F.lit(2.0))
    )
    ring = F.array(
        F.array(F.col("lon_min"), F.col("lat_min")),
        F.array(F.col("lon_max"), F.col("lat_min")),
        F.array(F.col("lon_max"), F.col("lat_max")),
        F.array(F.col("lon_min"), F.col("lat_max")),
        F.array(F.col("lon_min"), F.col("lat_min")),
    )
    geometry = F.to_json(F.struct(F.lit("Polygon").alias("type"), F.array(ring).alias("coordinates")))
    output = (
        grid
        .withColumn("visualization_run_id", F.concat_ws("_", F.lit("plume"), F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")))
        .withColumn("product_version", F.lit(str(cfg["product_version"])))
        .withColumn("schema_version", F.lit(str(cfg["schema_version"])))
        .withColumn("available", F.lit(True))
        .withColumn("unavailable_reason", F.lit(None).cast("string"))
        .withColumn("source_method", F.lit("hysplit_forward_particle_grid"))
        .withColumn("geometry_geojson", geometry)
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("year", F.year("base_time"))
        .withColumn("month", F.month("base_time"))
        .withColumn("day", F.dayofmonth("base_time"))
        .select(
            "visualization_run_id",
            "product_version",
            "schema_version",
            "base_time",
            "valid_time",
            "horizon_h",
            "cell_id",
            "lat",
            "lon",
            "lat_min",
            "lat_max",
            "lon_min",
            "lon_max",
            "particle_count",
            "total_particle_count",
            "probability",
            "available",
            "unavailable_reason",
            "source_run_count",
            "source_method",
            "geometry_geojson",
            "generated_at",
            "year",
            "month",
            "day",
        )
    )
    return source, output


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    cfg = get_visualization_config()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_table = args.source_table or tables["hysplit_traj_silver"]
    target_table = args.target_table or tables["visualization_forward_plume_probability_gold"]
    ensure_table(spark, target_table)

    source = spark.table(source_table)
    source = apply_date_range(source, args.start_date, args.end_date)
    source.createOrReplaceTempView("forward_source_window")
    input_df, output = build_output(spark, "forward_source_window", cfg)

    input_point_count = input_df.count()
    output_count = output.count()
    available_count = output.filter(F.col("available") == F.lit(True)).count()
    probability_sums = (
        output.filter(F.col("available") == F.lit(True))
        .groupBy("base_time", "horizon_h")
        .agg(F.sum("probability").alias("probability_sum"))
        .collect()
    )
    invalid_probability_count = sum(
        1 for row in probability_sums if row["probability_sum"] is None or abs(float(row["probability_sum"]) - 1.0) > 0.01
    )

    print(f"input_point_count={input_point_count}")
    print(f"output_count={output_count}")
    print(f"available_cell_count={available_count}")
    print(f"invalid_probability_count={invalid_probability_count}")
    print("status=ok" if invalid_probability_count == 0 else "status=invalid_probability_sum")

    if not as_bool(args.dry_run):
        merge_iceberg(spark, output, target_table, full_refresh=as_bool(args.full_refresh))
        print(f"Saved: {target_table}")
    else:
        print("Dry run: skipped write")
    spark.stop()


if __name__ == "__main__":
    main()
