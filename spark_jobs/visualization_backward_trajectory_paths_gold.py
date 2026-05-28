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
    parser = argparse.ArgumentParser(description="Build visualization backward trajectory LineString layer")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--dry-run", nargs="?", const="1", default=os.getenv("DRY_RUN", "0"))
    parser.add_argument("--cluster-table", default=os.getenv("HYSPLIT_CLUSTER_SILVER_TABLE", ""))
    parser.add_argument("--path-table", default=os.getenv("TRAJ_PATH_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("VIS_TRAJECTORY_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("VisualizationBackwardTrajectoryPathsGold")
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
            init_time TIMESTAMP,
            direction STRING,
            traj_id STRING,
            traj_no INT,
            cluster_id INT,
            source_label STRING,
            source_lat DOUBLE,
            source_lon DOUBLE,
            source_alt_m DOUBLE,
            start_lat DOUBLE,
            start_lon DOUBLE,
            end_lat DOUBLE,
            end_lon DOUBLE,
            age_start_h INT,
            age_end_h INT,
            point_count INT,
            path_no2_mean DOUBLE,
            path_aer_mean DOUBLE,
            path_no2_aer_ratio DOUBLE,
            geometry_geojson STRING,
            properties_json STRING,
            style_color STRING,
            generated_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (direction, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("timestamp") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("timestamp") <= F.to_date(F.lit(end_date)))
    return df


def cluster_label_expr(cfg: dict):
    labels = cfg.get("source_cluster_labels") or {}
    if not labels:
        return F.concat(F.lit("cluster_"), F.col("cluster_id").cast("string"))
    pairs = []
    for key, value in labels.items():
        pairs.extend([F.lit(int(key)), F.lit(str(value))])
    return F.coalesce(
        F.create_map(*pairs)[F.col("cluster_id")],
        F.concat(F.lit("cluster_"), F.col("cluster_id").cast("string")),
    )


def style_color_expr(cluster_col):
    colors = ["#2563eb", "#16a34a", "#f97316", "#dc2626", "#7c3aed", "#0891b2", "#ca8a04", "#be185d"]
    return F.element_at(F.array(*[F.lit(c) for c in colors]), (F.pmod(cluster_col.cast("int"), F.lit(len(colors))) + F.lit(1)))


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")
    df.createOrReplaceTempView("backward_trajectory_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING backward_trajectory_updates s
        ON t.base_time = s.base_time
           AND t.direction = s.direction
           AND t.traj_id = s.traj_id
           AND t.product_version = s.product_version
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def build_output(spark: SparkSession, cluster_table: str, path_table: str, cfg: dict):
    clustered = (
        spark.table(cluster_table)
        .filter(F.col("direction") == F.lit("backward"))
        .filter(F.col("traj_id").isNotNull())
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .filter(F.col("age_h").isNotNull())
        .filter(F.col("timestamp").isNotNull())
    )

    init = (
        clustered.filter(F.col("age_h") == F.lit(0))
        .select(
            "traj_id",
            F.col("timestamp").alias("init_time"),
            F.col("timestamp").alias("base_time"),
            F.col("lat").cast("double").alias("start_lat"),
            F.col("lon").cast("double").alias("start_lon"),
        )
        .dropDuplicates(["traj_id"])
    )
    end_w = Window.partitionBy("traj_id").orderBy(F.col("age_h").asc(), F.col("timestamp").asc())
    endpoints = (
        clustered.withColumn("rn_end", F.row_number().over(end_w))
        .filter(F.col("rn_end") == 1)
        .select(
            "traj_id",
            F.col("lat").cast("double").alias("end_lat"),
            F.col("lon").cast("double").alias("end_lon"),
        )
    )

    grouped = (
        clustered
        .groupBy("traj_id")
        .agg(
            F.first("direction", ignorenulls=True).alias("direction"),
            F.first("cluster_id", ignorenulls=True).cast("int").alias("cluster_id"),
            F.avg("source_lat").alias("source_lat"),
            F.avg("source_lon").alias("source_lon"),
            F.avg("source_alt_m").alias("source_alt_m"),
            F.min("age_h").cast("int").alias("age_start_h"),
            F.max("age_h").cast("int").alias("age_end_h"),
            F.count("*").cast("int").alias("point_count"),
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("age_h").cast("int").alias("age_h"),
                        F.col("timestamp").alias("timestamp"),
                        F.col("lon").cast("double").alias("lon"),
                        F.col("lat").cast("double").alias("lat"),
                        F.col("alt_m").cast("double").alias("alt_m"),
                    )
                )
            ).alias("points_sorted"),
        )
        .filter(F.col("point_count") >= F.lit(2))
        .withColumn("traj_no", F.lit(None).cast("int"))
    )

    path = spark.table(path_table).select(
        "traj_id",
        F.col("path_no2_mean").cast("double").alias("path_no2_mean"),
        F.col("path_aer_mean").cast("double").alias("path_aer_mean"),
        F.col("path_no2_aer_ratio").cast("double").alias("path_no2_aer_ratio"),
    )

    joined = grouped.join(init, on="traj_id", how="inner").join(endpoints, on="traj_id", how="left").join(path, on="traj_id", how="left")
    geometry = F.to_json(
        F.struct(
            F.lit("LineString").alias("type"),
            F.expr("transform(points_sorted, p -> array(p.lon, p.lat, p.alt_m))").alias("coordinates"),
        )
    )
    properties = F.to_json(
        F.struct(
            F.col("traj_id"),
            F.col("cluster_id"),
            F.col("source_label"),
            F.col("source_lat"),
            F.col("source_lon"),
            F.col("path_no2_mean"),
            F.col("path_aer_mean"),
            F.col("path_no2_aer_ratio"),
            F.col("age_start_h"),
            F.col("age_end_h"),
            F.col("point_count"),
        )
    )
    output = (
        joined
        .withColumn("visualization_run_id", F.concat_ws("_", F.lit("backward"), F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")))
        .withColumn("product_version", F.lit(str(cfg["product_version"])))
        .withColumn("schema_version", F.lit(str(cfg["schema_version"])))
        .withColumn("source_label", cluster_label_expr(cfg))
        .withColumn("style_color", style_color_expr(F.col("cluster_id")))
        .withColumn("geometry_geojson", geometry)
        .withColumn("properties_json", properties)
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("year", F.year("base_time"))
        .withColumn("month", F.month("base_time"))
        .withColumn("day", F.dayofmonth("base_time"))
        .select(
            "visualization_run_id",
            "product_version",
            "schema_version",
            "base_time",
            "init_time",
            "direction",
            "traj_id",
            "traj_no",
            "cluster_id",
            "source_label",
            "source_lat",
            "source_lon",
            "source_alt_m",
            "start_lat",
            "start_lon",
            "end_lat",
            "end_lon",
            "age_start_h",
            "age_end_h",
            "point_count",
            "path_no2_mean",
            "path_aer_mean",
            "path_no2_aer_ratio",
            "geometry_geojson",
            "properties_json",
            "style_color",
            "generated_at",
            "year",
            "month",
            "day",
        )
    )
    return clustered, output


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    cfg = get_visualization_config()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cluster_table = args.cluster_table or tables["hysplit_cluster_silver"]
    path_table = args.path_table or tables["trajectory_path_silver"]
    target_table = args.target_table or tables["visualization_backward_trajectory_paths_gold"]
    ensure_table(spark, target_table)

    clustered = spark.table(cluster_table)
    clustered = apply_date_range(clustered, args.start_date, args.end_date)
    clustered.createOrReplaceTempView("clustered_window")
    input_df, output = build_output(spark, "clustered_window", path_table, cfg)

    input_point_count = input_df.count()
    trajectory_count = output.count()
    cluster_count = output.select("cluster_id").distinct().count()
    missing_cluster_label_count = output.filter(F.col("source_label").isNull()).count()
    invalid_geometry_count = output.filter((F.col("geometry_geojson").isNull()) | (F.col("point_count") < 2)).count()

    print(f"input_point_count={input_point_count}")
    print(f"trajectory_count={trajectory_count}")
    print(f"cluster_count={cluster_count}")
    print(f"missing_cluster_label_count={missing_cluster_label_count}")
    print(f"invalid_geometry_count={invalid_geometry_count}")
    print(f"output_count={trajectory_count}")
    print("status=ok" if invalid_geometry_count == 0 else "status=invalid_geometry")

    if not as_bool(args.dry_run):
        merge_iceberg(spark, output, target_table, full_refresh=as_bool(args.full_refresh))
        print(f"Saved: {target_table}")
    else:
        print("Dry run: skipped write")
    spark.stop()


if __name__ == "__main__":
    main()
