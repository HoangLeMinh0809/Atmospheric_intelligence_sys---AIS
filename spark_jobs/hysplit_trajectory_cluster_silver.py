"""Cluster HYSPLIT trajectories and write per-point clustered silver rows."""

from __future__ import annotations

import argparse
import os
from datetime import datetime

import numpy as np
from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_table_names,
    get_trajectory_config,
)


def parse_args() -> argparse.Namespace:
    cfg = get_trajectory_config()
    parser = argparse.ArgumentParser(description="Cluster HYSPLIT trajectories")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--direction", choices=("backward", "forward", "all"), default=os.getenv("DIRECTION", "backward"))
    parser.add_argument("--source-table", default=os.getenv("HYSPLIT_TRAJ_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("HYSPLIT_CLUSTER_SILVER_TABLE", ""))
    parser.add_argument(
        "--anchor-hours",
        default=os.getenv("ANCHOR_HOURS")
        or ",".join(str(v) for v in cfg.get("anchor_hours", [0, -6, -12, -24, -36, -48, -60, -72])),
    )
    parser.add_argument("--k-min", type=int, default=int(cfg.get("cluster_k_min", 3)))
    parser.add_argument("--k-max", type=int, default=int(cfg.get("cluster_k_max", 10)))
    parser.add_argument("--k-default", type=int, default=int(cfg.get("cluster_k_default", 6)))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def anchor_col(prefix: str, hour: int) -> str:
    label = f"m{abs(hour)}" if hour < 0 else f"p{hour}"
    return f"{prefix}_{label}"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLITTrajectoryClusterSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", os.getenv("HDFS_DEFAULT_FS", os.getenv("HADOOP_DEFAULT_FS", "hdfs://namenode:9000"))))
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.trajectory")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            traj_id STRING,
            cluster_id INT,
            direction STRING,
            age_h INT,
            lat DOUBLE,
            lon DOUBLE,
            alt_m DOUBLE,
            timestamp TIMESTAMP,
            source_lat DOUBLE,
            source_lon DOUBLE,
            source_alt_m DOUBLE,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (direction)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def filter_window(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.col("timestamp") >= F.to_timestamp(F.lit(f"{start_date} 00:00:00")))
    if end_date:
        df = df.filter(F.col("timestamp") <= F.to_timestamp(F.lit(f"{end_date} 23:59:59")))
    return df


def filter_by_init_window(df, start_date: str, end_date: str):
    if not start_date and not end_date:
        return df
    init_points = df.filter(F.col("age_h") == F.lit(0))
    init_points = filter_window(init_points, start_date, end_date)
    init_ids = init_points.select("traj_id").distinct()
    return df.join(init_ids, on="traj_id", how="inner")


def delete_target_directions(spark: SparkSession, table_name: str, directions: list[str], start_date: str, end_date: str) -> None:
    if not directions:
        return
    direction_list = ", ".join(f"'{value}'" for value in sorted(set(directions)))
    predicates = [f"direction IN ({direction_list})"]
    if start_date:
        predicates.append(f"timestamp >= TIMESTAMP '{start_date} 00:00:00'")
    if end_date:
        predicates.append(f"timestamp <= TIMESTAMP '{end_date} 23:59:59'")
    spark.sql(f"DELETE FROM {table_name} WHERE {' AND '.join(predicates)}")


def append_cluster_rows(df, table_name: str) -> None:
    df.writeTo(table_name).append()


def keep_finite_feature_rows(df, feature_cols: list[str]):
    condition = None
    for col_name in feature_cols:
        col_condition = F.col(col_name).isNotNull() & (~F.isnan(F.col(col_name)))
        condition = col_condition if condition is None else condition & col_condition
    return df.filter(condition) if condition is not None else df


def build_sklearn_assignments(spark: SparkSession, df, feature_cols: list[str], args, source_cols: tuple[str, str, str]):
    source_lat_name, source_lon_name, source_alt_name = source_cols
    selected_cols = [
        "traj_id",
        "direction",
        F.col(source_lat_name).alias("source_lat"),
        F.col(source_lon_name).alias("source_lon"),
        F.col(source_alt_name).alias("source_alt_m"),
        *[F.col(col_name).cast("double").alias(col_name) for col_name in feature_cols],
    ]
    rows = df.select(*selected_cols).collect()
    if len(rows) < 2:
        assignment_rows = [
            Row(
                traj_id=row["traj_id"],
                direction=row["direction"],
                cluster_id=0,
                source_lat=row["source_lat"],
                source_lon=row["source_lon"],
                source_alt_m=row["source_alt_m"],
            )
            for row in rows
        ]
        print("[SWEEP] skipped: fewer than 2 valid fixed-size feature rows; assigned cluster_id=0")
        return spark.createDataFrame(assignment_rows) if assignment_rows else None

    matrix = np.array([[float(row[col_name]) for col_name in feature_cols] for row in rows], dtype=float)
    valid_mask = np.isfinite(matrix).all(axis=1)
    valid_rows = [row for row, is_valid in zip(rows, valid_mask) if bool(is_valid)]
    matrix = matrix[valid_mask]

    if len(valid_rows) < 2:
        assignment_rows = [
            Row(
                traj_id=row["traj_id"],
                direction=row["direction"],
                cluster_id=0,
                source_lat=row["source_lat"],
                source_lon=row["source_lon"],
                source_alt_m=row["source_alt_m"],
            )
            for row in rows
        ]
        print("[SWEEP] skipped: fewer than 2 finite feature rows; assigned cluster_id=0")
        return spark.createDataFrame(assignment_rows)

    scaled = StandardScaler().fit_transform(matrix)
    k_min = max(2, min(int(args.k_min), len(valid_rows)))
    k_max = max(k_min, min(int(args.k_max), len(valid_rows)))
    k_default = min(max(int(args.k_default), k_min), k_max)

    print(f"[INFO] Valid clustering rows={len(valid_rows)}; feature_size={len(feature_cols)}")
    print(f"[INFO] Running sklearn k-sweep {k_min}..{k_max}; final k={k_default}")
    for k in range(k_min, k_max + 1):
        model = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=40)
        model.fit(scaled)
        print(f"[SWEEP] k={k} WCSS={float(model.inertia_)}")

    final_model = KMeans(n_clusters=k_default, random_state=42, n_init=10, max_iter=100)
    labels = final_model.fit_predict(scaled)
    label_by_id = {
        (row["traj_id"], row["direction"]): int(label)
        for row, label in zip(valid_rows, labels)
    }
    assignment_rows = []
    for row in rows:
        assignment_rows.append(
            Row(
                traj_id=row["traj_id"],
                direction=row["direction"],
                cluster_id=int(label_by_id.get((row["traj_id"], row["direction"]), 0)),
                source_lat=row["source_lat"],
                source_lon=row["source_lon"],
                source_alt_m=row["source_alt_m"],
            )
        )
    return spark.createDataFrame(assignment_rows)


def main() -> None:
    args = parse_args()
    full_refresh = as_bool(args.full_refresh)
    anchor_hours = [int(value) for value in args.anchor_hours.split(",") if value.strip()]
    tables = get_table_names()
    source_table = args.source_table or tables["hysplit_traj_silver"]
    target_table = args.target_table or tables["hysplit_cluster_silver"]

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, target_table)

    points = spark.table(source_table).filter(
        F.col("lat").isNotNull() & F.col("lon").isNotNull() & F.col("age_h").isNotNull()
    )
    if args.direction != "all":
        points = points.filter(F.col("direction") == F.lit(args.direction))
    points = filter_by_init_window(points, args.start_date, args.end_date)
    raw_point_count = points.count()
    if raw_point_count == 0:
        print(
            "hysplit_cluster_checks={'input_count': 0, 'output_count': 0, 'duplicate_count': 0, "
            "'cluster_distribution': {}, 'missing_cluster_ratio': None, "
            "'min_time': None, 'max_time': None}"
        )
        spark.stop()
        return

    agg_exprs = []
    for hour in anchor_hours:
        agg_exprs.extend(
            [
                F.max(F.when(F.col("age_h") == F.lit(hour), F.col("lat"))).alias(anchor_col("lat", hour)),
                F.max(F.when(F.col("age_h") == F.lit(hour), F.col("lon"))).alias(anchor_col("lon", hour)),
                F.max(F.when(F.col("age_h") == F.lit(hour), F.col("alt_m"))).alias(anchor_col("alt", hour)),
            ]
        )

    grouped = points.groupBy("traj_id", "direction").agg(*agg_exprs)
    input_count = grouped.count()
    bounds = points.agg(F.min("timestamp").alias("min_time"), F.max("timestamp").alias("max_time")).first()

    source_lat_name = anchor_col("lat", 0)
    source_lon_name = anchor_col("lon", 0)
    source_alt_name = anchor_col("alt", 0)
    feature_cols = [anchor_col("lat", h) for h in anchor_hours] + [anchor_col("lon", h) for h in anchor_hours]
    count_exprs = [F.count(F.col(name)).alias(name) for name in feature_cols]
    non_null_counts = grouped.agg(*count_exprs).first().asDict()
    feature_cols = [name for name in feature_cols if int(non_null_counts.get(name) or 0) > 0]
    assign_without_kmeans = input_count < 2 or len(feature_cols) < 2

    if assign_without_kmeans:
        assignments = (
            grouped
            .withColumn("cluster_id", F.lit(0).cast("int"))
            .select(
                "traj_id",
                "direction",
                "cluster_id",
                F.col(source_lat_name).alias("source_lat"),
                F.col(source_lon_name).alias("source_lon"),
                F.col(source_alt_name).alias("source_alt_m"),
            )
        )
        print("[SWEEP] skipped: insufficient trajectories or anchor features; assigned cluster_id=0")
    else:
        imputed_cols = [f"{col_name}_imp" for col_name in feature_cols]
        imputer = Imputer(strategy="mean", inputCols=feature_cols, outputCols=imputed_cols)
        imputed = imputer.fit(grouped).transform(grouped)
        imputed = keep_finite_feature_rows(imputed, imputed_cols)
        valid_feature_count = imputed.count()
        if valid_feature_count < 2:
            assignments = (
                grouped
                .withColumn("cluster_id", F.lit(0).cast("int"))
                .select(
                    "traj_id",
                    "direction",
                    "cluster_id",
                    F.col(source_lat_name).alias("source_lat"),
                    F.col(source_lon_name).alias("source_lon"),
                    F.col(source_alt_name).alias("source_alt_m"),
                )
            )
            print("[SWEEP] skipped: fewer than 2 finite feature rows; assigned cluster_id=0")
        else:
            assignments = build_sklearn_assignments(
                spark,
                imputed,
                imputed_cols,
                args,
                (source_lat_name, source_lon_name, source_alt_name),
            )

    output = (
        points.join(assignments, on=["traj_id", "direction"], how="inner")
        .select(
            "traj_id",
            "cluster_id",
            "direction",
            "age_h",
            "lat",
            "lon",
            "alt_m",
            "timestamp",
            "source_lat",
            "source_lon",
            "source_alt_m",
        )
        .withColumn("spark_processed_at", F.lit(datetime.utcnow()).cast("timestamp"))
    )

    duplicate_count = (
        output.groupBy("traj_id", "age_h")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicates"))
        .first()["duplicates"]
    )
    duplicate_count = int(duplicate_count or 0)
    output = output.dropDuplicates(["traj_id", "age_h"])

    if full_refresh:
        refresh_directions = (
            [args.direction]
            if args.direction != "all"
            else [row["direction"] for row in output.select("direction").distinct().collect()]
        )
        delete_target_directions(spark, target_table, refresh_directions, args.start_date, args.end_date)
    else:
        existing = spark.table(target_table).select("traj_id", "age_h")
        output = output.join(existing, on=["traj_id", "age_h"], how="left_anti")

    output_count = output.count()
    if output_count:
        append_cluster_rows(output, target_table)

    cluster_rows = output.groupBy("cluster_id").count().orderBy("cluster_id").collect()
    cluster_distribution = {
        int(row["cluster_id"]): int(row["count"])
        for row in cluster_rows
        if row["cluster_id"] is not None
    }
    missing_cluster_count = output.filter(F.col("cluster_id").isNull()).count()
    missing_cluster_ratio = (
        float(missing_cluster_count) / float(output_count)
        if output_count
        else None
    )

    print("Cluster distribution:")
    for row in cluster_rows:
        print(f"  cluster={row['cluster_id']}: n={row['count']}")
    print(f"cluster_distribution={cluster_distribution}")
    print(f"missing_cluster_ratio={missing_cluster_ratio}")
    print(
        "hysplit_cluster_checks="
        f"{{'input_count': {input_count}, 'output_count': {output_count}, "
        f"'duplicate_count': {duplicate_count}, "
        f"'cluster_distribution': {cluster_distribution}, "
        f"'missing_cluster_ratio': {missing_cluster_ratio}, "
        f"'min_time': {repr(str(bounds['min_time']) if bounds else None)}, "
        f"'max_time': {repr(str(bounds['max_time']) if bounds else None)}}}"
    )
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
