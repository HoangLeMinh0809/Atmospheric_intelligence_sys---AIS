from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from hanoi_config import HDFS_NAMENODE, ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_table_names


FEATURE_GROUPS = [
    "aq_lag",
    "weather_proxy",
    "era5_surface",
    "satellite_s5p",
    "satellite_maiac",
    "time",
    # Tier-2
    "trajectory",
    "gradient",
    "time_sincos",
]

OUTPUT_COLUMNS = [
    "dataset_version",
    "feature_set_name",
    "split",
    "feature_groups",
    "created_at",
    "hour",
    "pm25_median",
    "pm25_mean",
    "station_count",
    "coverage_avg",
    "vis_km",
    "uv",
    "condition_code",
    "is_day",
    "will_it_rain",
    "chance_of_rain",
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
    "s5p_no2_mean",
    "s5p_co_mean",
    "s5p_so2_mean",
    "s5p_o3_mean",
    "s5p_aer_ai_mean",
    "s5p_no2_valid_pct",
    "s5p_aer_ai_valid_pct",
    "aod_047_mean",
    "aod_055_mean",
    "aod_mean",
    "aod_max",
    "aod_valid_pct",
    # Tier-2: gradient
    "pm25_grad_n",
    "pm25_grad_s",
    "pm25_grad_e",
    "pm25_grad_w",
    "pm25_spatial_std",
    "pm25_grad_mag",
    # Tier-2: trajectory
    "dominant_cluster",
    "n_traj",
    "traj_source_lat",
    "traj_source_lon",
    "traj_path_no2_mean",
    "traj_path_aer_mean",
    "traj_path_no2_aer_ratio",
    "hour_of_day",
    "day_of_week",
    "month",
    "season",
    "is_weekend",
    # Tier-2: time_sincos
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    "is_rush_hour",
    "pm25_lag_1h",
    "pm25_lag_3h",
    "pm25_lag_6h",
    "pm25_lag_12h",
    "pm25_lag_24h",
    "pm25_roll_mean_3h",
    "pm25_roll_mean_6h",
    "pm25_roll_mean_24h",
    "pm25_roll_max_24h",
    "pm25_roll_std_24h",
    "pm25_next_6h",
    "pm25_next_12h",
    "pm25_next_24h",
    "year",
    "month_partition",
    "spark_processed_at",
]

OUTPUT_COLUMN_TYPES = {
    "dataset_version": "STRING",
    "feature_set_name": "STRING",
    "split": "STRING",
    "feature_groups": "ARRAY<STRING>",
    "created_at": "TIMESTAMP",
    "hour": "TIMESTAMP",
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
    "pm25_next_6h": "DOUBLE",
    "pm25_next_12h": "DOUBLE",
    "pm25_next_24h": "DOUBLE",
    "year": "INT",
    "month_partition": "INT",
    "spark_processed_at": "TIMESTAMP",
}

TARGET_HORIZONS = {
    "pm25_next_6h": 6,
    "pm25_next_12h": 12,
    "pm25_next_24h": 24,
}
TARGET_COLUMNS = list(TARGET_HORIZONS.keys())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi PM2.5 training dataset gold table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--dataset-version", default=os.getenv("DATASET_VERSION", "hanoi_pm25_v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    packages = os.getenv("SPARK_JARS_PACKAGES", "").strip()
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
    builder = (
        SparkSession.builder
        .appName("HanoiPM25TrainingDatasetGold")
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", HDFS_NAMENODE))
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
        )
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.features")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dataset_version STRING,
            feature_set_name STRING,
            split STRING,
            feature_groups ARRAY<STRING>,
            created_at TIMESTAMP,
            hour TIMESTAMP,
            pm25_median DOUBLE,
            pm25_mean DOUBLE,
            station_count INT,
            coverage_avg DOUBLE,
            vis_km DOUBLE,
            uv DOUBLE,
            condition_code INT,
            is_day INT,
            will_it_rain INT,
            chance_of_rain INT,
            wind_u10 DOUBLE,
            wind_v10 DOUBLE,
            wind_speed DOUBLE,
            wind_dir DOUBLE,
            pbl_height_m DOUBLE,
            low_pbl BOOLEAN,
            surface_pressure DOUBLE,
            temperature_2m_c DOUBLE,
            dewpoint_2m_c DOUBLE,
            total_precipitation_mm DOUBLE,
            s5p_no2_mean DOUBLE,
            s5p_co_mean DOUBLE,
            s5p_so2_mean DOUBLE,
            s5p_o3_mean DOUBLE,
            s5p_aer_ai_mean DOUBLE,
            s5p_no2_valid_pct DOUBLE,
            s5p_aer_ai_valid_pct DOUBLE,
            aod_047_mean DOUBLE,
            aod_055_mean DOUBLE,
            aod_mean DOUBLE,
            aod_max DOUBLE,
            aod_valid_pct DOUBLE,
            pm25_grad_n DOUBLE,
            pm25_grad_s DOUBLE,
            pm25_grad_e DOUBLE,
            pm25_grad_w DOUBLE,
            pm25_spatial_std DOUBLE,
            pm25_grad_mag DOUBLE,
            dominant_cluster INT,
            n_traj INT,
            traj_source_lat DOUBLE,
            traj_source_lon DOUBLE,
            traj_path_no2_mean DOUBLE,
            traj_path_aer_mean DOUBLE,
            traj_path_no2_aer_ratio DOUBLE,
            hour_of_day INT,
            day_of_week INT,
            month INT,
            season STRING,
            is_weekend BOOLEAN,
            hour_sin DOUBLE,
            hour_cos DOUBLE,
            dow_sin DOUBLE,
            dow_cos DOUBLE,
            month_sin DOUBLE,
            month_cos DOUBLE,
            is_rush_hour BOOLEAN,
            pm25_lag_1h DOUBLE,
            pm25_lag_3h DOUBLE,
            pm25_lag_6h DOUBLE,
            pm25_lag_12h DOUBLE,
            pm25_lag_24h DOUBLE,
            pm25_roll_mean_3h DOUBLE,
            pm25_roll_mean_6h DOUBLE,
            pm25_roll_mean_24h DOUBLE,
            pm25_roll_max_24h DOUBLE,
            pm25_roll_std_24h DOUBLE,
            pm25_next_6h DOUBLE,
            pm25_next_12h DOUBLE,
            pm25_next_24h DOUBLE,
            year INT,
            month_partition INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month_partition)
        TBLPROPERTIES ('format-version'='2')
        """
    )
    existing = set(spark.table(table_name).columns)
    for column, dtype in OUTPUT_COLUMN_TYPES.items():
        if column not in existing:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {column} {dtype}")


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("hour") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("hour") <= F.to_date(F.lit(end_date)))
    return df


def mask_targets_crossing_cutoff(df, end_date: str):
    if not end_date:
        return df
    cutoff = F.to_timestamp(F.lit(f"{end_date} 23:59:59"))
    for target_col, horizon_h in TARGET_HORIZONS.items():
        target_time = F.expr(f"hour + INTERVAL {horizon_h} HOURS")
        df = df.withColumn(
            target_col,
            F.when(target_time <= cutoff, F.col(target_col)).otherwise(F.lit(None).cast("double")),
        )
    return df


def build_training_dataset(master, dataset_version: str, feature_set_name: str):
    filtered = master.dropna(subset=TARGET_COLUMNS)
    order_w = Window.orderBy("hour")
    all_w = Window.partitionBy()

    groups_expr = F.array(*[F.lit(item) for item in FEATURE_GROUPS])
    with_index = (
        filtered
        .withColumn("_row_num", F.row_number().over(order_w))
        .withColumn("_total_rows", F.count(F.lit(1)).over(all_w))
        .withColumn("_ratio", (F.col("_row_num") - F.lit(1)) / F.col("_total_rows"))
        .withColumn(
            "split",
            F.when(F.col("_ratio") < F.lit(0.70), F.lit("train"))
            .when(F.col("_ratio") < F.lit(0.85), F.lit("validation"))
            .otherwise(F.lit("test")),
        )
        .withColumn("dataset_version", F.lit(dataset_version))
        .withColumn("feature_set_name", F.lit(feature_set_name))
        .withColumn("feature_groups", groups_expr)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("spark_processed_at", F.current_timestamp())
        .drop("_row_num", "_total_rows", "_ratio")
    )
    return with_index.select(*OUTPUT_COLUMNS)


def log_metrics(df) -> None:
    count = df.count()
    print(f"output_count={count}")
    if not count:
        print("warning=training_dataset_empty")
        return
    split_counts = {row["split"]: row["count"] for row in df.groupBy("split").count().collect()}
    horizon_counts = df.agg(
        F.sum(F.when(F.col("pm25_next_6h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_6h"),
        F.sum(F.when(F.col("pm25_next_12h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_12h"),
        F.sum(F.when(F.col("pm25_next_24h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_24h"),
    ).first().asDict()
    bounds_by_split = df.groupBy("split").agg(F.min("hour").alias("min_hour"), F.max("hour").alias("max_hour"), F.count("*").alias("count"))
    print(f"train_validation_test_counts={split_counts}")
    print(f"target_non_null_count_by_horizon={horizon_counts}")
    bounds_by_split.show(truncate=False)


def delete_date_window(spark: SparkSession, table_name: str, time_col: str, start_date: str, end_date: str) -> None:
    predicates = []
    if start_date:
        predicates.append(f"to_date({time_col}) >= DATE '{start_date}'")
    if end_date:
        predicates.append(f"to_date({time_col}) <= DATE '{end_date}'")
    if predicates:
        spark.sql(f"DELETE FROM {table_name} WHERE {' AND '.join(predicates)}")
    else:
        spark.sql(f"DELETE FROM {table_name}")


def write_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool, start_date: str, end_date: str) -> None:
    if full_refresh:
        delete_date_window(spark, table_name, "hour", start_date, end_date)
    df.createOrReplaceTempView("hanoi_pm25_training_updates")
    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING hanoi_pm25_training_updates s
        ON t.dataset_version = s.dataset_version
           AND t.feature_set_name = s.feature_set_name
           AND t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    target_table = os.getenv("ICEBERG_TABLE", tables["training_gold"])
    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["master_gold"])

    ensure_table(spark, target_table)
    master = apply_date_range(spark.table(source_table), args.start_date, args.end_date)
    master = mask_targets_crossing_cutoff(master, args.end_date)
    training = build_training_dataset(master, args.dataset_version, args.feature_set_name)
    log_metrics(training)
    write_iceberg(spark, training, target_table, full_refresh=as_bool(args.full_refresh), start_date=args.start_date, end_date=args.end_date)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
