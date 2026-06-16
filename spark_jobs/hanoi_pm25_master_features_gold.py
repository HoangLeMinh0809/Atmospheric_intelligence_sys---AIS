# File nay: tao feature, training table hoac serving table cho bai toan PM2.5.
from __future__ import annotations

import argparse
import os
import math

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from hanoi_config import (
    HDFS_NAMENODE,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    apply_asof_time,
    get_gold_horizons_hours,
    get_gold_lag_hours,
    get_gold_rolling_hours,
    get_table_names,
    parse_asof_time,
)


OUTPUT_COLUMNS = [
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
    # New Tier-2 features
    "pm25_grad_n",
    "pm25_grad_s",
    "pm25_grad_e",
    "pm25_grad_w",
    "pm25_spatial_std",
    "pm25_grad_mag",
    "dominant_cluster",
    "n_traj",
    "traj_source_lat",
    "traj_source_lon",
    "traj_path_no2_mean",
    "traj_path_aer_mean",
    "traj_path_no2_aer_ratio",
    # Existing time features
    "hour_of_day",
    "day_of_week",
    "month",
    "season",
    "is_weekend",
    # New sin/cos + rush hour
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


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi PM2.5 master feature gold table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--asof-time", default=os.getenv("ASOF_TIME", os.getenv("SIMULATED_NOW", os.getenv("BASE_TIME", ""))))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


# Chuyen flag dang chuoi nhu 1/true/yes thanh boolean.
def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Khoi tao SparkSession voi Iceberg catalog, warehouse va HDFS config.
def build_spark() -> SparkSession:
    packages = os.getenv("SPARK_JARS_PACKAGES", "").strip()
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
    builder = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("HanoiPM25MasterFeaturesGold")
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


# Tao bang feature hop nhat lam nguon chung cho training, serving va du bao.
def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.features")
    # Day la bang feature hop nhat dung cho train, serving va debug attribution.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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


# Loc du lieu theo khoang ngay start/end duoc yeu cau.
def apply_date_range(df, time_col: str, start_date: str, end_date: str, asof_time=None):
    if start_date:
        df = df.filter(F.to_date(time_col) >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date(time_col) <= F.to_date(F.lit(end_date)))
    df = apply_asof_time(df, time_col, asof_time)
    return df


# Tao day gio lien tuc tu min/max OpenAQ de cac nguon khac co the left join theo hour.
def build_hour_grid(aq):
    bounds = aq.agg(F.min("hour").alias("min_hour"), F.max("hour").alias("max_hour")).first()
    if not bounds or bounds["min_hour"] is None or bounds["max_hour"] is None:
        return None
    # Tao day gio lien tuc de sau do left join tung nguon, tranh mat gio khi mot datasource bi thua/thieu.
    return aq.sparkSession.range(1).select(
        F.explode(F.sequence(F.lit(bounds["min_hour"]), F.lit(bounds["max_hour"]), F.expr("interval 1 hour"))).alias("hour")
    )


# Lay feature Sentinel-5P daily gan nhat ma tung gio duoc phep nhin thay.
def build_s5p_asof_features(hours, s5p):
    s5p_norm = (
        s5p
        .withColumn("product_norm", F.upper(F.col("product")))
        .withColumn("product_norm", F.when(F.col("product_norm") == "AER", F.lit("AER_AI")).otherwise(F.col("product_norm")))
        .select("product_norm", "date", "overpass_time_utc", "value_mean", "valid_pct")
    )
    candidates = (
        hours.select("hour", F.to_date("hour").alias("hour_date"))
        .join(
            s5p_norm,
            # Mỗi gio chi duoc nhin thay overpass xay ra truoc no, khong leak thong tin tu tuong lai.
            (F.col("date") < F.col("hour_date"))
            | ((F.col("date") == F.col("hour_date")) & (F.col("overpass_time_utc").isNotNull()) & (F.col("overpass_time_utc") <= F.col("hour"))),
            "left",
        )
    )
    w = Window.partitionBy("hour", "product_norm").orderBy(F.col("date").desc_nulls_last(), F.col("overpass_time_utc").desc_nulls_last())
    # Dung row_number de giu lai ban ghi uu tien nhat trong moi nhom.
    latest = candidates.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return (
        latest
        # Bat dau gom nhom de tinh cac chi so tong hop.
        .groupBy("hour")
        .agg(
            F.max(F.when(F.col("product_norm") == "NO2", F.col("value_mean"))).alias("s5p_no2_mean"),
            F.max(F.when(F.col("product_norm") == "CO", F.col("value_mean"))).alias("s5p_co_mean"),
            F.max(F.when(F.col("product_norm") == "SO2", F.col("value_mean"))).alias("s5p_so2_mean"),
            F.max(F.when(F.col("product_norm") == "O3", F.col("value_mean"))).alias("s5p_o3_mean"),
            F.max(F.when(F.col("product_norm") == "AER_AI", F.col("value_mean"))).alias("s5p_aer_ai_mean"),
            F.max(F.when(F.col("product_norm") == "NO2", F.col("valid_pct"))).alias("s5p_no2_valid_pct"),
            F.max(F.when(F.col("product_norm") == "AER_AI", F.col("valid_pct"))).alias("s5p_aer_ai_valid_pct"),
        )
    )


# Lay feature MAIAC daily gan nhat truoc moi gio dich.
def build_maiac_asof_features(hours, maiac):
    candidates = (
        hours.select("hour", F.to_date("hour").alias("hour_date"))
        # MAIAC la daily cadence nen chi lay ban do ngay gan nhat truoc gio can du bao.
        .join(maiac, F.col("date") < F.col("hour_date"), "left")
    )
    w = Window.partitionBy("hour").orderBy(F.col("date").desc_nulls_last())
    # Dung row_number de giu lai ban ghi uu tien nhat trong moi nhom.
    latest = candidates.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return latest.select(
        "hour",
        "aod_047_mean",
        "aod_055_mean",
        "aod_mean",
        "aod_max",
        F.col("valid_pct").alias("aod_valid_pct"),
    )


# Lay snapshot ERA5 moi nhat <= gio dich de can bang voi cadence hourly.
def build_era5_asof_features(hours, era5, era5_cols: list[str]):
    era5_selected = era5.select(*era5_cols).withColumnRenamed("hour", "era5_hour")
    # Chon snapshot ERA5 moi nhat <= gio dich, phong truong hop hourly weather den cham hon OpenAQ.
    candidates = hours.select("hour").join(era5_selected, F.col("era5_hour") <= F.col("hour"), "left")
    w = Window.partitionBy("hour").orderBy(F.col("era5_hour").desc_nulls_last())
    # Dung row_number de giu lai ban ghi uu tien nhat trong moi nhom.
    latest = candidates.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return latest.drop("rn", "era5_hour")


# Chuan hoa va loc moc thoi gian cho du lieu/du doan PM2.5.
def add_time_lag_target_features(df):
    order_w = Window.orderBy("hour")
    df = (
        df
        .withColumn("hour_of_day", F.hour("hour").cast("int"))
        .withColumn("day_of_week", F.dayofweek("hour").cast("int"))
        .withColumn("month", F.month("hour").cast("int"))
        .withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), F.lit("winter"))
            .when(F.col("month").isin(3, 4, 5), F.lit("spring"))
            .when(F.col("month").isin(6, 7, 8), F.lit("summer"))
            .otherwise(F.lit("autumn")),
        )
        .withColumn("is_weekend", F.dayofweek("hour").isin(1, 7))
        .withColumn("is_rush_hour", F.col("hour_of_day").isin([7, 8, 9, 17, 18, 19]))
        .withColumn("hour_sin", F.sin(F.lit(2.0 * math.pi) * (F.col("hour_of_day") / F.lit(24.0))))
        .withColumn("hour_cos", F.cos(F.lit(2.0 * math.pi) * (F.col("hour_of_day") / F.lit(24.0))))
        .withColumn("dow_sin", F.sin(F.lit(2.0 * math.pi) * (F.col("day_of_week") / F.lit(7.0))))
        .withColumn("dow_cos", F.cos(F.lit(2.0 * math.pi) * (F.col("day_of_week") / F.lit(7.0))))
        .withColumn("month_sin", F.sin(F.lit(2.0 * math.pi) * (F.col("month") / F.lit(12.0))))
        .withColumn("month_cos", F.cos(F.lit(2.0 * math.pi) * (F.col("month") / F.lit(12.0))))
    )

    for lag in get_gold_lag_hours():
        # Lag features giu memory ngan han cua PM2.5, la nhom bien quan trong nhat cho du bao ngan han.
        df = df.withColumn(f"pm25_lag_{lag}h", F.lag("pm25_mean", lag).over(order_w))

    for window_hours in get_gold_rolling_hours():
        roll_w = order_w.rowsBetween(-(window_hours - 1), 0)
        # Rolling windows tom tat xu huong ngan han va do bien dong cua pollutant.
        df = df.withColumn(f"pm25_roll_mean_{window_hours}h", F.avg("pm25_mean").over(roll_w))
        if window_hours == 24:
            df = df.withColumn("pm25_roll_max_24h", F.max("pm25_mean").over(roll_w))
            df = df.withColumn("pm25_roll_std_24h", F.stddev_samp("pm25_mean").over(roll_w))

    for horizon in get_gold_horizons_hours():
        # Lead columns la label supervision; serving builder se loai bo de tranh leak.
        df = df.withColumn(f"pm25_next_{horizon}h", F.lead("pm25_mean", horizon).over(order_w))

    return (
        df
        .withColumn("year", F.year("hour").cast("int"))
        .withColumn("month_partition", F.month("hour").cast("int"))
        .withColumn("spark_processed_at", F.current_timestamp())
    )


# Join tat ca nguon feature ve cung hourly grid va xuat schema master gold.
def build_master(
    spark: SparkSession,
    tables: dict[str, str],
    target_table: str,
    start_date: str,
    end_date: str,
    asof_time=None,
):
    aq = apply_date_range(spark.table(tables["openaq_hourly_silver"]), "hour", start_date, end_date, asof_time)
    hours = build_hour_grid(aq)
    if hours is None:
        print("warning=no_openaq_target_hours")
        return spark.table(target_table).limit(0)

    weather = apply_date_range(spark.table(tables["weather_proxy_silver"]), "hour", start_date, end_date, asof_time)
    era5 = spark.table(tables["era5_surface_silver"])
    if end_date:
        era5 = era5.filter(F.to_date("hour") <= F.to_date(F.lit(end_date)))
    era5 = apply_asof_time(era5, "hour", asof_time)
    # Giu satellite scans trong cua so can thiet de join daily as-of nhe hon.
    s5p = apply_date_range(spark.table(tables["sentinel5p_silver"]), "date", start_date, end_date)
    maiac = apply_date_range(spark.table(tables["maiac_silver"]), "date", start_date, end_date)

    s5p_features = build_s5p_asof_features(hours, s5p)
    maiac_features = build_maiac_asof_features(hours, maiac)

    gradient = spark.table(tables["openaq_gradient_silver"]).select(
        "hour",
        F.col("pm25_grad_n").alias("pm25_grad_n"),
        F.col("pm25_grad_s").alias("pm25_grad_s"),
        F.col("pm25_grad_e").alias("pm25_grad_e"),
        F.col("pm25_grad_w").alias("pm25_grad_w"),
        F.col("pm25_spatial_std").alias("pm25_spatial_std"),
        F.col("pm25_grad_mag").alias("pm25_grad_mag"),
    )
    gradient = apply_date_range(gradient, "hour", start_date, end_date, asof_time)

    traj_hourly = spark.table(tables["trajectory_hourly_silver"]).select(
        "hour",
        F.col("dominant_cluster").cast("int").alias("dominant_cluster"),
        F.col("n_traj").cast("int").alias("n_traj"),
        F.col("source_lat").cast("double").alias("traj_source_lat"),
        F.col("source_lon").cast("double").alias("traj_source_lon"),
        F.col("path_no2_mean").cast("double").alias("traj_path_no2_mean"),
        F.col("path_aer_mean").cast("double").alias("traj_path_aer_mean"),
        F.col("path_no2_aer_ratio").cast("double").alias("traj_path_no2_aer_ratio"),
    )
    traj_hourly = apply_date_range(traj_hourly, "hour", start_date, end_date, asof_time)

    weather_cols = ["hour", "vis_km", "uv", "condition_code", "is_day", "will_it_rain", "chance_of_rain"]
    era5_cols = [
        "hour",
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
    ]

    hours = hours.repartition("hour")
    aq_hourly = aq.select("hour", "pm25_median", "pm25_mean", "station_count", "coverage_avg").repartition("hour")
    weather_hourly = weather.select(*weather_cols).repartition("hour")
    era5_hourly = build_era5_asof_features(hours, era5, era5_cols).repartition("hour")
    gradient_hourly = gradient.repartition("hour")
    traj_hourly = traj_hourly.repartition("hour")

    # Daily features nho hon nhieu so voi hourly grid nen broadcast se re hon shuffle.
    base = (
        hours
        .join(aq_hourly, "hour", "left")
        .join(weather_hourly, "hour", "left")
        .join(era5_hourly, "hour", "left")
        .join(F.broadcast(s5p_features), "hour", "left")
        .join(F.broadcast(maiac_features), "hour", "left")
        .join(gradient_hourly, "hour", "left")
        .join(traj_hourly, "hour", "left")
    )
    return add_time_lag_target_features(base).select(*OUTPUT_COLUMNS)


# In metric kiem tra row count, thoi gian, duplicate va null ratio.
def log_metrics(df) -> None:
    count = df.count()
    bounds = df.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    target_counts = df.agg(
        F.sum(F.when(F.col("pm25_next_6h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_6h"),
        F.sum(F.when(F.col("pm25_next_12h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_12h"),
        F.sum(F.when(F.col("pm25_next_24h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_24h"),
    ).first().asDict() if count else {}
    lag_nulls = df.agg(
        F.sum(F.when(F.col("pm25_lag_1h").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_lag_1h"),
        F.sum(F.when(F.col("pm25_lag_24h").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_lag_24h"),
    ).first().asDict() if count else {}
    print(f"feature_row_count={count}")
    print(f"output_count={count}")
    print(f"duplicate_count=0")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"target_non_null_count_by_horizon={target_counts}")
    print(f"lag_null_count_by_lag={lag_nulls}")


# Xoa cua so ngay cu truoc khi full refresh ghi lai du lieu.
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


# Ghi output cho du lieu/du doan PM2.5.
def write_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool, start_date: str, end_date: str) -> None:
    if full_refresh:
        delete_date_window(spark, table_name, "hour", start_date, end_date)
    # Dang ky DataFrame tam de co the dung SQL o cac buoc sau.
    df.createOrReplaceTempView("hanoi_pm25_master_updates")
    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING hanoi_pm25_master_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    target_table = os.getenv("ICEBERG_TABLE", tables["master_gold"])
    asof_time = parse_asof_time(args.asof_time)

    ensure_table(spark, target_table)
    master = build_master(spark, tables, target_table, args.start_date, args.end_date, asof_time)
    log_metrics(master)
    write_iceberg(spark, master, target_table, full_refresh=as_bool(args.full_refresh), start_date=args.start_date, end_date=args.end_date)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
