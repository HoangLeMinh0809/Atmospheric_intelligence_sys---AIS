# File nay: tinh gradient khong gian PM2.5 tu cac tram OpenAQ.
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    apply_asof_time,
    get_hanoi_center,
    get_table_names,
    parse_asof_time,
)


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OpenAQ spatial gradient silver table")
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
    return (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("OpenAQSpatialGradientSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", os.getenv("HDFS_DEFAULT_FS", os.getenv("HADOOP_DEFAULT_FS", "hdfs://namenode:9000"))))
        .getOrCreate()
    )


# Tao bang hourly gradient PM2.5 quanh tam Ha Noi de bo sung spatial signal cho model/UI.
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


# Loc du lieu theo khoang ngay start/end duoc yeu cau.
def apply_date_range(df, start_date: str, end_date: str, asof_time=None):
    if start_date:
        df = df.filter(F.to_date("hour") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("hour") <= F.to_date(F.lit(end_date)))
    df = apply_asof_time(df, "hour", asof_time)
    return df


# Tinh toan chi so dan xuat cho du lieu OpenAQ PM2.5.
def compute_spatial_gradient(station, center_lat: float, center_lon: float):
    offset = 0.25

    valid = station.filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("pm25").isNotNull()
    )
    # Bat dau gom nhom de tinh cac chi so tong hop.
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
        # Dung row_number de giu lai ban ghi uu tien nhat trong moi nhom.
        expanded.withColumn("rank", F.row_number().over(nearest_window))
        .filter(F.col("rank") <= 3)
        .withColumn("weight", F.lit(1.0) / F.greatest(F.col("distance"), F.lit(1e-6)))
        # Bat dau gom nhom de tinh cac chi so tong hop.
        .groupBy("hour", "target")
        .agg((F.sum(F.col("weight") * F.col("pm25")) / F.sum("weight")).alias("pm25_idw"))
        # Bat dau gom nhom de tinh cac chi so tong hop.
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


# Doc station silver, tinh gradient khong gian, va tra them duplicate metric de log chat luong.
def build_output_df(spark: SparkSession, source_table: str, start_date: str, end_date: str, asof_time=None):
    station = spark.table(source_table)
    station = apply_date_range(station, start_date, end_date, asof_time)
    station = station.filter(F.col("hour").isNotNull())

    duplicate_count_row = (
        # Bat dau gom nhom de tinh cac chi so tong hop.
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


# Upsert DataFrame vao bang Iceberg theo khoa merge duoc truyen vao.
def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool, start_date: str, end_date: str) -> None:
    if full_refresh:
        delete_date_window(spark, table_name, "hour", start_date, end_date)

    # Dang ky DataFrame tam de co the dung SQL o cac buoc sau.
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


# In metric kiem tra row count, thoi gian, duplicate va null ratio.
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


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
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
        asof_time=parse_asof_time(args.asof_time),
    )
    log_metrics(station_df, output_df, duplicate_count)
    merge_iceberg(spark, output_df, target_table, full_refresh=full_refresh, start_date=args.start_date, end_date=args.end_date)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
