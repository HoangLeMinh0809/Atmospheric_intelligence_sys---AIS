# File nay: xu ly Sentinel-5P thanh bang satellite silver/summary cho Ha Noi.
"""
Sentinel-5P summary Kafka -> Iceberg streaming processor.

Default mode is long-running streaming.
Use --stop-after-batch 1 for bootstrap/backfill catchup runs.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    dayofmonth,
    from_json,
    month as spark_month,
    to_timestamp,
    year as spark_year,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from hanoi_config import SPARK_SQL_SESSION_TIMEZONE, get_table_names
from runtime_utils import apply_stream_trigger, parse_streaming_runtime
from streaming_bronze_utils import add_contract_columns, contract_schema_fields, start_bronze_streams

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sentinel5p-summary")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/sentinel5p_summary/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

TABLE_NAMES = get_table_names()
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", TABLE_NAMES["sentinel5p_bronze"])

SENTINEL5P_SCHEMA = StructType(
    [
        StructField("product", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("content_start", StringType(), True),
        StructField("content_end", StringType(), True),
        StructField("bbox", ArrayType(DoubleType()), True),
        StructField("product_name", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("download_url", StringType(), True),
        StructField("content_length", LongType(), True),
        StructField("s3_path", StringType(), True),
        StructField("raw_file_path", StringType(), True),
        StructField("raw_downloaded", BooleanType(), True),
        StructField("raw_download_error", StringType(), True),
        StructField(
            "stats",
            StructType(
                [
                    StructField("min", DoubleType(), True),
                    StructField("max", DoubleType(), True),
                    StructField("mean", DoubleType(), True),
                    StructField("valid_pct", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField("unit", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("window_mode", StringType(), True),
        StructField("window_start_utc", StringType(), True),
        StructField("window_end_utc", StringType(), True),
        StructField("window_now_utc", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
    ] + contract_schema_fields()
)


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    stop_after_batch, processing_time = parse_streaming_runtime(default_processing_time="45 seconds")

    spark = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("Sentinel5PSummary_Streaming")
        .config("spark.sql.session.timeZone", SPARK_SQL_SESSION_TIMEZONE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", os.getenv("HDFS_DEFAULT_FS", os.getenv("HADOOP_DEFAULT_FS", "hdfs://namenode:9000"))))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        # Doc stream dau vao de xu ly lien tuc hoac catch-up.
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        # Parse chuoi JSON thanh cot co schema ro rang.
        .select(col("json_str"), from_json(col("json_str"), SENTINEL5P_SCHEMA).alias("data"))
        .select("json_str", "data.*")
        .withColumnRenamed("json_str", "_raw_payload")
    )

    final_df = add_contract_columns(
        parsed_df
        .withColumn("stats_min", col("stats.min"))
        .withColumn("stats_max", col("stats.max"))
        .withColumn("stats_mean", col("stats.mean"))
        .withColumn("stats_valid_pct", col("stats.valid_pct"))
        .drop("stats")
        .withColumn("content_start_ts", to_timestamp(col("content_start")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("event_time", coalesce(col("content_start_ts"), col("window_end_utc"), col("ingest_time")))
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("spark_processed_at", col("ingest_time").cast("timestamp"))
        .select("*")
        .drop("content_start_ts")
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.satellite")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            product STRING,
            collection STRING,
            content_start STRING,
            content_end STRING,
            bbox ARRAY<DOUBLE>,
            product_name STRING,
            product_id STRING,
            file_name STRING,
            stats_min DOUBLE,
            stats_max DOUBLE,
            stats_mean DOUBLE,
            stats_valid_pct DOUBLE,
            unit STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_id STRING,
            source STRING,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT,
            download_url STRING,
            content_length BIGINT,
            s3_path STRING,
            raw_file_path STRING,
            raw_downloaded BOOLEAN,
            raw_download_error STRING
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    existing_columns = set(spark.table(ICEBERG_TABLE).columns)
    for column_name, column_type in [
        ("download_url", "STRING"),
        ("content_length", "BIGINT"),
        ("s3_path", "STRING"),
        ("raw_file_path", "STRING"),
        ("raw_downloaded", "BOOLEAN"),
        ("raw_download_error", "STRING"),
    ]:
        if column_name not in existing_columns:
            spark.sql(f"ALTER TABLE {ICEBERG_TABLE} ADD COLUMN {column_name} {column_type}")

    print(f"Sentinel-5P stream mode: {'availableNow' if stop_after_batch else processing_time}")
    print(f"Kafka startingOffsets: {KAFKA_STARTING_OFFSETS}")
    start_bronze_streams(
        final_df,
        table_name=ICEBERG_TABLE,
        topic=KAFKA_TOPIC,
        checkpoint_path=CHECKPOINT_PATH,
        catalog=ICEBERG_CATALOG,
        stop_after_batch=stop_after_batch,
        processing_time=processing_time,
    )


if __name__ == "__main__":
    main()
