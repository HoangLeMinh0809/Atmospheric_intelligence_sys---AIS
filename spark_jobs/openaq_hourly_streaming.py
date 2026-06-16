# File nay: stream OpenAQ tu Kafka vao Iceberg bronze.
"""
OpenAQ hourly Kafka -> Iceberg streaming processor.

Default mode is long-running streaming.
Use --stop-after-batch 1 for bootstrap/backfill catchup runs.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofmonth,
    from_json,
    hour,
    month as spark_month,
    to_timestamp,
    year as spark_year,
)
from pyspark.sql.types import (
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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "openaq-hourly")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/openaq_hourly/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

TABLE_NAMES = get_table_names()
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", TABLE_NAMES["openaq_bronze"])

OPENAQ_SCHEMA = StructType(
    [
        StructField("location_id", LongType(), True),
        StructField("location_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("provider", StringType(), True),
        StructField("sensor_id", LongType(), True),
        StructField("parameter", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("datetime_utc", StringType(), True),
        StructField("datetime_local", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("min", DoubleType(), True),
        StructField("max", DoubleType(), True),
        StructField("sd", DoubleType(), True),
        StructField("expected_count", LongType(), True),
        StructField("observed_count", LongType(), True),
        StructField("coverage_pct", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("window_mode", StringType(), True),
        StructField("window_start_utc", StringType(), True),
        StructField("window_end_utc", StringType(), True),
        StructField("window_now_utc", StringType(), True),
        StructField("event_id", StringType(), True),
    ] + contract_schema_fields()
)


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    stop_after_batch, processing_time = parse_streaming_runtime(default_processing_time="30 seconds")

    spark = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("OpenAQHourly_Streaming")
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
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        # Parse chuoi JSON thanh cot co schema ro rang.
        .select(col("json_str"), from_json(col("json_str"), OPENAQ_SCHEMA).alias("data"))
        .select("json_str", "data.*")
        .withColumnRenamed("json_str", "_raw_payload")
    )

    final_df = add_contract_columns(
        parsed_df
        .withColumn("event_time", to_timestamp(col("datetime_utc")))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("hour", hour(col("event_time")))
        .withColumn("spark_processed_at", col("ingest_time").cast("timestamp"))
        .select("*")
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            location_id BIGINT,
            location_name STRING,
            city STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            provider STRING,
            sensor_id BIGINT,
            parameter STRING,
            unit STRING,
            datetime_utc STRING,
            datetime_local STRING,
            value DOUBLE,
            min DOUBLE,
            max DOUBLE,
            sd DOUBLE,
            expected_count BIGINT,
            observed_count BIGINT,
            coverage_pct DOUBLE,
            source STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_id STRING,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day, hour)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    print(f"OpenAQ stream mode: {'availableNow' if stop_after_batch else processing_time}")
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
