# File nay: xu ly ERA5 thanh bang khi tuong hoac dau vao ARL cho HYSPLIT.
"""ERA5 file metadata Kafka -> Iceberg streaming processor.

This job consumes JSON events from Kafka topic `era5-files` and writes them
into Iceberg table `ais.weather.era5_files_bronze`.

Modes:
- Streaming (default)
- One-shot backfill: --stop-after-batch 1

Acceptance criteria:
- availableNow backfill works
- streaming works
- dedupe by event_id
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    desc_nulls_last,
    from_json,
    row_number,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import SPARK_SQL_SESSION_TIMEZONE, get_table_names
from runtime_utils import apply_stream_trigger, parse_streaming_runtime
from streaming_bronze_utils import add_contract_columns, contract_schema_fields, start_bronze_streams


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "era5-files")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/era5_files/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

TABLE_NAMES = get_table_names()
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", TABLE_NAMES["era5_files_bronze"])


ERA5_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("dataset_type", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("bbox", ArrayType(DoubleType()), True),
        StructField("file_path", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("checksum", StringType(), True),
        StructField("surface_file_path", StringType(), True),
        StructField("surface_file_size", LongType(), True),
        StructField("surface_checksum", StringType(), True),
        StructField("source", StringType(), True),
        StructField("ingest_time", StringType(), True),
    ] + contract_schema_fields()
)


# Tao bang bronze nhan metadata file ERA5 tu Kafka.
def ensure_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.weather")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            event_id STRING,
            dataset_type STRING,
            year INT,
            month INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            bbox ARRAY<DOUBLE>,
            file_path STRING,
            file_size BIGINT,
            checksum STRING,
            source STRING,
            ingest_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            surface_file_path STRING,
            surface_file_size BIGINT,
            surface_checksum STRING
        )
        USING ICEBERG
        PARTITIONED BY (dataset_type, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )


# Merge mot micro-batch metadata ERA5 vao bronze sau khi dedupe theo `event_id`.
def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    window = Window.partitionBy("event_id").orderBy(
        desc_nulls_last("ingest_time"),
        desc_nulls_last("spark_processed_at"),
    )
    updates = (
        batch_df
        .where(col("event_id").isNotNull())
        # Dung row_number de giu lai ban ghi uu tien nhat trong moi nhom.
        .withColumn("_rn", row_number().over(window))
        .where(col("_rn") == 1)
        .drop("_rn")
    )
    # Dang ky DataFrame tam de co the dung SQL o cac buoc sau.
    updates.createOrReplaceTempView("era5_file_updates")
    batch_df.sparkSession.sql(
        f"""
        MERGE INTO {ICEBERG_TABLE} t
        USING era5_file_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"era5_files_batch={{'batch_id': {batch_id}, 'rows': {updates.count()}}}")


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    stop_after_batch, processing_time = parse_streaming_runtime(default_processing_time="30 seconds")

    spark = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("ERA5Files_Streaming")
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
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_str")
        # Parse chuoi JSON thanh cot co schema ro rang.
        .select(col("json_str"), from_json(col("json_str"), ERA5_SCHEMA).alias("data"))
        .select("json_str", "data.*")
        .withColumnRenamed("json_str", "_raw_payload")
    )

    final_df = add_contract_columns(
        parsed_df
        .withColumn("start_time", to_timestamp(col("start_time")))
        .withColumn("end_time", to_timestamp(col("end_time")))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("spark_processed_at", current_timestamp().cast(TimestampType()))
        .withColumn("event_time", to_timestamp(col("event_time")))
        .select("*")
    )

    ensure_table(spark)

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
