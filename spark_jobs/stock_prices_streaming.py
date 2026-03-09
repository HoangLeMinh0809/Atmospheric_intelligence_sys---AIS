"""
=============================================================================
Stock Prices Daily — Spark Structured Streaming Job
=============================================================================
Chức năng:
  1. Đọc stream từ Kafka topic "stock-prices-daily"
  2. Parse JSON message theo schema đã định nghĩa
  3. Cast kiểu dữ liệu
  4. Thêm cột partition: year, month
  5. Ghi Parquet ra HDFS tại /data/stock_prices_daily/
  6. Sử dụng checkpoint để đảm bảo exactly-once

Lưu ý: Đây là pass-through pipeline, KHÔNG có business transform phức tạp.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    year as spark_year,
    month as spark_month,
    to_date,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

# =============================================================================
# Cấu hình
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "stock-prices-daily"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/stock_prices_daily/"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/stock_prices_daily/"

# =============================================================================
# Schema cho JSON message từ Kafka
# Phải khớp với output của ingest service
# =============================================================================
STOCK_PRICE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("trade_date", StringType(), True),
    StructField("adjusted_close", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("price_change_pct", DoubleType(), True),
    StructField("matched_volume", LongType(), True),
    StructField("matched_value", LongType(), True),
    StructField("negotiated_volume", LongType(), True),
    StructField("negotiated_value", LongType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("event_id", StringType(), True),
])


def main():
    # =========================================================================
    # 1. Tạo Spark Session
    # =========================================================================
    spark = (
        SparkSession.builder
        .appName("StockPricesDaily_Streaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        # Cấu hình HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("SPARK STRUCTURED STREAMING — stock-prices-daily")
    print("=" * 60)

    # =========================================================================
    # 2. Đọc stream từ Kafka
    # =========================================================================
    kafka_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")       # đọc từ đầu nếu lần đầu
        .option("failOnDataLoss", "false")           # không crash nếu mất data
        .option("maxOffsetsPerTrigger", 10000)       # giới hạn batch size
        .load()
    )

    # =========================================================================
    # 3. Parse JSON từ Kafka value
    # =========================================================================
    # Kafka message có key và value dạng binary → cast sang string
    parsed_df = (
        kafka_df
        .selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_str")
        .select(
            col("kafka_key"),
            from_json(col("json_str"), STOCK_PRICE_SCHEMA).alias("data"),
        )
        .select("data.*")  # flatten struct thành các cột riêng biệt
    )

    # =========================================================================
    # 4. Cast kiểu dữ liệu & thêm cột partition
    # =========================================================================
    final_df = (
        parsed_df
        # Cast trade_date từ string sang DateType
        .withColumn("trade_date", to_date(col("trade_date"), "yyyy-MM-dd"))
        # Thêm cột partition theo năm, tháng
        .withColumn("year", spark_year(col("trade_date")))
        .withColumn("month", spark_month(col("trade_date")))
        # Thêm thời gian Spark processing (để debug/audit)
        .withColumn("spark_processed_at", current_timestamp())
    )

    # =========================================================================
    # 5. Ghi ra HDFS dưới dạng Parquet
    # =========================================================================
    query = (
        final_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        # Partition theo year/month → tổ chức file tốt hơn
        .partitionBy("year", "month")
        # Trigger: xử lý micro-batch mỗi 30 giây
        .trigger(processingTime="30 seconds")
        .queryName("stock_prices_daily_to_hdfs")
        .start()
    )

    print(f"Streaming query started: {query.name}")
    print(f"  Kafka topic: {KAFKA_TOPIC}")
    print(f"  HDFS output: {HDFS_OUTPUT_PATH}")
    print(f"  Checkpoint:  {CHECKPOINT_PATH}")
    print("Waiting for data...")

    # Block cho đến khi query bị terminate
    query.awaitTermination()


if __name__ == "__main__":
    main()
