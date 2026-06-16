# File nay: stream/catch-up event Kafka vao Iceberg.
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType


LINEAGE_COLUMNS = {
    "schema_version": "STRING",
    "available_at": "TIMESTAMP",
    "quality_flags": "ARRAY<STRING>",
    "request_id": "STRING",
    "producer": "STRING",
    "retry_count": "INT",
    "job_run_id": "STRING",
    "input_snapshot_id": "STRING",
    "output_snapshot_id": "STRING",
}


# Cac truong contract chung ma moi event bronze hop le phai cung cap.
def contract_schema_fields() -> list[StructField]:
    return [
        StructField("event_time", StringType(), True),
        StructField("schema_version", StringType(), True),
        StructField("available_at", StringType(), True),
        StructField("quality_flags", ArrayType(StringType()), True),
        StructField("payload", MapType(StringType(), StringType(), True), True),
        StructField(
            "trace",
            StructType(
                [
                    StructField("request_id", StringType(), True),
                    StructField("producer", StringType(), True),
                    StructField("retry_count", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]


# Bo sung cac cot lineage/audit neu bang dich chua co.
def ensure_columns(spark: SparkSession, table_name: str, columns: dict[str, str]) -> None:
    existing = set(spark.table(table_name).columns)
    for name, dtype in columns.items():
        if name not in existing:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {name} {dtype}")


# Tao hai bang audit de giu event invalid contract va event den muon.
def ensure_audit_tables(spark: SparkSession, catalog: str) -> tuple[str, str]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.audit")
    ddl = """
        source_topic STRING,
        event_id STRING,
        source STRING,
        event_time TIMESTAMP,
        ingest_time TIMESTAMP,
        schema_version STRING,
        available_at TIMESTAMP,
        request_id STRING,
        producer STRING,
        retry_count INT,
        reason STRING,
        raw_payload STRING,
        recorded_at TIMESTAMP,
        year INT,
        month INT,
        day INT
    """
    invalid_table = f"{catalog}.audit.invalid_events_bronze"
    late_table = f"{catalog}.audit.late_events_bronze"
    for table in (invalid_table, late_table):
        spark.sql(
            f"""
                CREATE TABLE IF NOT EXISTS {table} ({ddl})
            USING ICEBERG
            PARTITIONED BY (source_topic, year, month, day)
            TBLPROPERTIES ('format-version'='2')
            """
        )
    return invalid_table, late_table


# Flatten trace metadata va them placeholder lineage truoc khi merge vao bronze.
def add_contract_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("available_at", F.to_timestamp("available_at"))
        .withColumn("request_id", F.col("trace.request_id"))
        .withColumn("producer", F.col("trace.producer"))
        .withColumn("retry_count", F.col("trace.retry_count").cast("int"))
        .withColumn("job_run_id", F.lit(os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or os.getenv("JOB_RUN_ID") or "streaming"))
        .withColumn("input_snapshot_id", F.lit(None).cast("string"))
        .withColumn("output_snapshot_id", F.lit(None).cast("string"))
    )


# Chuan hoa event bi loai thanh schema audit chung de co the append vao bang theo doi loi.
def _audit_rows(df: DataFrame, topic: str, reason: str) -> DataFrame:
    recorded_at = F.current_timestamp()
    return df.select(
        F.lit(topic).alias("source_topic"),
        F.col("event_id").cast("string"),
        F.col("source").cast("string"),
        F.col("event_time").cast("timestamp"),
        F.col("ingest_time").cast("timestamp"),
        F.col("schema_version").cast("string"),
        F.col("available_at").cast("timestamp"),
        F.col("request_id").cast("string"),
        F.col("producer").cast("string"),
        F.col("retry_count").cast("int"),
        F.lit(reason).alias("reason"),
        F.col("_raw_payload").cast("string").alias("raw_payload"),
        recorded_at.alias("recorded_at"),
        F.year(recorded_at).alias("year"),
        F.month(recorded_at).alias("month"),
        F.dayofmonth(recorded_at).alias("day"),
    )


# Dedupe tung micro-batch theo `event_id`, merge vao bang bronze, va cap nhat lineage snapshot neu lay duoc.
def _merge_batch(batch_df: DataFrame, batch_id: int, table_name: str) -> None:
    if batch_df.isEmpty():
        return
    spark = batch_df.sparkSession
    target_columns = spark.table(table_name).columns
    updates = (
        batch_df
        .withColumn("input_snapshot_id", F.lit(f"kafka_batch:{batch_id}"))
        .select(*[name for name in target_columns if name in batch_df.columns])
    )
    order_columns = [
        F.col(name).desc_nulls_last()
        for name in ("available_at", "ingest_time", "spark_processed_at")
        if name in updates.columns
    ]
    window = Window.partitionBy("event_id").orderBy(*(order_columns or [F.col("event_id")]))
    # Neu mot micro-batch co nhieu ban ghi cung event_id thi giu ban ghi moi nhat theo available/ingest time.
    updates = updates.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    # Dang ky DataFrame tam de co the dung SQL o cac buoc sau.
    updates.createOrReplaceTempView("bronze_stream_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING bronze_stream_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    try:
        snapshot_id = spark.sql(
            f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()["snapshot_id"]
        updates.select("event_id").distinct().withColumn(
            "output_snapshot_id", F.lit(str(snapshot_id))
        ).createOrReplaceTempView("bronze_lineage_updates")
        spark.sql(
            f"""
            MERGE INTO {table_name} t
            USING bronze_lineage_updates s
            ON t.event_id = s.event_id
            WHEN MATCHED THEN UPDATE SET t.output_snapshot_id = s.output_snapshot_id
            """
        )
    except Exception as exc:
        print(f"bronze_lineage_warning table={table_name} batch_id={batch_id} error={exc}")
    print(f"bronze_merge table={table_name} batch_id={batch_id} output_count={updates.count()}")


# Tach stream dau vao thanh 3 nhanh: invalid, late, va valid-merge; moi nhanh co checkpoint rieng.
def start_bronze_streams(
    df: DataFrame,
    *,
    table_name: str,
    topic: str,
    checkpoint_path: str,
    catalog: str,
    stop_after_batch: bool,
    processing_time: str,
    watermark: str = "24 hours",
):
    spark = df.sparkSession
    ensure_columns(spark, table_name, LINEAGE_COLUMNS)
    invalid_table, late_table = ensure_audit_tables(spark, catalog)

    invalid_condition = (
        F.col("event_id").isNull()
        | F.col("event_time").isNull()
        | F.col("schema_version").isNull()
        | (F.col("schema_version") != F.lit("v1"))
    )
    late_condition = (
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("ingest_time").isNotNull()
        & (F.col("ingest_time") > F.col("event_time") + F.expr(f"INTERVAL {watermark}"))
    )
    invalid = df.filter(invalid_condition)
    late = df.filter(late_condition & ~invalid_condition)
    valid = df.filter(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("schema_version") == F.lit("v1"))
        & ~late_condition
    )
    valid = valid.dropDuplicates(["event_id"]) if stop_after_batch else valid.withWatermark(
        "event_time", watermark
    ).dropDuplicates(["event_id"])

    # Backfill dung `availableNow`, con runtime thuong thi dung trigger theo chu ky.
    def trigger(writer):
        return writer.trigger(availableNow=True) if stop_after_batch else writer.trigger(processingTime=processing_time)

    query_prefix = topic.replace("-", "_")
    queries = [
        trigger(
            _audit_rows(invalid, topic, "invalid_contract")
            # Bat dau ghi ket qua streaming theo che do da cau hinh.
            .writeStream.format("iceberg").outputMode("append")
            .option("checkpointLocation", f"{checkpoint_path.rstrip('/')}/invalid")
            .queryName(f"{query_prefix}_invalid_events")
        ).toTable(invalid_table),
        trigger(
            _audit_rows(late, topic, f"older_than_{watermark.replace(' ', '_')}")
            # Bat dau ghi ket qua streaming theo che do da cau hinh.
            .writeStream.format("iceberg").outputMode("append")
            .option("checkpointLocation", f"{checkpoint_path.rstrip('/')}/late")
            .queryName(f"{query_prefix}_late_events")
        ).toTable(late_table),
        trigger(
            # Bat dau ghi ket qua streaming theo che do da cau hinh.
            valid.writeStream.foreachBatch(lambda batch, batch_id: _merge_batch(batch, batch_id, table_name))
            .option("checkpointLocation", f"{checkpoint_path.rstrip('/')}/valid")
            .queryName(f"{query_prefix}_bronze_merge")
        ).start(),
    ]
    for query in queries:
        query.awaitTermination()
