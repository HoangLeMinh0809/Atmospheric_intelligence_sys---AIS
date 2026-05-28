"""
Build PM2.5 timeseries - observed history + forecast future for chart visualization.
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hanoi_config import TABLES, get_visualization_product_version, get_visualization_schema_version
from hanoi_config import get_visualization_observation_history_hours


def build_spark() -> SparkSession:
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "ais")
    return (
        SparkSession.builder
        .appName("AIS_VisualizationPM25Timeseries")
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{iceberg_catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg"))
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000"))
        .config("spark.hadoop.dfs.client.use.datanode.hostname", os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"))
        .getOrCreate()
    )


def build_timeseries(
    spark: SparkSession,
    base_time: datetime,
    dry_run: bool = False
) -> None:
    """Build observed and forecast timeseries."""
    
    spark.sparkContext.setLogLevel("WARN")
    
    product_version = get_visualization_product_version()
    schema_version = get_visualization_schema_version()
    visualization_run_id = f"timeseries_{base_time.strftime('%Y%m%d_%H%M%S')}"
    location_id = os.getenv("LOCATION_ID", "hanoi")
    location_name = os.getenv("LOCATION_NAME", "Hanoi")
    history_hours = get_visualization_observation_history_hours()
    
    try:
        series_rows = []
        
        # Observed history
        obs_history_start = base_time - timedelta(hours=history_hours)
        obs_df = spark.table(TABLES["openaq_station_silver"]).filter(
            (F.col("timestamp") >= F.lit(obs_history_start.isoformat())) &
            (F.col("timestamp") <= F.lit(base_time.isoformat()))
        ).select(
            F.col("timestamp"),
            F.col("pm25"),
            F.lit("observed").alias("series_type"),
            F.lit(0).alias("horizon_h"),
            F.lit("openaq_station_silver").alias("source_table"),
            F.col("station_id").alias("source_id"),
            F.lit(None).alias("model_version")
        )
        
        if obs_df.count() > 0:
            obs_enriched = obs_df.withColumn(
                "series_id", F.lit(str(uuid.uuid4()))
            ).withColumn(
                "visualization_run_id", F.lit(visualization_run_id)
            ).withColumn(
                "product_version", F.lit(product_version)
            ).withColumn(
                "schema_version", F.lit(schema_version)
            ).withColumn(
                "location_id", F.lit(location_id)
            ).withColumn(
                "location_name", F.lit(location_name)
            ).withColumn(
                "base_time", F.lit(base_time.isoformat())
            ).withColumn(
                "risk", F.when(F.col("pm25") < 12, "low")
                        .when(F.col("pm25") < 35.4, "medium")
                        .when(F.col("pm25") < 55.4, "high")
                        .otherwise("very_high")
            ).withColumn(
                "generated_at", F.current_timestamp()
            ).withColumn(
                "year", F.year(F.col("timestamp"))
            ).withColumn(
                "month", F.month(F.col("timestamp"))
            ).withColumn(
                "day", F.dayofmonth(F.col("timestamp"))
            ).select(
                "series_id", "visualization_run_id", "product_version", "schema_version",
                "location_id", "location_name", "base_time", "timestamp",
                "series_type", "horizon_h", "pm25", "risk", "source_table", "source_id", "model_version",
                "generated_at", "year", "month", "day"
            )
            series_rows.append(obs_enriched)
        
        # Forecast future
        try:
            pred_df = spark.table(TABLES["prediction_gold"]).filter(
                F.col("base_hour") <= F.lit(base_time.isoformat())
            ).orderBy(F.desc("base_hour")).limit(1)
            
            # Explode forecast horizons
            forecast_rows = []
            for horizon_h in [6, 12, 24]:
                col_name = f"pm25_{horizon_h}h"
                if col_name in pred_df.columns:
                    row = pred_df.select(
                        F.lit(str(uuid.uuid4())).alias("series_id"),
                        F.lit(visualization_run_id).alias("visualization_run_id"),
                        F.lit(product_version).alias("product_version"),
                        F.lit(schema_version).alias("schema_version"),
                        F.lit(location_id).alias("location_id"),
                        F.lit(location_name).alias("location_name"),
                        F.col("base_hour").alias("base_time"),
                        F.col("base_hour") + F.expr(f"INTERVAL {horizon_h} HOUR").cast("timestamp"),
                        F.lit("forecast").alias("series_type"),
                        F.lit(horizon_h).alias("horizon_h"),
                        F.col(col_name).alias("pm25"),
                        F.when(F.col(col_name) < 12, "low")
                         .when(F.col(col_name) < 35.4, "medium")
                         .when(F.col(col_name) < 55.4, "high")
                         .otherwise("very_high").alias("risk"),
                        F.lit("prediction_gold").alias("source_table"),
                        F.col("prediction_id").alias("source_id"),
                        F.col("model_version"),
                        F.current_timestamp().alias("generated_at"),
                        F.year(F.col("base_hour")).alias("year"),
                        F.month(F.col("base_hour")).alias("month"),
                        F.dayofmonth(F.col("base_hour")).alias("day")
                    )
                    forecast_rows.append(row)
            
            if forecast_rows:
                forecast_df = forecast_rows[0]
                for row in forecast_rows[1:]:
                    forecast_df = forecast_df.unionByName(row)
                series_rows.append(forecast_df)
        except Exception as e:
            print(f"[timeseries] Warning: forecast data not available: {str(e)}")
        
        # Union all rows
        if series_rows:
            final_df = series_rows[0]
            for row_df in series_rows[1:]:
                final_df = final_df.unionByName(row_df, allowMissingColumns=True)
            
            if not dry_run:
                final_df.write.format("iceberg").mode("append").save(TABLES["visualization_pm25_timeseries_gold"])
                print(f"[timeseries] Wrote {final_df.count()} timeseries rows for {location_id}")
            else:
                print(f"[timeseries] DRY_RUN: would write {final_df.count()} timeseries rows")
                final_df.show()
        else:
            print("[timeseries] No data to write")
            
    except Exception as e:
        print(f"[timeseries] ERROR: {str(e)}")
        import traceback
        traceback.print_exc()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-time", type=str, default=datetime.now(timezone.utc).isoformat()[:19] + "Z")
    parser.add_argument("--dry-run", type=int, default=0)
    args = parser.parse_args()
    
    base_time = datetime.fromisoformat(args.base_time.replace("Z", "+00:00"))
    dry_run = bool(args.dry_run)
    
    spark = build_spark()
    build_timeseries(spark, base_time, dry_run)
    spark.stop()


if __name__ == "__main__":
    main()
