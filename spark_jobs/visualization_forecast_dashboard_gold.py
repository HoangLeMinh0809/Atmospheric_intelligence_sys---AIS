"""
Build PM2.5 forecast dashboard summary - one-row latest forecast for Hanoi.
Combines latest prediction, model metadata, and observation freshness.
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


def build_spark() -> SparkSession:
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "ais")
    return (
        SparkSession.builder
        .appName("AIS_VisualizationForecastDashboard")
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


def build_forecast_dashboard(
    spark: SparkSession,
    base_time: datetime,
    dry_run: bool = False
) -> None:
    """Build latest forecast dashboard summary row."""
    
    spark.sparkContext.setLogLevel("WARN")
    
    product_version = get_visualization_product_version()
    schema_version = get_visualization_schema_version()
    visualization_run_id = f"dashboard_{base_time.strftime('%Y%m%d_%H%M%S')}"
    location_id = os.getenv("LOCATION_ID", "hanoi")
    location_name = os.getenv("LOCATION_NAME", "Hanoi")
    
    try:
        # Get latest prediction
        pred_df = spark.table(TABLES["prediction_gold"]).filter(
            F.col("base_hour") <= F.lit(base_time.isoformat())
        ).orderBy(F.desc("base_hour")).limit(1)
        
        # Get latest model registry entry
        model_df = spark.table(TABLES["model_registry_gold"]).filter(
            F.col("status") == "production"
        ).orderBy(F.desc("promoted_at")).limit(1)
        
        # Get latest observation
        obs_df = spark.table(TABLES["openaq_station_silver"]).filter(
            F.col("timestamp") >= F.lit((base_time - timedelta(hours=1)).isoformat())
        ).orderBy(F.desc("timestamp")).limit(1)
        
        # Build dashboard row
        dashboard_df = pred_df.crossJoin(F.broadcast(model_df)).crossJoin(F.broadcast(obs_df)).select(
            F.lit(str(uuid.uuid4())).alias("dashboard_id"),
            F.lit(visualization_run_id).alias("visualization_run_id"),
            F.lit(product_version).alias("product_version"),
            F.lit(schema_version).alias("schema_version"),
            F.col("base_hour").alias("base_hour"),
            F.lit(location_id).alias("location_id"),
            F.lit(location_name).alias("location_name"),
            F.col("timestamp").alias("latest_observed_time"),
            F.col("pm25").alias("pm25_latest_observed"),
            F.col("pm25_6h").alias("pm25_now"),
            F.col("pm25_6h").alias("pm25_6h"),
            F.when(F.col("pm25_6h") < 12, "low")
             .when(F.col("pm25_6h") < 35.4, "medium")
             .when(F.col("pm25_6h") < 55.4, "high")
             .otherwise("very_high").alias("risk_6h"),
            F.col("pm25_12h").alias("pm25_12h"),
            F.when(F.col("pm25_12h") < 12, "low")
             .when(F.col("pm25_12h") < 35.4, "medium")
             .when(F.col("pm25_12h") < 55.4, "high")
             .otherwise("very_high").alias("risk_12h"),
            F.col("pm25_24h").alias("pm25_24h"),
            F.when(F.col("pm25_24h") < 12, "low")
             .when(F.col("pm25_24h") < 35.4, "medium")
             .when(F.col("pm25_24h") < 55.4, "high")
             .otherwise("very_high").alias("risk_24h"),
            F.lit(1).alias("dominant_cluster"),
            F.lit(21.0285).alias("source_lat"),
            F.lit(105.8542).alias("source_lon"),
            F.lit("Hanoi").alias("source_label"),
            F.lit(0.0).alias("path_no2_mean"),
            F.lit(0.0).alias("path_aer_mean"),
            F.lit(0.0).alias("pm25_grad_mag"),
            F.col("model_version").alias("model_version"),
            F.col("model_version").alias("model_version_6h"),
            F.col("model_version").alias("model_version_12h"),
            F.col("model_version").alias("model_version_24h"),
            F.lit("active").alias("model_status"),
            F.col("feature_version").alias("feature_version"),
            F.lit("").alias("feature_schema_hash"),
            F.lit("").alias("prediction_id"),
            F.col("created_at").alias("prediction_created_at"),
            F.current_timestamp().alias("generated_at"),
            F.lit(0).alias("prediction_freshness_minutes"),
            F.lit(0).alias("observation_freshness_minutes"),
            F.year(F.col("base_hour")).alias("year"),
            F.month(F.col("base_hour")).alias("month"),
            F.dayofmonth(F.col("base_hour")).alias("day")
        )
        
        if not dry_run:
            dashboard_df.write.format("iceberg").mode("append").save(TABLES["visualization_forecast_dashboard_gold"])
            print(f"[forecast_dashboard] Wrote dashboard row for {location_id}")
        else:
            print(f"[forecast_dashboard] DRY_RUN: would write dashboard row for {location_id}")
            dashboard_df.show()
            
    except Exception as e:
        print(f"[forecast_dashboard] ERROR: {str(e)}")
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
    build_forecast_dashboard(spark, base_time, dry_run)
    spark.stop()


if __name__ == "__main__":
    main()
