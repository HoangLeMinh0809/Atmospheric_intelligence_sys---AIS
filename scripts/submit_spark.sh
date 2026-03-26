#!/bin/bash
# =============================================================================
# Submit Spark Structured Streaming job
# Chạy trên host — exec vào spark-master container
# =============================================================================

echo "=== Tạo thư mục output trên HDFS ==="
docker exec namenode hdfs dfs -mkdir -p /data/stock_prices_daily
docker exec namenode hdfs dfs -mkdir -p /checkpoints/stock_prices_daily
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints

echo ""
echo "=== Submit Spark Streaming Job ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "StockPricesDaily_Streaming" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=1g" \
  /opt/spark-jobs/stock_prices_streaming.py
