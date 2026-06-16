#!/bin/bash
set -euo pipefail

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"

COMMON_ARGS=(
  --master local[1]
  --deploy-mode client
  --conf spark.sql.shuffle.partitions=4
  --conf spark.default.parallelism=4
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.ais.type=hadoop
  --conf "spark.sql.catalog.ais.warehouse=${ICEBERG_WAREHOUSE}"
  --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}"
  --conf "spark.hadoop.dfs.client.use.datanode.hostname=${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
)

"${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/online_pm25_feature_builder.py \
  --lookback-hours "${ONLINE_FEATURE_LOOKBACK_HOURS:-30}" \
  --dry-run 0

"${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/ml/predict_hanoi_pm25.py \
  --dry-run 0
