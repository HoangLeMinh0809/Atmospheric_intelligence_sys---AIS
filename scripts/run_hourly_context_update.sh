#!/bin/bash
set -euo pipefail

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
BASE_DATE="$(date -u +%F)"
START_DATE="${ERA5_HOURLY_START_DATE:-${START_DATE:-$BASE_DATE}}"
END_DATE="${ERA5_HOURLY_END_DATE:-${END_DATE:-$BASE_DATE}}"
FULL_REFRESH="${FULL_REFRESH:-0}"
KAFKA_TOPIC="${KAFKA_TOPIC:-era5-files}"
HYSPLIT_DIRECTION="${HYSPLIT_DIRECTION:-backward}"
HOURLY_CONTEXT_MODE="${HOURLY_CONTEXT_MODE:-static}"
HDFS_NAMENODE="${HDFS_NAMENODE:-${HDFS_DEFAULT_FS:-hdfs://namenode:9000}}"
STATIC_CONTEXT_DATE="${HOURLY_CONTEXT_STATIC_DATE:-${HISTORICAL_BACKFILL_END_DATE:-${REALTIME_SIMULATION_DATE:-$END_DATE}}}"
STATIC_CONTEXT_HOUR="${HOURLY_CONTEXT_STATIC_HOUR:-${STATIC_CONTEXT_DATE}T23:00:00Z}"
STATIC_CONTEXT_PATH="${HOURLY_CONTEXT_STATIC_PATH:-/runtime/hourly_context/latest.json}"

if [ "$HOURLY_CONTEXT_MODE" = "static" ]; then
  echo "ais_hourly_context_update mode=static context_hour=${STATIC_CONTEXT_HOUR} path=${STATIC_CONTEXT_PATH}"
  tmp_file="$(mktemp)"
  cat > "$tmp_file" <<JSON
{"mode":"static","context_hour":"${STATIC_CONTEXT_HOUR}","source":"historical_cache","status":"ready"}
JSON
  if command -v hdfs >/dev/null 2>&1; then
    hdfs dfs -fs "$HDFS_NAMENODE" -mkdir -p "$(dirname "$STATIC_CONTEXT_PATH")" || true
    hdfs dfs -fs "$HDFS_NAMENODE" -put -f "$tmp_file" "$STATIC_CONTEXT_PATH" || true
  fi
  rm -f "$tmp_file"
  echo "ais_hourly_context_update status=success mode=static context_hour=${STATIC_CONTEXT_HOUR}"
  exit 0
fi

if [ "$HOURLY_CONTEXT_MODE" != "refresh" ]; then
  echo "ais_hourly_context_update status=error invalid_mode=${HOURLY_CONTEXT_MODE} expected=static_or_refresh" >&2
  exit 2
fi

COMMON_ARGS=(
  --master local[1]
  --deploy-mode client
  --conf spark.sql.shuffle.partitions="${SPARK_SQL_SHUFFLE_PARTITIONS:-4}"
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM:-4}"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.ais.type=hadoop
  --conf "spark.sql.catalog.ais.warehouse=${ICEBERG_WAREHOUSE}"
  --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}"
  --conf "spark.hadoop.dfs.client.use.datanode.hostname=${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
)

echo "ais_hourly_context_update start=${START_DATE} end=${END_DATE} topic=${KAFKA_TOPIC}"

python /opt/ais/ingest/era5_ingest.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --dataset-type surface \
  --kafka-bootstrap "${KAFKA_BOOTSTRAP_SERVERS}" \
  --topic "$KAFKA_TOPIC"

python /opt/ais/ingest/era5_ingest.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --dataset-type pressure_levels \
  --kafka-bootstrap "${KAFKA_BOOTSTRAP_SERVERS}" \
  --topic "$KAFKA_TOPIC"

STOP_AFTER_BATCH=true KAFKA_STARTING_OFFSETS=earliest KAFKA_TOPIC="$KAFKA_TOPIC" \
  "${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" /opt/spark-jobs/era5_files_streaming.py

"${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/era5_surface_hanoi_silver.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

"${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/era5_pressure_levels_to_arl.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

DIRECTION="$HYSPLIT_DIRECTION" "${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/hysplit_trajectory_run.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

DIRECTION="$HYSPLIT_DIRECTION" "${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/hysplit_trajectory_parse_silver.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

DIRECTION="$HYSPLIT_DIRECTION" "${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/hysplit_trajectory_cluster_silver.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

"${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/trajectory_path_sampling_silver.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

"${SPARK_SUBMIT}" "${COMMON_ARGS[@]}" \
  /opt/spark-jobs/trajectory_hourly_features_silver.py \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --full-refresh "$FULL_REFRESH"

echo "ais_hourly_context_update status=success start=${START_DATE} end=${END_DATE}"
