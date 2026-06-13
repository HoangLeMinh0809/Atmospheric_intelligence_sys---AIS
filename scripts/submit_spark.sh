#!/bin/bash
# =============================================================================
# submit_spark.sh
# Submit AIS Spark jobs (streaming + batch load)
#
# KAFKA_STARTING_OFFSETS:
#   - "latest"  : Start from newest messages only (default)
#   - "earliest": Catch all historical Kafka messages (recommended for initial runs)
#
# Example:
#   KAFKA_STARTING_OFFSETS=earliest bash scripts/submit_spark.sh sentinel5p
#   bash scripts/submit_spark.sh era5-ingest   # download ERA5 + publish metadata to Kafka
#   bash scripts/submit_spark.sh era5-files    # Spark consumer: Kafka era5-files -> Iceberg
#   bash scripts/submit_spark.sh era5-pressure-arl  # Convert ERA5 pressure-level GRIB -> HYSPLIT ARL
#   bash scripts/submit_spark.sh hysplit-run
#   bash scripts/submit_spark.sh hysplit-parse
#   bash scripts/submit_spark.sh hysplit-cluster
# =============================================================================

set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

PRESET_DETACH="${DETACH:-}"
PRESET_STOP_AFTER_BATCH="${STOP_AFTER_BATCH:-}"
PRESET_PROCESSING_TIME="${PROCESSING_TIME:-}"
PRESET_KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-}"
PRESET_CHECKPOINT_PATH="${CHECKPOINT_PATH:-}"
PRESET_WINDOW_MODE="${WINDOW_MODE:-}"
PRESET_REALTIME_CONTINUOUS="${REALTIME_CONTINUOUS:-}"
PRESET_REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES:-}"
PRESET_REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS:-}"
PRESET_START_DATE="${START_DATE:-}"
PRESET_END_DATE="${END_DATE:-}"
PRESET_FULL_REFRESH="${FULL_REFRESH:-}"
PRESET_HYSPLIT_MAX_RUNS="${HYSPLIT_MAX_RUNS:-}"
PRESET_HYSPLIT_PARALLELISM="${HYSPLIT_PARALLELISM:-}"
PRESET_HYSPLIT_TIMEOUT_SEC="${HYSPLIT_TIMEOUT_SEC:-}"
PRESET_HYSPLIT_SHARD_ID="${HYSPLIT_SHARD_ID:-}"
PRESET_HYSPLIT_SHARD_COUNT="${HYSPLIT_SHARD_COUNT:-}"
PRESET_TRAJ_SPATIAL_BUCKET_DEG="${TRAJ_SPATIAL_BUCKET_DEG:-}"
PRESET_MAX_DISTANCE_DEG="${MAX_DISTANCE_DEG:-}"
PRESET_VIS_MAX_TRAJECTORIES="${VIS_MAX_TRAJECTORIES:-}"
PRESET_VIS_MAX_POINTS_PER_TRAJECTORY="${VIS_MAX_POINTS_PER_TRAJECTORY:-}"
PRESET_VIS_MAX_GEOJSON_FEATURES="${VIS_MAX_GEOJSON_FEATURES:-}"
PRESET_PIPELINE_SOURCES="${PIPELINE_SOURCES:-}"
PRESET_PIPELINE_CONTINUE_ON_ERROR="${PIPELINE_CONTINUE_ON_ERROR:-}"
PRESET_PIPELINE_STEPS="${PIPELINE_STEPS:-}"
PRESET_PIPELINE_LAYERS="${PIPELINE_LAYERS:-}"
PRESET_EXPORT_CACHE="${EXPORT_CACHE:-}"
PRESET_BASE_TIME="${BASE_TIME:-}"
PRESET_DRY_RUN="${DRY_RUN:-}"
PRESET_CASSANDRA_FEATURE_LATEST_ONLY="${CASSANDRA_FEATURE_LATEST_ONLY:-}"
PRESET_ONLINE_FEATURE_LOOKBACK_HOURS="${ONLINE_FEATURE_LOOKBACK_HOURS:-}"
PRESET_FEATURE_SOURCE="${FEATURE_SOURCE:-}"
PRESET_WRITE_CASSANDRA_FORECAST="${WRITE_CASSANDRA_FORECAST:-}"
PRESET_BASE_HOUR="${BASE_HOUR:-}"
PRESET_HDFS_NAMENODE="${HDFS_NAMENODE:-}"
PRESET_HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS:-}"
PRESET_HADOOP_DEFAULT_FS="${HADOOP_DEFAULT_FS:-}"
PRESET_ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-}"
PRESET_HDFS_WEBHDFS_BASE="${HDFS_WEBHDFS_BASE:-}"
PRESET_WEBHDFS_BASE="${WEBHDFS_BASE:-}"

# Load .env file to get credentials and configuration
if [ -f ".env" ]; then
  set +u  # Temporarily disable strict mode for variable substitution
  set -a
  # Support .env files edited on Windows with CRLF line endings.
  source <(tr -d '\r' < .env)
  set +a
  set -u  # Re-enable strict mode
fi

[ -n "$PRESET_DETACH" ] && DETACH="$PRESET_DETACH"
[ -n "$PRESET_STOP_AFTER_BATCH" ] && STOP_AFTER_BATCH="$PRESET_STOP_AFTER_BATCH"
[ -n "$PRESET_PROCESSING_TIME" ] && PROCESSING_TIME="$PRESET_PROCESSING_TIME"
[ -n "$PRESET_KAFKA_STARTING_OFFSETS" ] && KAFKA_STARTING_OFFSETS="$PRESET_KAFKA_STARTING_OFFSETS"
[ -n "$PRESET_CHECKPOINT_PATH" ] && CHECKPOINT_PATH="$PRESET_CHECKPOINT_PATH"
[ -n "$PRESET_WINDOW_MODE" ] && WINDOW_MODE="$PRESET_WINDOW_MODE"
[ -n "$PRESET_REALTIME_CONTINUOUS" ] && REALTIME_CONTINUOUS="$PRESET_REALTIME_CONTINUOUS"
[ -n "$PRESET_REALTIME_LOOKBACK_MINUTES" ] && REALTIME_LOOKBACK_MINUTES="$PRESET_REALTIME_LOOKBACK_MINUTES"
[ -n "$PRESET_REALTIME_POLL_SECONDS" ] && REALTIME_POLL_SECONDS="$PRESET_REALTIME_POLL_SECONDS"
[ -n "$PRESET_START_DATE" ] && START_DATE="$PRESET_START_DATE"
[ -n "$PRESET_END_DATE" ] && END_DATE="$PRESET_END_DATE"
[ -n "$PRESET_FULL_REFRESH" ] && FULL_REFRESH="$PRESET_FULL_REFRESH"
[ -n "$PRESET_HYSPLIT_MAX_RUNS" ] && HYSPLIT_MAX_RUNS="$PRESET_HYSPLIT_MAX_RUNS"
[ -n "$PRESET_HYSPLIT_PARALLELISM" ] && HYSPLIT_PARALLELISM="$PRESET_HYSPLIT_PARALLELISM"
[ -n "$PRESET_HYSPLIT_TIMEOUT_SEC" ] && HYSPLIT_TIMEOUT_SEC="$PRESET_HYSPLIT_TIMEOUT_SEC"
[ -n "$PRESET_HYSPLIT_SHARD_ID" ] && HYSPLIT_SHARD_ID="$PRESET_HYSPLIT_SHARD_ID"
[ -n "$PRESET_HYSPLIT_SHARD_COUNT" ] && HYSPLIT_SHARD_COUNT="$PRESET_HYSPLIT_SHARD_COUNT"
[ -n "$PRESET_TRAJ_SPATIAL_BUCKET_DEG" ] && TRAJ_SPATIAL_BUCKET_DEG="$PRESET_TRAJ_SPATIAL_BUCKET_DEG"
[ -n "$PRESET_MAX_DISTANCE_DEG" ] && MAX_DISTANCE_DEG="$PRESET_MAX_DISTANCE_DEG"
[ -n "$PRESET_VIS_MAX_TRAJECTORIES" ] && VIS_MAX_TRAJECTORIES="$PRESET_VIS_MAX_TRAJECTORIES"
[ -n "$PRESET_VIS_MAX_POINTS_PER_TRAJECTORY" ] && VIS_MAX_POINTS_PER_TRAJECTORY="$PRESET_VIS_MAX_POINTS_PER_TRAJECTORY"
[ -n "$PRESET_VIS_MAX_GEOJSON_FEATURES" ] && VIS_MAX_GEOJSON_FEATURES="$PRESET_VIS_MAX_GEOJSON_FEATURES"
[ -n "$PRESET_PIPELINE_SOURCES" ] && PIPELINE_SOURCES="$PRESET_PIPELINE_SOURCES"
[ -n "$PRESET_PIPELINE_CONTINUE_ON_ERROR" ] && PIPELINE_CONTINUE_ON_ERROR="$PRESET_PIPELINE_CONTINUE_ON_ERROR"
[ -n "$PRESET_PIPELINE_STEPS" ] && PIPELINE_STEPS="$PRESET_PIPELINE_STEPS"
[ -n "$PRESET_PIPELINE_LAYERS" ] && PIPELINE_LAYERS="$PRESET_PIPELINE_LAYERS"
[ -n "$PRESET_EXPORT_CACHE" ] && EXPORT_CACHE="$PRESET_EXPORT_CACHE"
[ -n "$PRESET_BASE_TIME" ] && BASE_TIME="$PRESET_BASE_TIME"
[ -n "$PRESET_DRY_RUN" ] && DRY_RUN="$PRESET_DRY_RUN"
[ -n "$PRESET_CASSANDRA_FEATURE_LATEST_ONLY" ] && CASSANDRA_FEATURE_LATEST_ONLY="$PRESET_CASSANDRA_FEATURE_LATEST_ONLY"
[ -n "$PRESET_ONLINE_FEATURE_LOOKBACK_HOURS" ] && ONLINE_FEATURE_LOOKBACK_HOURS="$PRESET_ONLINE_FEATURE_LOOKBACK_HOURS"
[ -n "$PRESET_FEATURE_SOURCE" ] && FEATURE_SOURCE="$PRESET_FEATURE_SOURCE"
[ -n "$PRESET_WRITE_CASSANDRA_FORECAST" ] && WRITE_CASSANDRA_FORECAST="$PRESET_WRITE_CASSANDRA_FORECAST"
[ -n "$PRESET_BASE_HOUR" ] && BASE_HOUR="$PRESET_BASE_HOUR"
[ -n "$PRESET_HDFS_NAMENODE" ] && HDFS_NAMENODE="$PRESET_HDFS_NAMENODE"
[ -n "$PRESET_HDFS_DEFAULT_FS" ] && HDFS_DEFAULT_FS="$PRESET_HDFS_DEFAULT_FS"
[ -n "$PRESET_HADOOP_DEFAULT_FS" ] && HADOOP_DEFAULT_FS="$PRESET_HADOOP_DEFAULT_FS"
[ -n "$PRESET_ICEBERG_WAREHOUSE" ] && ICEBERG_WAREHOUSE="$PRESET_ICEBERG_WAREHOUSE"
[ -n "$PRESET_HDFS_WEBHDFS_BASE" ] && HDFS_WEBHDFS_BASE="$PRESET_HDFS_WEBHDFS_BASE"
[ -n "$PRESET_WEBHDFS_BASE" ] && WEBHDFS_BASE="$PRESET_WEBHDFS_BASE"

JOB_TYPE="${1:-weather}"
DETACH="${DETACH:-false}"
STOP_AFTER_BATCH="${STOP_AFTER_BATCH:-false}"
PROCESSING_TIME="${PROCESSING_TIME:-}"
KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"
CHECKPOINT_PATH_OVERRIDE="${CHECKPOINT_PATH:-}"
WINDOW_MODE="${WINDOW_MODE:-}"
REALTIME_CONTINUOUS="${REALTIME_CONTINUOUS:-}"
REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES:-}"
REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS:-}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
ERA5_START_DATE="${ERA5_START_DATE:-}"
ERA5_END_DATE="${ERA5_END_DATE:-}"
FULL_REFRESH="${FULL_REFRESH:-0}"
MAIAC_LOCAL_FALLBACK_PATH="${MAIAC_LOCAL_FALLBACK_PATH:-/opt/maiac_data}"
MAIAC_RELAXED_QA="${MAIAC_RELAXED_QA:-0}"
SPARK_JARS_IVY="${SPARK_JARS_IVY:-/root/.ivy2}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-atmospheric_intelligence_sys---ais}"
HYSPLIT_MAX_RUNS="${HYSPLIT_MAX_RUNS:-}"
HYSPLIT_PARALLELISM="${HYSPLIT_PARALLELISM:-}"
HYSPLIT_TIMEOUT_SEC="${HYSPLIT_TIMEOUT_SEC:-}"
HYSPLIT_SHARD_ID="${HYSPLIT_SHARD_ID:-}"
HYSPLIT_SHARD_COUNT="${HYSPLIT_SHARD_COUNT:-}"
TRAJ_SPATIAL_BUCKET_DEG="${TRAJ_SPATIAL_BUCKET_DEG:-}"
MAX_DISTANCE_DEG="${MAX_DISTANCE_DEG:-}"
VIS_MAX_TRAJECTORIES="${VIS_MAX_TRAJECTORIES:-}"
VIS_MAX_POINTS_PER_TRAJECTORY="${VIS_MAX_POINTS_PER_TRAJECTORY:-}"
VIS_MAX_GEOJSON_FEATURES="${VIS_MAX_GEOJSON_FEATURES:-}"
PIPELINE_SOURCES="${PIPELINE_SOURCES:-}"
PIPELINE_CONTINUE_ON_ERROR="${PIPELINE_CONTINUE_ON_ERROR:-}"
PIPELINE_STEPS="${PIPELINE_STEPS:-}"
PIPELINE_LAYERS="${PIPELINE_LAYERS:-}"
EXPORT_CACHE="${EXPORT_CACHE:-}"
BASE_TIME="${BASE_TIME:-}"
DRY_RUN="${DRY_RUN:-0}"
CASSANDRA_FEATURE_LATEST_ONLY="${CASSANDRA_FEATURE_LATEST_ONLY:-0}"
ONLINE_FEATURE_LOOKBACK_HOURS="${ONLINE_FEATURE_LOOKBACK_HOURS:-72}"
ERA5_CONVERT_TIMEOUT_SEC="${ERA5_CONVERT_TIMEOUT_SEC:-}"
HDFS_CMD_TIMEOUT_SEC="${HDFS_CMD_TIMEOUT_SEC:-}"
HDFS_NAMENODE="${HDFS_NAMENODE:-${HDFS_DEFAULT_FS:-${HADOOP_DEFAULT_FS:-hdfs://namenode:9000}}}"
HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS:-$HDFS_NAMENODE}"
HADOOP_DEFAULT_FS="${HADOOP_DEFAULT_FS:-$HDFS_DEFAULT_FS}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-${HDFS_NAMENODE%/}/warehouse/iceberg}"
HDFS_WEBHDFS_BASE="${HDFS_WEBHDFS_BASE:-${WEBHDFS_BASE:-http://namenode:9870/webhdfs/v1}}"
WEBHDFS_BASE="${WEBHDFS_BASE:-$HDFS_WEBHDFS_BASE}"
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"

KAFKA_HADOOP_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-client:3.3.4"
ICEBERG_PACKAGES="${KAFKA_HADOOP_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
CASSANDRA_PACKAGES="${ICEBERG_PACKAGES},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

APP_NAME=""
JOB_FILE=""
JOB_ARGS=()
STREAM_ARGS=()
HDFS_DATA_DIR=""
HDFS_CHECKPOINT_DIR=""
KAFKA_TOPIC=""
ICEBERG_TABLE=""
CHECKPOINT_PATH=""
PACKAGES="${ICEBERG_PACKAGES}"
SPARK_CORES_MAX="${SPARK_CORES_MAX:-}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-}"

normalize_hdfs_uri() {
  local value="$1"
  local base="${HDFS_NAMENODE%/}"
  if [[ "$value" == hdfs://namenode:9000/* ]]; then
    printf '%s%s' "$base" "${value#hdfs://namenode:9000}"
  elif [[ "$value" == hdfs://host.docker.internal:9000/* ]]; then
    printf '%s%s' "$base" "${value#hdfs://host.docker.internal:9000}"
  else
    printf '%s' "$value"
  fi
}

wait_for_hdfs_writable() {
  local timeout_sec="${1:-300}"
  local elapsed=0

  while true; do
    local safemode_output
    safemode_output="$(docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -fs "$HDFS_NAMENODE" -safemode get 2>/dev/null || true)"

    if echo "$safemode_output" | grep -q "Safe mode is OFF"; then
      if docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -ls / >/dev/null 2>&1; then
        echo "[OK] HDFS RPC reachable and safemode is OFF"
        return 0
      fi
    fi

    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[ERROR] HDFS is not writable after ${timeout_sec}s"
      docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -fs "$HDFS_NAMENODE" -safemode get || true
      return 1
    fi

    echo "[WAIT] HDFS not writable yet (${elapsed}s/${timeout_sec}s)"
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

spark_app_registered() {
  local app_name="$1"

  docker exec -i -e APP_NAME="$app_name" spark-master python3 - <<'PY'
import json
import os
import sys
import urllib.request

app_name = os.environ.get("APP_NAME", "")
try:
    raw = urllib.request.urlopen("http://localhost:8080/json", timeout=10).read().decode("utf-8")
    payload = json.loads(raw)
except Exception:
    sys.exit(1)

for app in payload.get("activeapps", []):
  if app.get("name") == app_name and app.get("state") == "RUNNING":
    sys.exit(0)

sys.exit(1)
PY
}

case "$JOB_TYPE" in
  weather-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="WeatherHistory_Ingest"
    INGEST_SERVICE="ingest"
    INGEST_SCRIPT="ingest_weather.py"
    KAFKA_TOPIC="weather_history"
    INGEST_LOOKBACK_DAYS="${WEATHER_BATCH_LOOKBACK_DAYS:-7}"
    KAFKA_TOPIC="weather_history"
    ;;
  openaq-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="OpenAQHourly_Ingest"
    INGEST_SERVICE="openaq-ingest"
    INGEST_SCRIPT="openaq_ingest.py"
    KAFKA_TOPIC="openaq-hourly"
    INGEST_LOOKBACK_DAYS="${OPENAQ_BATCH_LOOKBACK_DAYS:-7}"
    KAFKA_TOPIC="openaq-hourly"
    ;;
  sentinel5p-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="Sentinel5PSummary_Ingest"
    INGEST_SERVICE="sentinel5p-ingest"
    INGEST_SCRIPT="sentinel5p_ingest.py"
    KAFKA_TOPIC="sentinel5p-summary"
    INGEST_LOOKBACK_DAYS="${LOOKBACK_DAYS:-7}"
    KAFKA_TOPIC="sentinel5p-summary"
    SENTINEL5P_LOCAL_METADATA_PATH="/app/data/crawling/outputs/sentinel5p_vietnam_last_3d.json"
    ;;
  maiac-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="MAIACSummary_Ingest"
    INGEST_SERVICE="maiac-ingest"
    INGEST_SCRIPT="maiac_ingest.py"
    KAFKA_TOPIC="maiac-summary"
    INGEST_LOOKBACK_DAYS="${MAIAC_BATCH_LOOKBACK_DAYS:-${LOOKBACK_DAYS:-30}}"
    KAFKA_TOPIC="maiac-summary"
    ;;
  era5-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="ERA5Files_Ingest"
    INGEST_SERVICE="ingest"
    INGEST_SCRIPT="era5_ingest.py"
    KAFKA_TOPIC="era5-files"
    INGEST_LOOKBACK_DAYS="${ERA5_BATCH_LOOKBACK_DAYS:-${LOOKBACK_DAYS:-7}}"
    KAFKA_TOPIC="era5-files"
    ;;
  weather)
    JOB_TYPE_KIND="spark"
    APP_NAME="WeatherHistory_Streaming"
    JOB_FILE="/opt/spark-jobs/weather_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/weather/weather_history_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
    KAFKA_TOPIC="weather_history"
    ICEBERG_TABLE="ais.weather.weather_history_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/weather_history/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-files)
    JOB_TYPE_KIND="spark"
    APP_NAME="ERA5Files_Streaming"
    JOB_FILE="/opt/spark-jobs/era5_files_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/weather/era5_files_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/era5_files"
    KAFKA_TOPIC="era5-files"
    ICEBERG_TABLE="ais.weather.era5_files_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/era5_files/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq)
    JOB_TYPE_KIND="spark"
    APP_NAME="OpenAQHourly_Streaming"
    JOB_FILE="/opt/spark-jobs/openaq_hourly_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/air_quality/openaq_hourly_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/openaq_hourly"
    KAFKA_TOPIC="openaq-hourly"
    ICEBERG_TABLE="ais.air_quality.openaq_hourly_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/openaq_hourly/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p)
    JOB_TYPE_KIND="spark"
    APP_NAME="Sentinel5PSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/sentinel5p_summary_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/sentinel5p_summary_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_summary"
    KAFKA_TOPIC="sentinel5p-summary"
    ICEBERG_TABLE="ais.satellite.sentinel5p_summary_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/sentinel5p_summary/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac)
    JOB_TYPE_KIND="spark"
    APP_NAME="MAIACSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/maiac_summary_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/maiac_summary_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/maiac_summary"
    KAFKA_TOPIC="maiac-summary"
    ICEBERG_TABLE="ais.satellite.maiac_summary_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/maiac_summary/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-openaq-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiOpenAQSilver"
    JOB_FILE="/opt/spark-jobs/hanoi_openaq_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/air_quality/openaq_hanoi_hourly_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_openaq_silver"
    ICEBERG_TABLE="ais.air_quality.openaq_hanoi_hourly_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_openaq_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-weather-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiWeatherSurfaceProxySilver"
    JOB_FILE="/opt/spark-jobs/hanoi_weather_surface_proxy_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/weather/weather_hanoi_surface_proxy_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_weather_surface_proxy_silver"
    ICEBERG_TABLE="ais.weather.weather_hanoi_surface_proxy_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_weather_surface_proxy_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-surface-hanoi-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="ERA5SurfaceHanoiSilver"
    JOB_FILE="/opt/spark-jobs/era5_surface_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/weather/era5_surface_hanoi_hourly_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/era5_surface_hanoi_silver"
    ICEBERG_TABLE="ais.weather.era5_surface_hanoi_hourly_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/era5_surface_hanoi_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-pressure-arl)
    JOB_TYPE_KIND="spark"
    APP_NAME="ERA5PressureLevelsToARL"
    JOB_FILE="/opt/spark-jobs/era5_pressure_levels_to_arl.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/raw/era5/arl/pressure_levels"
    HDFS_CHECKPOINT_DIR="/checkpoints/era5_pressure_arl"
    ICEBERG_TABLE="ais.weather.era5_arl_files_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/era5_pressure_arl/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hysplit-run)
    JOB_TYPE_KIND="spark"
    APP_NAME="HYSPLITTrajectoryRun"
    JOB_FILE="/opt/spark-jobs/hysplit_trajectory_run.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/raw/hysplit/trajectories"
    HDFS_CHECKPOINT_DIR="/checkpoints/hysplit_run"
    ICEBERG_TABLE="ais.trajectory.hysplit_runs_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hysplit_run/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hysplit-parse)
    JOB_TYPE_KIND="spark"
    APP_NAME="HYSPLITTrajectoryParseSilver"
    JOB_FILE="/opt/spark-jobs/hysplit_trajectory_parse_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/trajectory/hysplit_trajectories_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/hysplit_parse"
    ICEBERG_TABLE="ais.trajectory.hysplit_trajectories_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hysplit_parse/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hysplit-cluster)
    JOB_TYPE_KIND="spark"
    APP_NAME="HYSPLITTrajectoryClusterSilver"
    JOB_FILE="/opt/spark-jobs/hysplit_trajectory_cluster_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    if [ -n "${ANCHOR_HOURS:-}" ]; then
      JOB_ARGS+=("--anchor-hours" "$ANCHOR_HOURS")
    fi
    HDFS_DATA_DIR="/warehouse/iceberg/trajectory/hysplit_trajectories_clustered_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/hysplit_cluster"
    ICEBERG_TABLE="ais.trajectory.hysplit_trajectories_clustered_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hysplit_cluster/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p-hanoi-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="Sentinel5PHanoiSilver"
    JOB_FILE="/opt/spark-jobs/sentinel5p_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/sentinel5p_hanoi_daily_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_hanoi_silver"
    ICEBERG_TABLE="ais.satellite.sentinel5p_hanoi_daily_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/sentinel5p_hanoi_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq-gradient)
    JOB_TYPE_KIND="spark"
    APP_NAME="OpenAQSpatialGradientSilver"
    JOB_FILE="/opt/spark-jobs/openaq_spatial_gradient_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/openaq_spatial_gradient_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/openaq_spatial_gradient_silver"
    ICEBERG_TABLE="ais.features.openaq_spatial_gradient_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/openaq_spatial_gradient_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  s5p-grid-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="Sentinel5PGridSilver"
    JOB_FILE="/opt/spark-jobs/sentinel5p_grid_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/sentinel5p_grid_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_grid_silver"
    ICEBERG_TABLE="ais.satellite.sentinel5p_grid_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/sentinel5p_grid_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  traj-path-sampling)
    JOB_TYPE_KIND="spark"
    APP_NAME="TrajectoryPathSamplingSilver"
    JOB_FILE="/opt/spark-jobs/trajectory_path_sampling_silver.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--full-refresh" "$FULL_REFRESH")
    if [ -n "${PATH_WINDOW_START_H:-}" ]; then
      JOB_ARGS+=("--path-window-start-h" "$PATH_WINDOW_START_H")
    fi
    if [ -n "${PATH_WINDOW_END_H:-}" ]; then
      JOB_ARGS+=("--path-window-end-h" "$PATH_WINDOW_END_H")
    fi
    if [ -n "${MAX_DISTANCE_DEG:-}" ]; then
      JOB_ARGS+=("--max-distance-deg" "$MAX_DISTANCE_DEG")
    fi
    HDFS_DATA_DIR="/warehouse/iceberg/features/trajectory_path_satellite_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/trajectory_path_sampling_silver"
    ICEBERG_TABLE="ais.features.trajectory_path_satellite_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/trajectory_path_sampling_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  traj-hourly-features)
    JOB_TYPE_KIND="spark"
    APP_NAME="TrajectoryHourlyFeaturesSilver"
    JOB_FILE="/opt/spark-jobs/trajectory_hourly_features_silver.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/trajectory_hourly_features_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/trajectory_hourly_features_silver"
    ICEBERG_TABLE="ais.features.trajectory_hourly_features_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/trajectory_hourly_features_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac-hanoi-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="MAIACHanoiSilver"
    JOB_FILE="/opt/spark-jobs/maiac_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--local-fallback-path" "$MAIAC_LOCAL_FALLBACK_PATH" "--relaxed-qa" "$MAIAC_RELAXED_QA")
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/maiac_hanoi_daily_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/maiac_hanoi_daily_silver"
    ICEBERG_TABLE="ais.satellite.maiac_hanoi_daily_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/maiac_hanoi_daily_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-master-features-gold)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiPM25MasterFeaturesGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_master_features_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/hanoi_pm25_master_hourly_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_pm25_master_features_gold"
    ICEBERG_TABLE="ais.features.hanoi_pm25_master_hourly_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_pm25_master_features_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-serving-features-gold)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiPM25ServingFeaturesGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_serving_features_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/hanoi_pm25_serving_features_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_pm25_serving_features_gold"
    ICEBERG_TABLE="ais.features.hanoi_pm25_serving_features_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_pm25_serving_features_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-training-dataset-gold)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiPM25TrainingDatasetGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_training_dataset_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/hanoi_pm25_training_dataset_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_pm25_training_dataset_gold"
    ICEBERG_TABLE="ais.features.hanoi_pm25_training_dataset_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_pm25_training_dataset_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-train-baseline)
    JOB_TYPE_KIND="spark"
    APP_NAME="TrainHanoiPM25Baseline"
    JOB_FILE="/opt/ml/train_hanoi_pm25.py"
    JOB_ARGS=("--dataset-version" "${DATASET_VERSION:-hanoi_pm25_v1}" "--feature-set-name" "${FEATURE_SET_NAME:-hanoi_pm25_core_v1}" "--model-type" "${MODEL_TYPE:-lightgbm}" "--output-dir" "${MODEL_OUTPUT_DIR:-/opt/models/hanoi_pm25}")
    HDFS_DATA_DIR="/warehouse/iceberg/models/hanoi_pm25_model_runs_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_train_baseline"
    ICEBERG_TABLE="ais.models.hanoi_pm25_model_runs_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_train_baseline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  pm25-features-cassandra)
    JOB_TYPE_KIND="spark"
    APP_NAME="PM25ServingFeaturesToCassandra"
    JOB_FILE="/opt/spark-jobs/pm25_serving_features_to_cassandra.py"
    JOB_ARGS=("--latest-only" "${CASSANDRA_FEATURE_LATEST_ONLY:-0}" "--dry-run" "${DRY_RUN:-0}")
    HDFS_DATA_DIR="/data/pm25_features_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/pm25_features_cassandra"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/pm25_features_cassandra/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  online-pm25-features)
    JOB_TYPE_KIND="spark"
    APP_NAME="OnlinePM25FeatureBuilder"
    JOB_FILE="/opt/spark-jobs/online_pm25_feature_builder.py"
    JOB_ARGS=("--base-time" "${BASE_TIME:-${BASE_HOUR:-}}" "--lookback-hours" "${ONLINE_FEATURE_LOOKBACK_HOURS:-72}" "--dry-run" "${DRY_RUN:-0}")
    HDFS_DATA_DIR="/data/online_pm25_features"
    HDFS_CHECKPOINT_DIR="/checkpoints/online_pm25_features"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/online_pm25_features/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-weather)
    JOB_TYPE_KIND="spark"
    APP_NAME="IcebergToCassandra_Weather"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("weather")
    HDFS_DATA_DIR="/data/iceberg_to_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-openaq)
    JOB_TYPE_KIND="spark"
    APP_NAME="IcebergToCassandra_OpenAQ"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("openaq")
    HDFS_DATA_DIR="/data/iceberg_to_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  ensure-iceberg)
    JOB_TYPE_KIND="spark"
    APP_NAME="AIS_EnsureIcebergTables"
    JOB_FILE="/opt/spark-jobs/ensure_iceberg_tables.py"
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/ensure_iceberg/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maintenance-iceberg)
    JOB_TYPE_KIND="spark"
    APP_NAME="AIS_IcebergMaintenance"
    JOB_FILE="/opt/spark-jobs/iceberg_maintenance.py"
    JOB_ARGS=("--retention-hours" "${RETENTION_HOURS:-168}")
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/iceberg_maintenance/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  reconcile-serving)
    JOB_TYPE_KIND="spark"
    APP_NAME="AIS_ReconcileServing"
    JOB_FILE="/opt/spark-jobs/reconcile_iceberg_cassandra.py"
    JOB_ARGS=("--lookback-hours" "${RECONCILE_LOOKBACK_HOURS:-24}" "--tolerance" "${RECONCILE_TOLERANCE:-0.95}")
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/reconcile_serving/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  bronze-pipeline)
    JOB_TYPE_KIND="spark"
    APP_NAME="AISBronzeIngestToIcebergPipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/bronze_ingest_to_iceberg_pipeline.py"
    JOB_ARGS=("--sources" "${PIPELINE_SOURCES:-openaq,weather,sentinel5p,maiac,era5-files}" "--continue-on-error" "${PIPELINE_CONTINUE_ON_ERROR:-false}")
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/bronze_pipeline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  pm25-feature-pipeline)
    JOB_TYPE_KIND="spark"
    APP_NAME="AISPM25FeaturePipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/pm25_feature_pipeline.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--full-refresh" "$FULL_REFRESH" "--steps" "${PIPELINE_STEPS:-}")
    HDFS_DATA_DIR="/warehouse/iceberg/features"
    HDFS_CHECKPOINT_DIR="/checkpoints/pm25_feature_pipeline"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/pm25_feature_pipeline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  trajectory-post-pipeline)
    JOB_TYPE_KIND="spark"
    APP_NAME="AISTrajectoryPostPipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/trajectory_post_pipeline.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--direction" "${DIRECTION:-both}" "--full-refresh" "$FULL_REFRESH" "--spatial-bucket-deg" "${TRAJ_SPATIAL_BUCKET_DEG:-0.25}" "--max-distance-deg" "${MAX_DISTANCE_DEG:-0.25}")
    HDFS_DATA_DIR="/warehouse/iceberg/trajectory"
    HDFS_CHECKPOINT_DIR="/checkpoints/trajectory_post_pipeline"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/trajectory_post_pipeline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  visualization-pipeline)
    JOB_TYPE_KIND="spark"
    APP_NAME="AISVisualizationPipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/visualization_pipeline.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--asof-time" "$BASE_TIME" "--layers" "${PIPELINE_LAYERS:-heatmap,backward_trajectories,forward_plume,source_attribution,stations,forecast,timeseries}" "--export-cache" "${EXPORT_CACHE:-true}" "--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/visualization"
    HDFS_CHECKPOINT_DIR="/checkpoints/visualization_pipeline"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/visualization_pipeline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  *)
    echo "Usage: $0 [weather|openaq|sentinel5p|maiac|era5-files|weather-ingest|openaq-ingest|sentinel5p-ingest|maiac-ingest|era5-ingest|hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|traj-path-sampling|traj-hourly-features|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-serving-features-gold|hanoi-train-baseline|pm25-features-cassandra|online-pm25-features|cassandra-weather|cassandra-openaq|ensure-iceberg|maintenance-iceberg|reconcile-serving|bronze-pipeline|pm25-feature-pipeline|trajectory-post-pipeline|visualization-pipeline]"
    exit 1
    ;;
esac

if [ -n "$CHECKPOINT_PATH_OVERRIDE" ]; then
  CHECKPOINT_PATH="$CHECKPOINT_PATH_OVERRIDE"
fi
CHECKPOINT_PATH="$(normalize_hdfs_uri "$CHECKPOINT_PATH")"

case "$JOB_TYPE" in
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|traj-path-sampling|traj-hourly-features|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-serving-features-gold|pm25-features-cassandra)
    if [ -n "$START_DATE" ]; then
      JOB_ARGS+=("--start-date" "$START_DATE")
    fi
    if [ -n "$END_DATE" ]; then
      JOB_ARGS+=("--end-date" "$END_DATE")
    fi
    ;;
esac

case "$JOB_TYPE" in
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|openaq-gradient|hanoi-master-features-gold|hanoi-serving-features-gold|pm25-feature-pipeline)
    if [ -n "${BASE_TIME:-}" ]; then
      JOB_ARGS+=("--asof-time" "$BASE_TIME")
    fi
    ;;
  visualization-pipeline)
    if [ -n "${BASE_TIME:-}" ]; then
      JOB_ARGS+=("--base-time" "$BASE_TIME")
    fi
    ;;
esac

case "$JOB_TYPE" in
  hysplit-run)
    if [ -n "${DIRECTION:-}" ]; then
      JOB_ARGS+=("--direction" "$DIRECTION")
    fi
    if [ -n "$HYSPLIT_TIMEOUT_SEC" ]; then
      JOB_ARGS+=("--timeout-sec" "$HYSPLIT_TIMEOUT_SEC")
    fi
    if [ -n "$HYSPLIT_MAX_RUNS" ]; then
      JOB_ARGS+=("--max-runs" "$HYSPLIT_MAX_RUNS")
    fi
    if [ -n "$HYSPLIT_PARALLELISM" ]; then
      JOB_ARGS+=("--parallelism" "$HYSPLIT_PARALLELISM")
    fi
    if [ -n "$HYSPLIT_SHARD_ID" ]; then
      JOB_ARGS+=("--shard-id" "$HYSPLIT_SHARD_ID")
    fi
    if [ -n "$HYSPLIT_SHARD_COUNT" ]; then
      JOB_ARGS+=("--shard-count" "$HYSPLIT_SHARD_COUNT")
    fi
    ;;
  traj-path-sampling)
    if [ -n "$TRAJ_SPATIAL_BUCKET_DEG" ]; then
      JOB_ARGS+=("--spatial-bucket-deg" "$TRAJ_SPATIAL_BUCKET_DEG")
    fi
    ;;
esac

if [ "${JOB_TYPE_KIND:-spark}" = "ingest" ]; then
  echo "=== Submit Ingest Job: $APP_NAME ==="
  INGEST_WINDOW_MODE="${WINDOW_MODE:-batch}"
  INGEST_REALTIME_CONTINUOUS="${REALTIME_CONTINUOUS:-false}"
  INGEST_REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES:-180}"
  INGEST_REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS:-300}"
  docker compose -p "$COMPOSE_PROJECT_NAME" run --rm --no-deps \
    -e WINDOW_MODE="$INGEST_WINDOW_MODE" \
    -e BATCH_LOOKBACK_DAYS="$INGEST_LOOKBACK_DAYS" \
    -e LOOKBACK_DAYS="$INGEST_LOOKBACK_DAYS" \
    -e REALTIME_CONTINUOUS="$INGEST_REALTIME_CONTINUOUS" \
    -e REALTIME_LOOKBACK_MINUTES="$INGEST_REALTIME_LOOKBACK_MINUTES" \
    -e REALTIME_POLL_SECONDS="$INGEST_REALTIME_POLL_SECONDS" \
    -e WINDOW_START_UTC="${WINDOW_START_UTC:-}" \
    -e WINDOW_END_UTC="${WINDOW_END_UTC:-}" \
    -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" \
    -e KAFKA_TOPIC="$KAFKA_TOPIC" \
    -e KAFKA_CONNECT_MAX_RETRIES=36 \
    -e KAFKA_CONNECT_RETRY_DELAY=5 \
    -e HDFS_NAMENODE="$HDFS_NAMENODE" \
    -e HDFS_DEFAULT_FS="$HDFS_DEFAULT_FS" \
    -e HADOOP_DEFAULT_FS="$HADOOP_DEFAULT_FS" \
    -e HDFS_WEBHDFS_BASE="$HDFS_WEBHDFS_BASE" \
    -e WEBHDFS_BASE="$WEBHDFS_BASE" \
    -e CDS_URL="${CDS_URL:-}" \
    -e CDS_KEY="${CDS_KEY:-}" \
    -e ERA5_START_DATE="${ERA5_START_DATE:-}" \
    -e ERA5_END_DATE="${ERA5_END_DATE:-}" \
    -e ERA5_DATASET_TYPE="${ERA5_DATASET_TYPE:-surface}" \
    -e ERA5_OUTPUT_BASE_PATH="${ERA5_OUTPUT_BASE_PATH:-}" \
    -e ERA5_SKIP_EXISTING="${ERA5_SKIP_EXISTING:-true}" \
    -e SENTINEL5P_LOCAL_METADATA_PATH="${SENTINEL5P_LOCAL_METADATA_PATH:-}" \
    "$INGEST_SERVICE" \
    python3 "$INGEST_SCRIPT"
  exit 0
fi

case "$JOB_TYPE" in
  weather|openaq|sentinel5p|maiac|era5-files)
    # Keep each streaming app lightweight so multiple consumers can run on a small local cluster.
    SPARK_CORES_MAX="${SPARK_CORES_MAX:-1}"
    SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
    if [ "$STOP_AFTER_BATCH" = "true" ]; then
      STREAM_ARGS+=("--stop-after-batch" "1")
    fi
    if [ -n "$PROCESSING_TIME" ]; then
      STREAM_ARGS+=("--processing-time" "$PROCESSING_TIME")
    fi
    ;;
esac

echo "=== Create HDFS output paths ==="
wait_for_hdfs_writable 300
docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -mkdir -p "$HDFS_DATA_DIR"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -mkdir -p "$HDFS_CHECKPOINT_DIR"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -mkdir -p /warehouse/iceberg
docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -chmod 777 "$HDFS_DATA_DIR"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -chmod 777 "$HDFS_CHECKPOINT_DIR"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_NAMENODE" -chmod 777 /warehouse/iceberg

echo
echo "=== Submit Spark Job: $APP_NAME ==="

if [ "$DETACH" = "true" ]; then
  if spark_app_registered "$APP_NAME"; then
    echo "[WARN] Spark app already active: ${APP_NAME}; skip duplicate submit"
    exit 0
  fi
fi

docker exec spark-master sh -lc "mkdir -p '$SPARK_JARS_IVY' && find '$SPARK_JARS_IVY' -type f -name '*.part' -delete" >/dev/null 2>&1 || true

DOCKER_EXEC_ARGS=()
if [ "$DETACH" = "true" ]; then
  DOCKER_EXEC_ARGS+=("-d")
fi
DOCKER_EXEC_ARGS+=("-e" "KAFKA_STARTING_OFFSETS=${KAFKA_STARTING_OFFSETS}")
DOCKER_EXEC_ARGS+=("-e" "KAFKA_TOPIC=${KAFKA_TOPIC:-}")
DOCKER_EXEC_ARGS+=("-e" "ICEBERG_TABLE=${ICEBERG_TABLE:-}")
DOCKER_EXEC_ARGS+=("-e" "CHECKPOINT_PATH=${CHECKPOINT_PATH:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_QA_THRESHOLD=${S5P_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_NO2_QA_THRESHOLD=${S5P_NO2_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_CO_QA_THRESHOLD=${S5P_CO_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_SO2_QA_THRESHOLD=${S5P_SO2_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_O3_QA_THRESHOLD=${S5P_O3_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_AER_AI_QA_THRESHOLD=${S5P_AER_AI_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "ERA5_ARL_OUTPUT_BASE_PATH=${ERA5_ARL_OUTPUT_BASE_PATH:-}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_ERA5_2ARL_BIN=${HYSPLIT_ERA5_2ARL_BIN:-}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_ERA5_2ARL_TEMPLATE=${HYSPLIT_ERA5_2ARL_TEMPLATE:-}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_BIN=${HYSPLIT_BIN:-}")
DOCKER_EXEC_ARGS+=("-e" "PM25_TRIGGER_THRESHOLD=${PM25_TRIGGER_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_BIN=${HYSPLIT_BIN:-/opt/hysplit/exec/hyts_std}")
DOCKER_EXEC_ARGS+=("-e" "HDFS_NAMENODE=${HDFS_NAMENODE}")
DOCKER_EXEC_ARGS+=("-e" "HDFS_DEFAULT_FS=${HDFS_DEFAULT_FS}")
DOCKER_EXEC_ARGS+=("-e" "HADOOP_DEFAULT_FS=${HADOOP_DEFAULT_FS}")
DOCKER_EXEC_ARGS+=("-e" "ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_OUTPUT_BASE_PATH=${HYSPLIT_OUTPUT_BASE_PATH:-${HDFS_NAMENODE%/}/raw/hysplit/trajectories}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_MAX_RUNS=${HYSPLIT_MAX_RUNS}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_PARALLELISM=${HYSPLIT_PARALLELISM}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_TIMEOUT_SEC=${HYSPLIT_TIMEOUT_SEC}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_SHARD_ID=${HYSPLIT_SHARD_ID}")
DOCKER_EXEC_ARGS+=("-e" "HYSPLIT_SHARD_COUNT=${HYSPLIT_SHARD_COUNT}")
DOCKER_EXEC_ARGS+=("-e" "TRAJ_SPATIAL_BUCKET_DEG=${TRAJ_SPATIAL_BUCKET_DEG}")
DOCKER_EXEC_ARGS+=("-e" "MAX_DISTANCE_DEG=${MAX_DISTANCE_DEG}")
DOCKER_EXEC_ARGS+=("-e" "VIS_MAX_TRAJECTORIES=${VIS_MAX_TRAJECTORIES}")
DOCKER_EXEC_ARGS+=("-e" "VIS_MAX_POINTS_PER_TRAJECTORY=${VIS_MAX_POINTS_PER_TRAJECTORY}")
DOCKER_EXEC_ARGS+=("-e" "VIS_MAX_GEOJSON_FEATURES=${VIS_MAX_GEOJSON_FEATURES}")
DOCKER_EXEC_ARGS+=("-e" "PIPELINE_SOURCES=${PIPELINE_SOURCES}")
DOCKER_EXEC_ARGS+=("-e" "PIPELINE_CONTINUE_ON_ERROR=${PIPELINE_CONTINUE_ON_ERROR}")
DOCKER_EXEC_ARGS+=("-e" "PIPELINE_STEPS=${PIPELINE_STEPS}")
DOCKER_EXEC_ARGS+=("-e" "PIPELINE_LAYERS=${PIPELINE_LAYERS}")
DOCKER_EXEC_ARGS+=("-e" "EXPORT_CACHE=${EXPORT_CACHE}")
DOCKER_EXEC_ARGS+=("-e" "BASE_TIME=${BASE_TIME}")
DOCKER_EXEC_ARGS+=("-e" "ERA5_CONVERT_TIMEOUT_SEC=${ERA5_CONVERT_TIMEOUT_SEC}")
DOCKER_EXEC_ARGS+=("-e" "HDFS_CMD_TIMEOUT_SEC=${HDFS_CMD_TIMEOUT_SEC}")
DOCKER_EXEC_ARGS+=("-e" "CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra}")
DOCKER_EXEC_ARGS+=("-e" "CASSANDRA_PORT=${CASSANDRA_PORT:-9042}")
DOCKER_EXEC_ARGS+=("-e" "CASSANDRA_KEYSPACE=${CASSANDRA_KEYSPACE:-ais_serving}")
DOCKER_EXEC_ARGS+=("-e" "CASSANDRA_FEATURE_TABLE=${CASSANDRA_FEATURE_TABLE:-pm25_feature_state_by_location_hour}")
DOCKER_EXEC_ARGS+=("-e" "CASSANDRA_FORECAST_TABLE=${CASSANDRA_FORECAST_TABLE:-pm25_forecast_latest_by_location}")
DOCKER_EXEC_ARGS+=("-e" "FEATURE_SOURCE=${FEATURE_SOURCE:-iceberg}")
DOCKER_EXEC_ARGS+=("-e" "WRITE_CASSANDRA_FORECAST=${WRITE_CASSANDRA_FORECAST:-0}")
DOCKER_EXEC_ARGS+=("-e" "BASE_HOUR=${BASE_HOUR:-}")
DOCKER_EXEC_ARGS+=("-e" "ONLINE_FEATURE_LOOKBACK_HOURS=${ONLINE_FEATURE_LOOKBACK_HOURS}")
if [ -n "${DIRECTION:-}" ]; then
  DOCKER_EXEC_ARGS+=("-e" "DIRECTION=${DIRECTION}")
fi
if [ -n "${ANCHOR_HOURS:-}" ]; then
  DOCKER_EXEC_ARGS+=("-e" "ANCHOR_HOURS=${ANCHOR_HOURS}")
fi

SPARK_EXTRA_CONF=()
if [ -n "$SPARK_CORES_MAX" ]; then
  SPARK_EXTRA_CONF+=(--conf "spark.cores.max=${SPARK_CORES_MAX}")
fi
if [ -n "$SPARK_EXECUTOR_CORES" ]; then
  SPARK_EXTRA_CONF+=(--conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}")
fi
SPARK_EXTRA_CONF+=(--conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-16}")
SPARK_EXTRA_CONF+=(--conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM:-16}")

docker exec "${DOCKER_EXEC_ARGS[@]}" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "$APP_NAME" \
  --conf "spark.jars.ivy=${SPARK_JARS_IVY}" \
  --repositories "https://repo.maven.apache.org/maven2,https://repo1.maven.org/maven2,https://repos.spark-packages.org" \
  --packages "$PACKAGES" \
  --conf "spark.sql.streaming.checkpointLocation=${CHECKPOINT_PATH}" \
  --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}" \
  --conf "spark.yarn.maxAppAttempts=1" \
  "${SPARK_EXTRA_CONF[@]}" \
  "$JOB_FILE" \
  "${JOB_ARGS[@]}" \
  "${STREAM_ARGS[@]}"

if [ "$DETACH" = "true" ]; then
  echo "Submitted in detached mode: $APP_NAME"
fi
