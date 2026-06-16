#!/bin/bash
# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
# =============================================================================
# submit_spark_k8s.sh
# Submit AIS Spark jobs through Spark-on-Kubernetes.
#
# This script creates a short-lived Kubernetes Job that runs spark-submit inside
# the Spark runtime image. spark-submit then creates the Spark driver and
# executor pods in cluster deploy mode.
# =============================================================================

set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

_AIS_ENV_KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS-}"
_AIS_ENV_STOP_AFTER_BATCH="${STOP_AFTER_BATCH-}"
_AIS_ENV_PROCESSING_TIME="${PROCESSING_TIME-}"
_AIS_ENV_CHECKPOINT_PATH="${CHECKPOINT_PATH-}"
_AIS_ENV_START_DATE="${START_DATE-}"
_AIS_ENV_END_DATE="${END_DATE-}"
_AIS_ENV_FULL_REFRESH="${FULL_REFRESH-}"
_AIS_ENV_HYSPLIT_MAX_RUNS="${HYSPLIT_MAX_RUNS-}"
_AIS_ENV_HYSPLIT_PARALLELISM="${HYSPLIT_PARALLELISM-}"
_AIS_ENV_HYSPLIT_TIMEOUT_SEC="${HYSPLIT_TIMEOUT_SEC-}"
_AIS_ENV_HYSPLIT_SHARD_ID="${HYSPLIT_SHARD_ID-}"
_AIS_ENV_HYSPLIT_SHARD_COUNT="${HYSPLIT_SHARD_COUNT-}"
_AIS_ENV_TRAJ_SPATIAL_BUCKET_DEG="${TRAJ_SPATIAL_BUCKET_DEG-}"
_AIS_ENV_MAX_DISTANCE_DEG="${MAX_DISTANCE_DEG-}"
_AIS_ENV_VIS_MAX_TRAJECTORIES="${VIS_MAX_TRAJECTORIES-}"
_AIS_ENV_VIS_MAX_POINTS_PER_TRAJECTORY="${VIS_MAX_POINTS_PER_TRAJECTORY-}"
_AIS_ENV_VIS_MAX_GEOJSON_FEATURES="${VIS_MAX_GEOJSON_FEATURES-}"
_AIS_ENV_PIPELINE_SOURCES="${PIPELINE_SOURCES-}"
_AIS_ENV_PIPELINE_CONTINUE_ON_ERROR="${PIPELINE_CONTINUE_ON_ERROR-}"
_AIS_ENV_PIPELINE_STEPS="${PIPELINE_STEPS-}"
_AIS_ENV_PIPELINE_LAYERS="${PIPELINE_LAYERS-}"
_AIS_ENV_EXPORT_CACHE="${EXPORT_CACHE-}"
_AIS_ENV_BRONZE_CHECKPOINT_RUN_ID="${BRONZE_CHECKPOINT_RUN_ID-}"
_AIS_ENV_BASE_TIME="${BASE_TIME-}"
_AIS_ENV_ERA5_CONVERT_TIMEOUT_SEC="${ERA5_CONVERT_TIMEOUT_SEC-}"
_AIS_ENV_HDFS_CMD_TIMEOUT_SEC="${HDFS_CMD_TIMEOUT_SEC-}"
_AIS_ENV_HDFS_CLIENT_USE_DATANODE_HOSTNAME="${HDFS_CLIENT_USE_DATANODE_HOSTNAME-}"
_AIS_ENV_HDFS_NAMENODE="${HDFS_NAMENODE-}"
_AIS_ENV_HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS-}"
_AIS_ENV_HADOOP_DEFAULT_FS="${HADOOP_DEFAULT_FS-}"
_AIS_ENV_DRY_RUN="${DRY_RUN-}"
_AIS_ENV_CASSANDRA_FEATURE_LATEST_ONLY="${CASSANDRA_FEATURE_LATEST_ONLY-}"
_AIS_ENV_ONLINE_FEATURE_LOOKBACK_HOURS="${ONLINE_FEATURE_LOOKBACK_HOURS-}"
_AIS_ENV_ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK="${ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK-}"
_AIS_ENV_FEATURE_SOURCE="${FEATURE_SOURCE-}"
_AIS_ENV_WRITE_CASSANDRA_FORECAST="${WRITE_CASSANDRA_FORECAST-}"
_AIS_ENV_BASE_HOUR="${BASE_HOUR-}"

if [ -f ".env" ]; then
  set +u
  set -a
  source <(tr -d '\r' < .env)
  set +a
  set -u
fi

[ -n "$_AIS_ENV_KAFKA_STARTING_OFFSETS" ] && KAFKA_STARTING_OFFSETS="$_AIS_ENV_KAFKA_STARTING_OFFSETS"
[ -n "$_AIS_ENV_STOP_AFTER_BATCH" ] && STOP_AFTER_BATCH="$_AIS_ENV_STOP_AFTER_BATCH"
[ -n "$_AIS_ENV_PROCESSING_TIME" ] && PROCESSING_TIME="$_AIS_ENV_PROCESSING_TIME"
[ -n "$_AIS_ENV_CHECKPOINT_PATH" ] && CHECKPOINT_PATH="$_AIS_ENV_CHECKPOINT_PATH"
[ -n "$_AIS_ENV_START_DATE" ] && START_DATE="$_AIS_ENV_START_DATE"
[ -n "$_AIS_ENV_END_DATE" ] && END_DATE="$_AIS_ENV_END_DATE"
[ -n "$_AIS_ENV_FULL_REFRESH" ] && FULL_REFRESH="$_AIS_ENV_FULL_REFRESH"
[ -n "$_AIS_ENV_HYSPLIT_MAX_RUNS" ] && HYSPLIT_MAX_RUNS="$_AIS_ENV_HYSPLIT_MAX_RUNS"
[ -n "$_AIS_ENV_HYSPLIT_PARALLELISM" ] && HYSPLIT_PARALLELISM="$_AIS_ENV_HYSPLIT_PARALLELISM"
[ -n "$_AIS_ENV_HYSPLIT_TIMEOUT_SEC" ] && HYSPLIT_TIMEOUT_SEC="$_AIS_ENV_HYSPLIT_TIMEOUT_SEC"
[ -n "$_AIS_ENV_HYSPLIT_SHARD_ID" ] && HYSPLIT_SHARD_ID="$_AIS_ENV_HYSPLIT_SHARD_ID"
[ -n "$_AIS_ENV_HYSPLIT_SHARD_COUNT" ] && HYSPLIT_SHARD_COUNT="$_AIS_ENV_HYSPLIT_SHARD_COUNT"
[ -n "$_AIS_ENV_TRAJ_SPATIAL_BUCKET_DEG" ] && TRAJ_SPATIAL_BUCKET_DEG="$_AIS_ENV_TRAJ_SPATIAL_BUCKET_DEG"
[ -n "$_AIS_ENV_MAX_DISTANCE_DEG" ] && MAX_DISTANCE_DEG="$_AIS_ENV_MAX_DISTANCE_DEG"
[ -n "$_AIS_ENV_VIS_MAX_TRAJECTORIES" ] && VIS_MAX_TRAJECTORIES="$_AIS_ENV_VIS_MAX_TRAJECTORIES"
[ -n "$_AIS_ENV_VIS_MAX_POINTS_PER_TRAJECTORY" ] && VIS_MAX_POINTS_PER_TRAJECTORY="$_AIS_ENV_VIS_MAX_POINTS_PER_TRAJECTORY"
[ -n "$_AIS_ENV_VIS_MAX_GEOJSON_FEATURES" ] && VIS_MAX_GEOJSON_FEATURES="$_AIS_ENV_VIS_MAX_GEOJSON_FEATURES"
[ -n "$_AIS_ENV_PIPELINE_SOURCES" ] && PIPELINE_SOURCES="$_AIS_ENV_PIPELINE_SOURCES"
[ -n "$_AIS_ENV_PIPELINE_CONTINUE_ON_ERROR" ] && PIPELINE_CONTINUE_ON_ERROR="$_AIS_ENV_PIPELINE_CONTINUE_ON_ERROR"
[ -n "$_AIS_ENV_PIPELINE_STEPS" ] && PIPELINE_STEPS="$_AIS_ENV_PIPELINE_STEPS"
[ -n "$_AIS_ENV_PIPELINE_LAYERS" ] && PIPELINE_LAYERS="$_AIS_ENV_PIPELINE_LAYERS"
[ -n "$_AIS_ENV_EXPORT_CACHE" ] && EXPORT_CACHE="$_AIS_ENV_EXPORT_CACHE"
[ -n "$_AIS_ENV_BRONZE_CHECKPOINT_RUN_ID" ] && BRONZE_CHECKPOINT_RUN_ID="$_AIS_ENV_BRONZE_CHECKPOINT_RUN_ID"
[ -n "$_AIS_ENV_BASE_TIME" ] && BASE_TIME="$_AIS_ENV_BASE_TIME"
[ -n "$_AIS_ENV_ERA5_CONVERT_TIMEOUT_SEC" ] && ERA5_CONVERT_TIMEOUT_SEC="$_AIS_ENV_ERA5_CONVERT_TIMEOUT_SEC"
[ -n "$_AIS_ENV_HDFS_CMD_TIMEOUT_SEC" ] && HDFS_CMD_TIMEOUT_SEC="$_AIS_ENV_HDFS_CMD_TIMEOUT_SEC"
[ -n "$_AIS_ENV_HDFS_CLIENT_USE_DATANODE_HOSTNAME" ] && HDFS_CLIENT_USE_DATANODE_HOSTNAME="$_AIS_ENV_HDFS_CLIENT_USE_DATANODE_HOSTNAME"
[ -n "$_AIS_ENV_HDFS_NAMENODE" ] && HDFS_NAMENODE="$_AIS_ENV_HDFS_NAMENODE"
[ -n "$_AIS_ENV_HDFS_DEFAULT_FS" ] && HDFS_DEFAULT_FS="$_AIS_ENV_HDFS_DEFAULT_FS"
[ -n "$_AIS_ENV_HADOOP_DEFAULT_FS" ] && HADOOP_DEFAULT_FS="$_AIS_ENV_HADOOP_DEFAULT_FS"
[ -n "$_AIS_ENV_DRY_RUN" ] && DRY_RUN="$_AIS_ENV_DRY_RUN"
[ -n "$_AIS_ENV_CASSANDRA_FEATURE_LATEST_ONLY" ] && CASSANDRA_FEATURE_LATEST_ONLY="$_AIS_ENV_CASSANDRA_FEATURE_LATEST_ONLY"
[ -n "$_AIS_ENV_ONLINE_FEATURE_LOOKBACK_HOURS" ] && ONLINE_FEATURE_LOOKBACK_HOURS="$_AIS_ENV_ONLINE_FEATURE_LOOKBACK_HOURS"
[ -n "$_AIS_ENV_ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK" ] && ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK="$_AIS_ENV_ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK"
[ -n "$_AIS_ENV_FEATURE_SOURCE" ] && FEATURE_SOURCE="$_AIS_ENV_FEATURE_SOURCE"
[ -n "$_AIS_ENV_WRITE_CASSANDRA_FORECAST" ] && WRITE_CASSANDRA_FORECAST="$_AIS_ENV_WRITE_CASSANDRA_FORECAST"
[ -n "$_AIS_ENV_BASE_HOUR" ] && BASE_HOUR="$_AIS_ENV_BASE_HOUR"

JOB_TYPE="${1:-spark-smoke}"
if [ "$#" -gt 0 ]; then
  shift
fi

KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"
STOP_AFTER_BATCH="${STOP_AFTER_BATCH:-false}"
PROCESSING_TIME="${PROCESSING_TIME:-}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
FULL_REFRESH="${FULL_REFRESH:-0}"
MAIAC_LOCAL_FALLBACK_PATH="${MAIAC_LOCAL_FALLBACK_PATH:-/opt/maiac_data}"
MAIAC_RELAXED_QA="${MAIAC_RELAXED_QA:-0}"
BASE_TIME="${BASE_TIME:-}"
DRY_RUN="${DRY_RUN:-0}"
VIS_HORIZONS="${VIS_HORIZONS:-}"
VIS_PRODUCT_VERSION="${VIS_PRODUCT_VERSION:-windy_v1}"
VIS_SCHEMA_VERSION="${VIS_SCHEMA_VERSION:-1}"
VIS_GRID_RESOLUTION_DEG="${VIS_GRID_RESOLUTION_DEG:-}"
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
BRONZE_CHECKPOINT_RUN_ID="${BRONZE_CHECKPOINT_RUN_ID:-}"
ERA5_CONVERT_TIMEOUT_SEC="${ERA5_CONVERT_TIMEOUT_SEC:-}"
HDFS_CMD_TIMEOUT_SEC="${HDFS_CMD_TIMEOUT_SEC:-}"
HDFS_NAMENODE="${HDFS_NAMENODE:-${HDFS_DEFAULT_FS:-${HADOOP_DEFAULT_FS:-hdfs://namenode:9000}}}"
HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS:-$HDFS_NAMENODE}"
HADOOP_DEFAULT_FS="${HADOOP_DEFAULT_FS:-$HDFS_DEFAULT_FS}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-${HDFS_NAMENODE%/}/warehouse/iceberg}"
CASSANDRA_FEATURE_LATEST_ONLY="${CASSANDRA_FEATURE_LATEST_ONLY:-0}"
ONLINE_FEATURE_LOOKBACK_HOURS="${ONLINE_FEATURE_LOOKBACK_HOURS:-30}"
ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK="${ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK:-1}"
FEATURE_SOURCE="${FEATURE_SOURCE:-iceberg}"
WRITE_CASSANDRA_FORECAST="${WRITE_CASSANDRA_FORECAST:-0}"
BASE_HOUR="${BASE_HOUR:-}"

# Runtime image pre-bakes Spark dependencies; keep package injection opt-in.
KAFKA_HADOOP_PACKAGES=""
ICEBERG_PACKAGES=""
CASSANDRA_PACKAGES=""

APP_NAME=""
JOB_FILE=""
JOB_ARGS=()
STREAM_ARGS=()
PACKAGES="${ICEBERG_PACKAGES}"
KAFKA_TOPIC="${KAFKA_TOPIC:-}"
ICEBERG_TABLE="${ICEBERG_TABLE:-}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-}"

# Chuan hoa gia tri dau vao truoc khi tao lenh submit.
normalize_hdfs_uri() {
  local value="$1"
  local base="${HDFS_NAMENODE%/}"
  if [[ "$value" == hdfs://namenode:9000/* ]]; then
    printf '%s%s' "$base" "${value#hdfs://namenode:9000}"
  elif [[ "$value" == hdfs://host.docker.internal:9000/* ]]; then
    printf '%s%s' "$base" "${value#hdfs://host.docker.internal:9000}"
  elif [[ "$value" == hdfs://192.168.65.254:9000/* ]]; then
    printf '%s%s' "$base" "${value#hdfs://192.168.65.254:9000}"
  else
    printf '%s' "$value"
  fi
}

case "$JOB_TYPE" in
  spark-smoke)
    APP_NAME="AIS_SparkK8sSmoke"
    JOB_FILE="/opt/spark-jobs/spark_k8s_smoke.py"
    ;;
  weather)
    APP_NAME="WeatherHistory_Streaming"
    JOB_FILE="/opt/spark-jobs/weather_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-weather_history}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.weather.weather_history_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://192.168.65.254:9000/checkpoints/weather_history/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-files)
    APP_NAME="ERA5Files_Streaming"
    JOB_FILE="/opt/spark-jobs/era5_files_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-era5-files}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.weather.era5_files_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://192.168.65.254:9000/checkpoints/era5_files/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq)
    APP_NAME="OpenAQHourly_Streaming"
    JOB_FILE="/opt/spark-jobs/openaq_hourly_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-openaq-hourly}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.air_quality.openaq_hourly_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://192.168.65.254:9000/checkpoints/openaq_hourly/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p)
    APP_NAME="Sentinel5PSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/sentinel5p_summary_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-sentinel5p-summary}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.satellite.sentinel5p_summary_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://192.168.65.254:9000/checkpoints/sentinel5p_summary/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac)
    APP_NAME="MAIACSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/maiac_summary_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-maiac-summary}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.satellite.maiac_summary_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://192.168.65.254:9000/checkpoints/maiac_summary/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-openaq-silver)
    APP_NAME="HanoiOpenAQSilver"
    JOB_FILE="/opt/spark-jobs/hanoi_openaq_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  hanoi-weather-silver)
    APP_NAME="HanoiWeatherSurfaceProxySilver"
    JOB_FILE="/opt/spark-jobs/hanoi_weather_surface_proxy_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  era5-surface-hanoi-silver)
    APP_NAME="ERA5SurfaceHanoiSilver"
    JOB_FILE="/opt/spark-jobs/era5_surface_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  era5-pressure-arl)
    APP_NAME="ERA5PressureLevelsToARL"
    JOB_FILE="/opt/spark-jobs/era5_pressure_levels_to_arl.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  hysplit-run)
    APP_NAME="HYSPLITTrajectoryRun"
    JOB_FILE="/opt/spark-jobs/hysplit_trajectory_run.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
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
  hysplit-parse)
    APP_NAME="HYSPLITTrajectoryParseSilver"
    JOB_FILE="/opt/spark-jobs/hysplit_trajectory_parse_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  hysplit-cluster)
    APP_NAME="HYSPLITTrajectoryClusterSilver"
    JOB_FILE="/opt/spark-jobs/hysplit_trajectory_cluster_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    if [ -n "${ANCHOR_HOURS:-}" ]; then
      JOB_ARGS+=("--anchor-hours" "$ANCHOR_HOURS")
    fi
    ;;
  sentinel5p-hanoi-silver)
    APP_NAME="Sentinel5PHanoiSilver"
    JOB_FILE="/opt/spark-jobs/sentinel5p_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  openaq-gradient)
    APP_NAME="OpenAQSpatialGradientSilver"
    JOB_FILE="/opt/spark-jobs/openaq_spatial_gradient_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  s5p-grid-silver)
    APP_NAME="Sentinel5PGridSilver"
    JOB_FILE="/opt/spark-jobs/sentinel5p_grid_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  traj-path-sampling)
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
    if [ -n "$TRAJ_SPATIAL_BUCKET_DEG" ]; then
      JOB_ARGS+=("--spatial-bucket-deg" "$TRAJ_SPATIAL_BUCKET_DEG")
    fi
    ;;
  traj-hourly-features)
    APP_NAME="TrajectoryHourlyFeaturesSilver"
    JOB_FILE="/opt/spark-jobs/trajectory_hourly_features_silver.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--full-refresh" "$FULL_REFRESH")
    ;;
  maiac-hanoi-silver)
    APP_NAME="MAIACHanoiSilver"
    JOB_FILE="/opt/spark-jobs/maiac_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--local-fallback-path" "$MAIAC_LOCAL_FALLBACK_PATH" "--relaxed-qa" "$MAIAC_RELAXED_QA")
    ;;
  hanoi-master-features-gold)
    APP_NAME="HanoiPM25MasterFeaturesGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_master_features_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  hanoi-training-dataset-gold)
    APP_NAME="HanoiPM25TrainingDatasetGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_training_dataset_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  hanoi-serving-features-gold)
    APP_NAME="HanoiPM25ServingFeaturesGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_serving_features_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ;;
  pm25-features-cassandra)
    APP_NAME="PM25ServingFeaturesToCassandra"
    JOB_FILE="/opt/spark-jobs/pm25_serving_features_to_cassandra.py"
    JOB_ARGS=("--latest-only" "${CASSANDRA_FEATURE_LATEST_ONLY:-0}" "--dry-run" "$DRY_RUN")
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  online-pm25-features)
    APP_NAME="OnlinePM25FeatureBuilder"
    JOB_FILE="/opt/spark-jobs/online_pm25_feature_builder.py"
    JOB_ARGS=("--base-time" "${BASE_TIME:-${BASE_HOUR:-}}" "--lookback-hours" "${ONLINE_FEATURE_LOOKBACK_HOURS:-30}" "--dry-run" "$DRY_RUN")
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-weather)
    APP_NAME="IcebergToCassandra_Weather"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("weather")
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-openaq)
    APP_NAME="IcebergToCassandra_OpenAQ"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("openaq")
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  ensure-iceberg)
    APP_NAME="AIS_EnsureIcebergTables"
    JOB_FILE="/opt/spark-jobs/ensure_iceberg_tables.py"
    ;;
  bronze-pipeline)
    APP_NAME="AISBronzeIngestToIcebergPipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/bronze_ingest_to_iceberg_pipeline.py"
    JOB_ARGS=("--sources" "${PIPELINE_SOURCES:-openaq,weather,sentinel5p,maiac,era5-files}" "--continue-on-error" "${PIPELINE_CONTINUE_ON_ERROR:-false}")
    ;;
  pm25-feature-pipeline)
    APP_NAME="AISPM25FeaturePipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/pm25_feature_pipeline.py"
    JOB_ARGS=()
    if [ -n "$START_DATE" ]; then
      JOB_ARGS+=("--start-date" "$START_DATE")
    fi
    if [ -n "$END_DATE" ]; then
      JOB_ARGS+=("--end-date" "$END_DATE")
    fi
    JOB_ARGS+=("--full-refresh" "$FULL_REFRESH")
    if [ -n "${PIPELINE_STEPS:-}" ]; then
      JOB_ARGS+=("--steps" "${PIPELINE_STEPS:-}")
    fi
    ;;
  trajectory-post-pipeline)
    APP_NAME="AISTrajectoryPostPipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/trajectory_post_pipeline.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--direction" "${DIRECTION:-both}" "--full-refresh" "$FULL_REFRESH" "--spatial-bucket-deg" "${TRAJ_SPATIAL_BUCKET_DEG:-0.25}" "--max-distance-deg" "${MAX_DISTANCE_DEG:-0.25}")
    ;;
  visualization-pipeline)
    APP_NAME="AISVisualizationPipeline"
    JOB_FILE="/opt/spark-jobs/pipelines/visualization_pipeline.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--asof-time" "${BASE_TIME:-}" "--layers" "${PIPELINE_LAYERS:-heatmap,backward_trajectories,forward_plume,source_attribution,stations,forecast,timeseries}" "--export-cache" "${EXPORT_CACHE:-true}" "--full-refresh" "$FULL_REFRESH")
    ;;
  maintenance-iceberg)
    APP_NAME="AIS_IcebergMaintenance"
    JOB_FILE="/opt/spark-jobs/iceberg_maintenance.py"
    JOB_ARGS=("--retention-hours" "${RETENTION_HOURS:-168}")
    ;;
  reconcile-serving)
    APP_NAME="AIS_ReconcileServing"
    JOB_FILE="/opt/spark-jobs/reconcile_iceberg_cassandra.py"
    JOB_ARGS=("--lookback-hours" "${RECONCILE_LOOKBACK_HOURS:-24}" "--tolerance" "${RECONCILE_TOLERANCE:-0.95}")
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  visualization-heatmap-grid)
    APP_NAME="VisualizationPM25HeatmapGridGold"
    JOB_FILE="/opt/spark-jobs/visualization_pm25_heatmap_grid_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-backward-trajectories)
    APP_NAME="VisualizationBackwardTrajectoryPathsGold"
    JOB_FILE="/opt/spark-jobs/visualization_backward_trajectory_paths_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-forward-plume)
    APP_NAME="VisualizationForwardPlumeProbabilityGold"
    JOB_FILE="/opt/spark-jobs/visualization_forward_plume_probability_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-forecast-dashboard)
    APP_NAME="VisualizationForecastDashboardGold"
    JOB_FILE="/opt/spark-jobs/visualization_forecast_dashboard_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-pm25-timeseries)
    APP_NAME="VisualizationPM25TimeseriesGold"
    JOB_FILE="/opt/spark-jobs/visualization_pm25_timeseries_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-source-attribution)
    APP_NAME="VisualizationSourceAttributionGold"
    JOB_FILE="/opt/spark-jobs/visualization_source_attribution_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-station-observations)
    APP_NAME="VisualizationStationObservationsGold"
    JOB_FILE="/opt/spark-jobs/visualization_station_observations_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-export-cache)
    APP_NAME="ExportVisualizationCache"
    JOB_FILE="/opt/spark-jobs/export_visualization_cache.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--dry-run" "$DRY_RUN")
    ;;
  visualization-quality-checks)
    APP_NAME="VisualizationQualityChecks"
    JOB_FILE="/opt/spark-jobs/visualization_quality_checks.py"
    JOB_ARGS=("--dry-run" "$DRY_RUN")
    ;;
  *)
    echo "Usage: $0 [spark-smoke|weather|openaq|sentinel5p|maiac|era5-files|hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|traj-path-sampling|traj-hourly-features|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-serving-features-gold|pm25-features-cassandra|online-pm25-features|cassandra-weather|cassandra-openaq|ensure-iceberg|maintenance-iceberg|reconcile-serving|bronze-pipeline|pm25-feature-pipeline|trajectory-post-pipeline|visualization-pipeline|visualization-heatmap-grid|visualization-backward-trajectories|visualization-forward-plume|visualization-forecast-dashboard|visualization-pm25-timeseries|visualization-source-attribution|visualization-station-observations|visualization-export-cache|visualization-quality-checks]"
    exit 1
    ;;
esac

CHECKPOINT_PATH="$(normalize_hdfs_uri "$CHECKPOINT_PATH")"
if [ -n "${HYSPLIT_OUTPUT_BASE_PATH:-}" ]; then
  HYSPLIT_OUTPUT_BASE_PATH="$(normalize_hdfs_uri "$HYSPLIT_OUTPUT_BASE_PATH")"
else
  HYSPLIT_OUTPUT_BASE_PATH="${HDFS_NAMENODE%/}/raw/hysplit/trajectories"
fi

case "$JOB_TYPE" in
  weather|openaq|sentinel5p|maiac|era5-files)
    if [ "$STOP_AFTER_BATCH" = "true" ]; then
      STREAM_ARGS+=("--stop-after-batch" "1")
    fi
    if [ -n "$PROCESSING_TIME" ]; then
      STREAM_ARGS+=("--processing-time" "$PROCESSING_TIME")
    fi
    ;;
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-serving-features-gold|pm25-features-cassandra|visualization-heatmap-grid|visualization-backward-trajectories|visualization-forward-plume|visualization-forecast-dashboard|visualization-pm25-timeseries|visualization-source-attribution|visualization-station-observations|visualization-export-cache|visualization-quality-checks)
    if [ -n "$START_DATE" ]; then
      JOB_ARGS+=("--start-date" "$START_DATE")
    fi
    if [ -n "$END_DATE" ]; then
      JOB_ARGS+=("--end-date" "$END_DATE")
    fi
    if [ -n "$VIS_HORIZONS" ]; then
      JOB_ARGS+=("--horizons" "$VIS_HORIZONS")
    fi
    if [ -n "$VIS_GRID_RESOLUTION_DEG" ]; then
      JOB_ARGS+=("--grid-resolution-deg" "$VIS_GRID_RESOLUTION_DEG")
    fi
    ;;
esac

case "$JOB_TYPE" in
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|openaq-gradient|hanoi-master-features-gold|hanoi-serving-features-gold|pm25-feature-pipeline)
    if [ -n "$BASE_TIME" ]; then
      JOB_ARGS+=("--asof-time" "$BASE_TIME")
    fi
    ;;
  visualization-heatmap-grid|visualization-backward-trajectories|visualization-forward-plume|visualization-forecast-dashboard|visualization-pm25-timeseries|visualization-source-attribution|visualization-station-observations|visualization-export-cache|visualization-quality-checks)
    if [ -n "$BASE_TIME" ]; then
      JOB_ARGS+=("--base-time" "$BASE_TIME")
    fi
    ;;
esac

case "$JOB_TYPE" in
  visualization-heatmap-grid|visualization-backward-trajectories|visualization-forward-plume|visualization-forecast-dashboard|visualization-pm25-timeseries|visualization-source-attribution|visualization-station-observations|visualization-export-cache|visualization-quality-checks)
    JOB_ARGS+=("--product-version" "$VIS_PRODUCT_VERSION" "--schema-version" "$VIS_SCHEMA_VERSION")
    ;;
esac

JOB_ARGS+=("${STREAM_ARGS[@]}")
JOB_ARGS+=("$@")

# Kiem tra command bat buoc da ton tai trong moi truong.
require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] Required command not found: $1" >&2
    exit 127
  fi
}

# Escape gia tri de truyen an toan qua shell.
shell_quote() {
  printf "'%s'" "$(printf "%s" "$1" | sed "s/'/'\\\\''/g")"
}

job_args_string=""
for arg in "${JOB_ARGS[@]}"; do
  if [ -n "$job_args_string" ]; then
    job_args_string+=" "
  fi
  job_args_string+="$(shell_quote "$arg")"
done

# Chuan hoa ten de dung an toan trong resource name.
sanitize_name() {
  printf "%s" "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//' \
    | cut -c1-42
}

K8S_NAMESPACE="${SPARK_K8S_NAMESPACE:-${K8S_NAMESPACE:-ais}}"
SUBMIT_JOB_PREFIX="$(sanitize_name "ais-${JOB_TYPE}")"
SUBMIT_JOB_NAME="${SPARK_SUBMIT_JOB_NAME:-${SUBMIT_JOB_PREFIX}-$(date +%Y%m%d%H%M%S)}"
SPARK_RUNTIME_APP_NAME="${SPARK_RUNTIME_APP_NAME:-${APP_NAME}-${SUBMIT_JOB_NAME}}"
SPARK_SUBMIT_IMAGE="${SPARK_SUBMIT_IMAGE:-${SPARK_IMAGE:-ais-spark-runtime:local}}"
SPARK_SUBMIT_IMAGE_PULL_POLICY="${SPARK_SUBMIT_IMAGE_PULL_POLICY:-IfNotPresent}"
SPARK_SUBMIT_SERVICE_ACCOUNT="${SPARK_SUBMIT_SERVICE_ACCOUNT:-spark}"
KUBECTL_TIMEOUT="${KUBECTL_TIMEOUT:-1800s}"
JOB_ACTIVE_DEADLINE_SECONDS="${JOB_ACTIVE_DEADLINE_SECONDS:-3600}"
JOB_TTL_SECONDS_AFTER_FINISHED="${JOB_TTL_SECONDS_AFTER_FINISHED:-600}"
WAIT_FOR_COMPLETION="${WAIT_FOR_COMPLETION:-true}"
FOLLOW_LOGS="${FOLLOW_LOGS:-true}"
DELETE_EXISTING="${DELETE_EXISTING:-false}"
SPARK_PACKAGES="${SPARK_JARS_PACKAGES:-}"

require_command kubectl

if [ "$DELETE_EXISTING" = "true" ]; then
  kubectl -n "$K8S_NAMESPACE" delete job "$SUBMIT_JOB_NAME" --ignore-not-found=true
fi

echo "=== Submit Spark-on-Kubernetes Job ==="
echo "namespace=${K8S_NAMESPACE}"
echo "submit_job=${SUBMIT_JOB_NAME}"
echo "app=${SPARK_RUNTIME_APP_NAME}"
echo "job_file=${JOB_FILE}"

kubectl -n "$K8S_NAMESPACE" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${SUBMIT_JOB_NAME}
  labels:
    app.kubernetes.io/name: spark-submit
    app.kubernetes.io/part-of: atmospheric-intelligence-system
    ais/job-type: ${JOB_TYPE}
spec:
  activeDeadlineSeconds: ${JOB_ACTIVE_DEADLINE_SECONDS}
  backoffLimit: 0
  ttlSecondsAfterFinished: ${JOB_TTL_SECONDS_AFTER_FINISHED}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: spark-submit
        ais/job-type: ${JOB_TYPE}
    spec:
      serviceAccountName: ${SPARK_SUBMIT_SERVICE_ACCOUNT}
      restartPolicy: Never
      containers:
        - name: spark-submit
          image: ${SPARK_SUBMIT_IMAGE}
          imagePullPolicy: ${SPARK_SUBMIT_IMAGE_PULL_POLICY}
          envFrom:
            - configMapRef:
                name: ais-runtime-config
            - secretRef:
                name: ais-runtime-secrets
                optional: true
          env:
            - name: APP_NAME
              value: "$(printf "%s" "$SPARK_RUNTIME_APP_NAME")"
            - name: JOB_FILE
              value: "$(printf "%s" "$JOB_FILE")"
            - name: AIS_JOB_ARGS
              value: "$(printf "%s" "$job_args_string")"
            - name: SPARK_JARS_PACKAGES
              value: "$(printf "%s" "$SPARK_PACKAGES")"
            - name: KAFKA_STARTING_OFFSETS
              value: "$(printf "%s" "$KAFKA_STARTING_OFFSETS")"
            - name: KAFKA_TOPIC
              value: "$(printf "%s" "$KAFKA_TOPIC")"
            - name: ICEBERG_TABLE
              value: "$(printf "%s" "$ICEBERG_TABLE")"
            - name: CHECKPOINT_PATH
              value: "$(printf "%s" "$CHECKPOINT_PATH")"
            - name: START_DATE
              value: "$(printf "%s" "$START_DATE")"
            - name: END_DATE
              value: "$(printf "%s" "$END_DATE")"
            - name: FULL_REFRESH
              value: "$(printf "%s" "$FULL_REFRESH")"
            - name: HDFS_NAMENODE
              value: "$(printf "%s" "$HDFS_NAMENODE")"
            - name: HDFS_DEFAULT_FS
              value: "$(printf "%s" "$HDFS_DEFAULT_FS")"
            - name: HADOOP_DEFAULT_FS
              value: "$(printf "%s" "$HADOOP_DEFAULT_FS")"
            - name: HDFS_CLIENT_USE_DATANODE_HOSTNAME
              value: "$(printf "%s" "${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}")"
            - name: S5P_QA_THRESHOLD
              value: "$(printf "%s" "${S5P_QA_THRESHOLD:-}")"
            - name: S5P_NO2_QA_THRESHOLD
              value: "$(printf "%s" "${S5P_NO2_QA_THRESHOLD:-}")"
            - name: S5P_CO_QA_THRESHOLD
              value: "$(printf "%s" "${S5P_CO_QA_THRESHOLD:-}")"
            - name: S5P_SO2_QA_THRESHOLD
              value: "$(printf "%s" "${S5P_SO2_QA_THRESHOLD:-}")"
            - name: S5P_O3_QA_THRESHOLD
              value: "$(printf "%s" "${S5P_O3_QA_THRESHOLD:-}")"
            - name: S5P_AER_AI_QA_THRESHOLD
              value: "$(printf "%s" "${S5P_AER_AI_QA_THRESHOLD:-}")"
            - name: ERA5_ARL_OUTPUT_BASE_PATH
              value: "$(printf "%s" "${ERA5_ARL_OUTPUT_BASE_PATH:-}")"
            - name: HYSPLIT_ERA5_2ARL_BIN
              value: "$(printf "%s" "${HYSPLIT_ERA5_2ARL_BIN:-}")"
            - name: HYSPLIT_ERA5_2ARL_TEMPLATE
              value: "$(printf "%s" "${HYSPLIT_ERA5_2ARL_TEMPLATE:-}")"
            - name: HYSPLIT_BIN
              value: "$(printf "%s" "${HYSPLIT_BIN:-/opt/hysplit/exec/hyts_std}")"
            - name: HYSPLIT_OUTPUT_BASE_PATH
              value: "$(printf "%s" "$HYSPLIT_OUTPUT_BASE_PATH")"
            - name: HYSPLIT_MAX_RUNS
              value: "$(printf "%s" "${HYSPLIT_MAX_RUNS:-}")"
            - name: HYSPLIT_PARALLELISM
              value: "$(printf "%s" "${HYSPLIT_PARALLELISM:-}")"
            - name: HYSPLIT_TIMEOUT_SEC
              value: "$(printf "%s" "${HYSPLIT_TIMEOUT_SEC:-}")"
            - name: HYSPLIT_SHARD_ID
              value: "$(printf "%s" "${HYSPLIT_SHARD_ID:-}")"
            - name: HYSPLIT_SHARD_COUNT
              value: "$(printf "%s" "${HYSPLIT_SHARD_COUNT:-}")"
            - name: TRAJ_SPATIAL_BUCKET_DEG
              value: "$(printf "%s" "${TRAJ_SPATIAL_BUCKET_DEG:-}")"
            - name: MAX_DISTANCE_DEG
              value: "$(printf "%s" "${MAX_DISTANCE_DEG:-}")"
            - name: VIS_MAX_TRAJECTORIES
              value: "$(printf "%s" "${VIS_MAX_TRAJECTORIES:-}")"
            - name: VIS_MAX_POINTS_PER_TRAJECTORY
              value: "$(printf "%s" "${VIS_MAX_POINTS_PER_TRAJECTORY:-}")"
            - name: VIS_MAX_GEOJSON_FEATURES
              value: "$(printf "%s" "${VIS_MAX_GEOJSON_FEATURES:-}")"
            - name: PIPELINE_SOURCES
              value: "$(printf "%s" "${PIPELINE_SOURCES:-}")"
            - name: PIPELINE_CONTINUE_ON_ERROR
              value: "$(printf "%s" "${PIPELINE_CONTINUE_ON_ERROR:-}")"
            - name: PIPELINE_STEPS
              value: "$(printf "%s" "${PIPELINE_STEPS:-}")"
            - name: PIPELINE_LAYERS
              value: "$(printf "%s" "${PIPELINE_LAYERS:-}")"
            - name: EXPORT_CACHE
              value: "$(printf "%s" "${EXPORT_CACHE:-}")"
            - name: BRONZE_CHECKPOINT_RUN_ID
              value: "$(printf "%s" "${BRONZE_CHECKPOINT_RUN_ID:-}")"
            - name: ERA5_CONVERT_TIMEOUT_SEC
              value: "$(printf "%s" "${ERA5_CONVERT_TIMEOUT_SEC:-}")"
            - name: HDFS_CMD_TIMEOUT_SEC
              value: "$(printf "%s" "${HDFS_CMD_TIMEOUT_SEC:-}")"
            - name: DIRECTION
              value: "$(printf "%s" "${DIRECTION:-}")"
            - name: ANCHOR_HOURS
              value: "$(printf "%s" "${ANCHOR_HOURS:-}")"
            - name: PM25_TRIGGER_THRESHOLD
              value: "$(printf "%s" "${PM25_TRIGGER_THRESHOLD:-}")"
            - name: SPARK_SMOKE_CHECK_ICEBERG
              value: "$(printf "%s" "${SPARK_SMOKE_CHECK_ICEBERG:-1}")"
            - name: BASE_TIME
              value: "$(printf "%s" "${BASE_TIME:-}")"
            - name: BASE_HOUR
              value: "$(printf "%s" "${BASE_HOUR:-}")"
            - name: ONLINE_FEATURE_LOOKBACK_HOURS
              value: "$(printf "%s" "${ONLINE_FEATURE_LOOKBACK_HOURS:-30}")"
            - name: ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK
              value: "$(printf "%s" "${ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK:-1}")"
          resources:
            requests:
              cpu: ${SPARK_SUBMIT_REQUEST_CPU:-500m}
              memory: ${SPARK_SUBMIT_REQUEST_MEMORY:-1Gi}
            limits:
              cpu: ${SPARK_SUBMIT_LIMIT_CPU:-1}
              memory: ${SPARK_SUBMIT_LIMIT_MEMORY:-2Gi}
          command:
            - /bin/bash
            - -lc
          args:
            - |
              set -euo pipefail
              eval "set -- \${AIS_JOB_ARGS:-}"
              mkdir -p "\${SPARK_IVY_DIR:-/tmp/.ivy2}"

              submit_args=(
                --master "\${SPARK_K8S_MASTER:-k8s://https://kubernetes.default.svc}"
                --deploy-mode cluster
                --name "\${APP_NAME}"
                --conf "spark.kubernetes.namespace=\${SPARK_K8S_NAMESPACE:-ais}"
                --conf "spark.kubernetes.container.image=\${SPARK_IMAGE:-${SPARK_SUBMIT_IMAGE}}"
                --conf "spark.kubernetes.authenticate.driver.serviceAccountName=\${SPARK_DRIVER_SERVICE_ACCOUNT:-spark}"
                --conf "spark.kubernetes.submission.waitAppCompletion=\${SPARK_K8S_WAIT_APP_COMPLETION:-true}"
                --conf "spark.executor.instances=\${SPARK_EXECUTOR_INSTANCES:-2}"
                --conf "spark.executor.memory=\${SPARK_EXECUTOR_MEMORY:-1g}"
                --conf "spark.executor.cores=\${SPARK_EXECUTOR_CORES:-1}"
                --conf "spark.driver.memory=\${SPARK_DRIVER_MEMORY:-1g}"
                --conf "spark.sql.adaptive.enabled=\${SPARK_SQL_ADAPTIVE_ENABLED:-true}"
                --conf "spark.sql.adaptive.coalescePartitions.enabled=\${SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED:-true}"
                --conf "spark.sql.shuffle.partitions=\${SPARK_SQL_SHUFFLE_PARTITIONS:-16}"
                --conf "spark.default.parallelism=\${SPARK_DEFAULT_PARALLELISM:-16}"
                --conf "spark.sql.session.timeZone=\${SPARK_SQL_SESSION_TIMEZONE:-UTC}"
                --conf "spark.kubernetes.driver.request.cores=\${SPARK_DRIVER_REQUEST_CORES:-500m}"
                --conf "spark.kubernetes.driver.limit.cores=\${SPARK_DRIVER_LIMIT_CORES:-1}"
                --conf "spark.kubernetes.executor.request.cores=\${SPARK_EXECUTOR_REQUEST_CORES:-500m}"
                --conf "spark.kubernetes.executor.limit.cores=\${SPARK_EXECUTOR_LIMIT_CORES:-1}"
                --conf "spark.hadoop.fs.defaultFS=\${HDFS_NAMENODE:-\${HDFS_DEFAULT_FS:-\${HADOOP_DEFAULT_FS:-}}}"
                --conf "spark.hadoop.dfs.client.use.datanode.hostname=\${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
                --conf "spark.jars.ivy=\${SPARK_IVY_DIR:-/tmp/.ivy2}"
                --conf "spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS=\${KAFKA_BOOTSTRAP_SERVERS:-}"
                --conf "spark.kubernetes.driverEnv.KAFKA_TOPIC=\${KAFKA_TOPIC:-}"
                --conf "spark.kubernetes.driverEnv.KAFKA_STARTING_OFFSETS=\${KAFKA_STARTING_OFFSETS:-latest}"
                --conf "spark.kubernetes.driverEnv.CHECKPOINT_PATH=\${CHECKPOINT_PATH:-}"
                --conf "spark.kubernetes.driverEnv.START_DATE=\${START_DATE:-}"
                --conf "spark.kubernetes.driverEnv.END_DATE=\${END_DATE:-}"
                --conf "spark.kubernetes.driverEnv.FULL_REFRESH=\${FULL_REFRESH:-0}"
                --conf "spark.kubernetes.driverEnv.HDFS_NAMENODE=\${HDFS_NAMENODE:-}"
                --conf "spark.kubernetes.driverEnv.HDFS_DEFAULT_FS=\${HDFS_DEFAULT_FS:-\${HDFS_NAMENODE:-}}"
                --conf "spark.kubernetes.driverEnv.HADOOP_DEFAULT_FS=\${HADOOP_DEFAULT_FS:-\${HDFS_DEFAULT_FS:-\${HDFS_NAMENODE:-}}}"
                --conf "spark.kubernetes.driverEnv.HDFS_WEBHDFS_BASE=\${HDFS_WEBHDFS_BASE:-}"
                --conf "spark.kubernetes.driverEnv.HDFS_CLIENT_USE_DATANODE_HOSTNAME=\${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_CATALOG=\${ICEBERG_CATALOG:-ais}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_CATALOG_URI=\${ICEBERG_CATALOG_URI:-}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_WAREHOUSE=\${ICEBERG_WAREHOUSE:-}"
                --conf "spark.kubernetes.driverEnv.CASSANDRA_HOST=\${CASSANDRA_HOST:-}"
                --conf "spark.kubernetes.driverEnv.CASSANDRA_PORT=\${CASSANDRA_PORT:-9042}"
                --conf "spark.kubernetes.driverEnv.CASSANDRA_KEYSPACE=\${CASSANDRA_KEYSPACE:-ais_serving}"
                --conf "spark.kubernetes.driverEnv.CASSANDRA_FEATURE_TABLE=\${CASSANDRA_FEATURE_TABLE:-pm25_feature_state_by_location_hour}"
                --conf "spark.kubernetes.driverEnv.CASSANDRA_FORECAST_TABLE=\${CASSANDRA_FORECAST_TABLE:-pm25_forecast_latest_by_location}"
                --conf "spark.kubernetes.driverEnv.FEATURE_SOURCE=\${FEATURE_SOURCE:-iceberg}"
                --conf "spark.kubernetes.driverEnv.WRITE_CASSANDRA_FORECAST=\${WRITE_CASSANDRA_FORECAST:-0}"
                --conf "spark.kubernetes.driverEnv.HANOI_PIPELINE_CONFIG=\${HANOI_PIPELINE_CONFIG:-/opt/config/hanoi_pipeline.yaml}"
                --conf "spark.kubernetes.driverEnv.S5P_QA_THRESHOLD=\${S5P_QA_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.S5P_NO2_QA_THRESHOLD=\${S5P_NO2_QA_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.S5P_CO_QA_THRESHOLD=\${S5P_CO_QA_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.S5P_SO2_QA_THRESHOLD=\${S5P_SO2_QA_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.S5P_O3_QA_THRESHOLD=\${S5P_O3_QA_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.S5P_AER_AI_QA_THRESHOLD=\${S5P_AER_AI_QA_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.ERA5_ARL_OUTPUT_BASE_PATH=\${ERA5_ARL_OUTPUT_BASE_PATH:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_ERA5_2ARL_BIN=\${HYSPLIT_ERA5_2ARL_BIN:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_ERA5_2ARL_TEMPLATE=\${HYSPLIT_ERA5_2ARL_TEMPLATE:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_BIN=\${HYSPLIT_BIN:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_OUTPUT_BASE_PATH=\${HYSPLIT_OUTPUT_BASE_PATH:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_MAX_RUNS=\${HYSPLIT_MAX_RUNS:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_PARALLELISM=\${HYSPLIT_PARALLELISM:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_TIMEOUT_SEC=\${HYSPLIT_TIMEOUT_SEC:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_SHARD_ID=\${HYSPLIT_SHARD_ID:-}"
                --conf "spark.kubernetes.driverEnv.HYSPLIT_SHARD_COUNT=\${HYSPLIT_SHARD_COUNT:-}"
                --conf "spark.kubernetes.driverEnv.TRAJ_SPATIAL_BUCKET_DEG=\${TRAJ_SPATIAL_BUCKET_DEG:-}"
                --conf "spark.kubernetes.driverEnv.MAX_DISTANCE_DEG=\${MAX_DISTANCE_DEG:-}"
                --conf "spark.kubernetes.driverEnv.VIS_MAX_TRAJECTORIES=\${VIS_MAX_TRAJECTORIES:-}"
                --conf "spark.kubernetes.driverEnv.VIS_MAX_POINTS_PER_TRAJECTORY=\${VIS_MAX_POINTS_PER_TRAJECTORY:-}"
                --conf "spark.kubernetes.driverEnv.VIS_MAX_GEOJSON_FEATURES=\${VIS_MAX_GEOJSON_FEATURES:-}"
                --conf "spark.kubernetes.driverEnv.PIPELINE_SOURCES=\${PIPELINE_SOURCES:-}"
                --conf "spark.kubernetes.driverEnv.PIPELINE_CONTINUE_ON_ERROR=\${PIPELINE_CONTINUE_ON_ERROR:-}"
                --conf "spark.kubernetes.driverEnv.PIPELINE_STEPS=\${PIPELINE_STEPS:-}"
                --conf "spark.kubernetes.driverEnv.PIPELINE_LAYERS=\${PIPELINE_LAYERS:-}"
                --conf "spark.kubernetes.driverEnv.EXPORT_CACHE=\${EXPORT_CACHE:-}"
                --conf "spark.kubernetes.driverEnv.BRONZE_CHECKPOINT_RUN_ID=\${BRONZE_CHECKPOINT_RUN_ID:-}"
                --conf "spark.kubernetes.driverEnv.ERA5_CONVERT_TIMEOUT_SEC=\${ERA5_CONVERT_TIMEOUT_SEC:-}"
                --conf "spark.kubernetes.driverEnv.HDFS_CMD_TIMEOUT_SEC=\${HDFS_CMD_TIMEOUT_SEC:-}"
                --conf "spark.kubernetes.driverEnv.DIRECTION=\${DIRECTION:-}"
                --conf "spark.kubernetes.driverEnv.ANCHOR_HOURS=\${ANCHOR_HOURS:-}"
                --conf "spark.kubernetes.driverEnv.PM25_TRIGGER_THRESHOLD=\${PM25_TRIGGER_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.SPARK_SMOKE_CHECK_ICEBERG=\${SPARK_SMOKE_CHECK_ICEBERG:-1}"
                --conf "spark.kubernetes.driverEnv.BASE_TIME=\${BASE_TIME:-}"
                --conf "spark.kubernetes.driverEnv.BASE_HOUR=\${BASE_HOUR:-}"
                --conf "spark.kubernetes.driverEnv.ONLINE_FEATURE_LOOKBACK_HOURS=\${ONLINE_FEATURE_LOOKBACK_HOURS:-30}"
                --conf "spark.kubernetes.driverEnv.ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK=\${ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK:-1}"
                --conf "spark.kubernetes.driverEnv.DRY_RUN=\${DRY_RUN:-0}"
                --conf "spark.kubernetes.driverEnv.VIS_PRODUCT_VERSION=\${VIS_PRODUCT_VERSION:-windy_v1}"
                --conf "spark.kubernetes.driverEnv.VIS_SCHEMA_VERSION=\${VIS_SCHEMA_VERSION:-1}"
                --conf "spark.kubernetes.driverEnv.VIS_HORIZONS=\${VIS_HORIZONS:-0,6,12,24}"
                --conf "spark.kubernetes.driverEnv.VIS_GRID_RESOLUTION_DEG=\${VIS_GRID_RESOLUTION_DEG:-}"
                --conf "spark.kubernetes.driverEnv.VIS_CACHE_BASE_URI=\${VIS_CACHE_BASE_URI:-}"
                --conf "spark.kubernetes.driverEnv.VIS_FORWARD_PLUME_REQUIRED=\${VIS_FORWARD_PLUME_REQUIRED:-false}"
                --conf "spark.kubernetes.driverEnv.VIS_OBS_HISTORY_HOURS=\${VIS_OBS_HISTORY_HOURS:-48}"
                --conf "spark.kubernetes.driverEnv.LOCATION_ID=\${LOCATION_ID:-hanoi}"
                --conf "spark.kubernetes.driverEnv.LOCATION_NAME=\${LOCATION_NAME:-Hanoi}"
                --conf "spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=\${KAFKA_BOOTSTRAP_SERVERS:-}"
                --conf "spark.executorEnv.KAFKA_TOPIC=\${KAFKA_TOPIC:-}"
                --conf "spark.executorEnv.KAFKA_STARTING_OFFSETS=\${KAFKA_STARTING_OFFSETS:-latest}"
                --conf "spark.executorEnv.CHECKPOINT_PATH=\${CHECKPOINT_PATH:-}"
                --conf "spark.executorEnv.START_DATE=\${START_DATE:-}"
                --conf "spark.executorEnv.END_DATE=\${END_DATE:-}"
                --conf "spark.executorEnv.FULL_REFRESH=\${FULL_REFRESH:-0}"
                --conf "spark.executorEnv.HDFS_NAMENODE=\${HDFS_NAMENODE:-}"
                --conf "spark.executorEnv.HDFS_DEFAULT_FS=\${HDFS_DEFAULT_FS:-\${HDFS_NAMENODE:-}}"
                --conf "spark.executorEnv.HADOOP_DEFAULT_FS=\${HADOOP_DEFAULT_FS:-\${HDFS_DEFAULT_FS:-\${HDFS_NAMENODE:-}}}"
                --conf "spark.executorEnv.HDFS_WEBHDFS_BASE=\${HDFS_WEBHDFS_BASE:-}"
                --conf "spark.executorEnv.HDFS_CLIENT_USE_DATANODE_HOSTNAME=\${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
                --conf "spark.executorEnv.ICEBERG_CATALOG=\${ICEBERG_CATALOG:-ais}"
                --conf "spark.executorEnv.ICEBERG_CATALOG_URI=\${ICEBERG_CATALOG_URI:-}"
                --conf "spark.executorEnv.ICEBERG_WAREHOUSE=\${ICEBERG_WAREHOUSE:-}"
                --conf "spark.executorEnv.CASSANDRA_HOST=\${CASSANDRA_HOST:-}"
                --conf "spark.executorEnv.CASSANDRA_PORT=\${CASSANDRA_PORT:-9042}"
                --conf "spark.executorEnv.CASSANDRA_KEYSPACE=\${CASSANDRA_KEYSPACE:-ais_serving}"
                --conf "spark.executorEnv.CASSANDRA_FEATURE_TABLE=\${CASSANDRA_FEATURE_TABLE:-pm25_feature_state_by_location_hour}"
                --conf "spark.executorEnv.CASSANDRA_FORECAST_TABLE=\${CASSANDRA_FORECAST_TABLE:-pm25_forecast_latest_by_location}"
                --conf "spark.executorEnv.FEATURE_SOURCE=\${FEATURE_SOURCE:-iceberg}"
                --conf "spark.executorEnv.WRITE_CASSANDRA_FORECAST=\${WRITE_CASSANDRA_FORECAST:-0}"
                --conf "spark.executorEnv.HANOI_PIPELINE_CONFIG=\${HANOI_PIPELINE_CONFIG:-/opt/config/hanoi_pipeline.yaml}"
                --conf "spark.executorEnv.S5P_QA_THRESHOLD=\${S5P_QA_THRESHOLD:-}"
                --conf "spark.executorEnv.S5P_NO2_QA_THRESHOLD=\${S5P_NO2_QA_THRESHOLD:-}"
                --conf "spark.executorEnv.S5P_CO_QA_THRESHOLD=\${S5P_CO_QA_THRESHOLD:-}"
                --conf "spark.executorEnv.S5P_SO2_QA_THRESHOLD=\${S5P_SO2_QA_THRESHOLD:-}"
                --conf "spark.executorEnv.S5P_O3_QA_THRESHOLD=\${S5P_O3_QA_THRESHOLD:-}"
                --conf "spark.executorEnv.S5P_AER_AI_QA_THRESHOLD=\${S5P_AER_AI_QA_THRESHOLD:-}"
                --conf "spark.executorEnv.ERA5_ARL_OUTPUT_BASE_PATH=\${ERA5_ARL_OUTPUT_BASE_PATH:-}"
                --conf "spark.executorEnv.HYSPLIT_ERA5_2ARL_BIN=\${HYSPLIT_ERA5_2ARL_BIN:-}"
                --conf "spark.executorEnv.HYSPLIT_ERA5_2ARL_TEMPLATE=\${HYSPLIT_ERA5_2ARL_TEMPLATE:-}"
                --conf "spark.executorEnv.HYSPLIT_BIN=\${HYSPLIT_BIN:-}"
                --conf "spark.executorEnv.HYSPLIT_OUTPUT_BASE_PATH=\${HYSPLIT_OUTPUT_BASE_PATH:-}"
                --conf "spark.executorEnv.HYSPLIT_MAX_RUNS=\${HYSPLIT_MAX_RUNS:-}"
                --conf "spark.executorEnv.HYSPLIT_PARALLELISM=\${HYSPLIT_PARALLELISM:-}"
                --conf "spark.executorEnv.HYSPLIT_TIMEOUT_SEC=\${HYSPLIT_TIMEOUT_SEC:-}"
                --conf "spark.executorEnv.HYSPLIT_SHARD_ID=\${HYSPLIT_SHARD_ID:-}"
                --conf "spark.executorEnv.HYSPLIT_SHARD_COUNT=\${HYSPLIT_SHARD_COUNT:-}"
                --conf "spark.executorEnv.TRAJ_SPATIAL_BUCKET_DEG=\${TRAJ_SPATIAL_BUCKET_DEG:-}"
                --conf "spark.executorEnv.MAX_DISTANCE_DEG=\${MAX_DISTANCE_DEG:-}"
                --conf "spark.executorEnv.VIS_MAX_TRAJECTORIES=\${VIS_MAX_TRAJECTORIES:-}"
                --conf "spark.executorEnv.VIS_MAX_POINTS_PER_TRAJECTORY=\${VIS_MAX_POINTS_PER_TRAJECTORY:-}"
                --conf "spark.executorEnv.VIS_MAX_GEOJSON_FEATURES=\${VIS_MAX_GEOJSON_FEATURES:-}"
                --conf "spark.executorEnv.PIPELINE_SOURCES=\${PIPELINE_SOURCES:-}"
                --conf "spark.executorEnv.PIPELINE_CONTINUE_ON_ERROR=\${PIPELINE_CONTINUE_ON_ERROR:-}"
                --conf "spark.executorEnv.PIPELINE_STEPS=\${PIPELINE_STEPS:-}"
                --conf "spark.executorEnv.PIPELINE_LAYERS=\${PIPELINE_LAYERS:-}"
                --conf "spark.executorEnv.EXPORT_CACHE=\${EXPORT_CACHE:-}"
                --conf "spark.executorEnv.BRONZE_CHECKPOINT_RUN_ID=\${BRONZE_CHECKPOINT_RUN_ID:-}"
                --conf "spark.executorEnv.ERA5_CONVERT_TIMEOUT_SEC=\${ERA5_CONVERT_TIMEOUT_SEC:-}"
                --conf "spark.executorEnv.HDFS_CMD_TIMEOUT_SEC=\${HDFS_CMD_TIMEOUT_SEC:-}"
                --conf "spark.executorEnv.DIRECTION=\${DIRECTION:-}"
                --conf "spark.executorEnv.ANCHOR_HOURS=\${ANCHOR_HOURS:-}"
                --conf "spark.executorEnv.PM25_TRIGGER_THRESHOLD=\${PM25_TRIGGER_THRESHOLD:-}"
                --conf "spark.executorEnv.SPARK_SMOKE_CHECK_ICEBERG=\${SPARK_SMOKE_CHECK_ICEBERG:-1}"
                --conf "spark.executorEnv.BASE_TIME=\${BASE_TIME:-}"
                --conf "spark.executorEnv.BASE_HOUR=\${BASE_HOUR:-}"
                --conf "spark.executorEnv.ONLINE_FEATURE_LOOKBACK_HOURS=\${ONLINE_FEATURE_LOOKBACK_HOURS:-30}"
                --conf "spark.executorEnv.ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK=\${ONLINE_FEATURE_ALLOW_KAFKA_FALLBACK:-1}"
                --conf "spark.executorEnv.DRY_RUN=\${DRY_RUN:-0}"
                --conf "spark.executorEnv.VIS_PRODUCT_VERSION=\${VIS_PRODUCT_VERSION:-windy_v1}"
                --conf "spark.executorEnv.VIS_SCHEMA_VERSION=\${VIS_SCHEMA_VERSION:-1}"
                --conf "spark.executorEnv.VIS_HORIZONS=\${VIS_HORIZONS:-0,6,12,24}"
                --conf "spark.executorEnv.VIS_GRID_RESOLUTION_DEG=\${VIS_GRID_RESOLUTION_DEG:-}"
                --conf "spark.executorEnv.VIS_CACHE_BASE_URI=\${VIS_CACHE_BASE_URI:-}"
                --conf "spark.executorEnv.VIS_FORWARD_PLUME_REQUIRED=\${VIS_FORWARD_PLUME_REQUIRED:-false}"
                --conf "spark.executorEnv.VIS_OBS_HISTORY_HOURS=\${VIS_OBS_HISTORY_HOURS:-48}"
                --conf "spark.executorEnv.LOCATION_ID=\${LOCATION_ID:-hanoi}"
                --conf "spark.executorEnv.LOCATION_NAME=\${LOCATION_NAME:-Hanoi}"
                --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                --conf "spark.sql.catalog.\${ICEBERG_CATALOG:-ais}=org.apache.iceberg.spark.SparkCatalog"
                --conf "spark.sql.catalog.\${ICEBERG_CATALOG:-ais}.type=hadoop"
                --conf "spark.sql.catalog.\${ICEBERG_CATALOG:-ais}.warehouse=\${ICEBERG_WAREHOUSE}"
              )

              if [ -n "\${SPARK_JARS_PACKAGES:-}" ]; then
                submit_args+=(--packages "\${SPARK_JARS_PACKAGES}")
              fi
              if [ -n "\${ICEBERG_TABLE:-}" ]; then
                submit_args+=(--conf "spark.kubernetes.driverEnv.ICEBERG_TABLE=\${ICEBERG_TABLE}")
                submit_args+=(--conf "spark.executorEnv.ICEBERG_TABLE=\${ICEBERG_TABLE}")
              fi

              submit_args+=("local://\${JOB_FILE}")
              submit_args+=("\$@")
              exec /opt/spark/bin/spark-submit "\${submit_args[@]}"
YAML

# Chuan hoa tham so thoi gian truoc khi truy van latest hoac historical.
timeout_to_seconds() {
  local raw="${1:-1800s}"
  case "$raw" in
    *s) echo "${raw%s}" ;;
    *m) echo "$(( ${raw%m} * 60 ))" ;;
    *h) echo "$(( ${raw%h} * 3600 ))" ;;
    *) echo "$raw" ;;
  esac
}

# Cho den khi tai nguyen hoac service can dung da san sang.
wait_for_job_terminal() {
  local timeout_sec
  timeout_sec="$(timeout_to_seconds "$KUBECTL_TIMEOUT")"
  local deadline=$((SECONDS + timeout_sec))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local conditions
    conditions="$(kubectl -n "$K8S_NAMESPACE" get job "$SUBMIT_JOB_NAME" -o jsonpath='{range .status.conditions[*]}{.type}={.status}{"\n"}{end}' 2>/dev/null || true)"
    if printf "%s\n" "$conditions" | grep -q '^Complete=True$'; then
      return 0
    fi
    if printf "%s\n" "$conditions" | grep -q '^Failed=True$'; then
      return 1
    fi
    sleep 5
  done
  return 2
}

if [ "$WAIT_FOR_COMPLETION" = "true" ]; then
  if wait_for_job_terminal; then
    DRIVER_PHASES="$(kubectl -n "$K8S_NAMESPACE" get pods -l "spark-role=driver" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}' 2>/dev/null | grep "$SUBMIT_JOB_NAME" || true)"
    if [ -n "$DRIVER_PHASES" ] && printf "%s\n" "$DRIVER_PHASES" | grep -q " Failed"; then
      echo "[ERROR] Spark driver pod failed for submit job ${SUBMIT_JOB_NAME}" >&2
      printf "%s\n" "$DRIVER_PHASES" >&2
      DRIVER_POD_NAME="$(printf "%s\n" "$DRIVER_PHASES" | awk 'NR==1{print $1}')"
      if [ -n "$DRIVER_POD_NAME" ]; then
        kubectl -n "$K8S_NAMESPACE" logs "$DRIVER_POD_NAME" --tail=200 || true
      fi
      exit 1
    fi
    echo "[OK] Spark submit job completed: ${SUBMIT_JOB_NAME}"
    if [ "$FOLLOW_LOGS" = "true" ]; then
      kubectl -n "$K8S_NAMESPACE" logs "job/${SUBMIT_JOB_NAME}" --all-containers=true --tail=300 || true
    fi
  else
    echo "[ERROR] Spark submit job did not complete cleanly: ${SUBMIT_JOB_NAME}" >&2
    kubectl -n "$K8S_NAMESPACE" get job "$SUBMIT_JOB_NAME" -o wide || true
    kubectl -n "$K8S_NAMESPACE" describe job "$SUBMIT_JOB_NAME" || true
    kubectl -n "$K8S_NAMESPACE" get pods -l "job-name=${SUBMIT_JOB_NAME}" -o wide || true
    kubectl -n "$K8S_NAMESPACE" logs "job/${SUBMIT_JOB_NAME}" --all-containers=true --tail=300 || true
    exit 1
  fi
else
  if [ "$FOLLOW_LOGS" = "true" ]; then
    kubectl -n "$K8S_NAMESPACE" logs "job/${SUBMIT_JOB_NAME}" --all-containers=true --tail=300 || true
  fi
fi
