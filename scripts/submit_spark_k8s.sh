#!/bin/bash
# =============================================================================
# submit_spark_k8s.sh
# Submit AIS Spark jobs to Spark-on-Kubernetes.
#
# Required runtime env for production/K8s:
#   SPARK_K8S_MASTER, SPARK_IMAGE, HDFS_NAMENODE, ICEBERG_WAREHOUSE
#
# Example:
#   SPARK_K8S_MASTER=k8s://https://kubernetes.default.svc \
#   SPARK_IMAGE=ais-spark-runtime:local \
#   HDFS_NAMENODE=hdfs://host.docker.internal:9000 \
#   ICEBERG_WAREHOUSE=hdfs://host.docker.internal:9000/warehouse/iceberg \
#   bash scripts/submit_spark_k8s.sh ensure-iceberg
# =============================================================================

set -euo pipefail

if [ -f ".env" ]; then
  set +u
  set -a
  # Support .env files edited on Windows with CRLF line endings.
  source <(tr -d '\r' < .env)
  set +a
  set -u
fi

JOB_TYPE="${1:-ensure-iceberg}"
shift || true

APP_NAME=""
JOB_FILE=""
JOB_ARGS=()
STREAM_ARGS=()
PACKAGES=""
KAFKA_TOPIC=""
ICEBERG_TABLE=""
CHECKPOINT_PATH=""

KAFKA_HADOOP_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-client:3.3.4"
ICEBERG_PACKAGES="${KAFKA_HADOOP_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
CASSANDRA_PACKAGES="${ICEBERG_PACKAGES},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

SPARK_K8S_MASTER="${SPARK_K8S_MASTER:-}"
SPARK_K8S_NAMESPACE="${SPARK_K8S_NAMESPACE:-ais}"
SPARK_DRIVER_SERVICE_ACCOUNT="${SPARK_DRIVER_SERVICE_ACCOUNT:-spark}"
SPARK_IMAGE="${SPARK_IMAGE:-}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-2}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_DRIVER_CORES="${SPARK_DRIVER_CORES:-1}"
SPARK_DRIVER_REQUEST_CORES="${SPARK_DRIVER_REQUEST_CORES:-500m}"
SPARK_EXECUTOR_REQUEST_CORES="${SPARK_EXECUTOR_REQUEST_CORES:-500m}"
SPARK_JARS_IVY="${SPARK_JARS_IVY:-/tmp/.ivy2}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-ais}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-}"
HDFS_NAMENODE="${HDFS_NAMENODE:-}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-}"
CASSANDRA_HOST="${CASSANDRA_HOST:-}"
CHECKPOINT_BASE_URI="${CHECKPOINT_BASE_URI:-${HDFS_NAMENODE%/}/checkpoints}"
KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"
STOP_AFTER_BATCH="${STOP_AFTER_BATCH:-false}"
PROCESSING_TIME="${PROCESSING_TIME:-}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
FULL_REFRESH="${FULL_REFRESH:-0}"
MAIAC_LOCAL_FALLBACK_PATH="${MAIAC_LOCAL_FALLBACK_PATH:-/opt/maiac_data}"
MAIAC_RELAXED_QA="${MAIAC_RELAXED_QA:-0}"

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "[ERROR] Required env var is not set for Spark-on-K8s: ${name}" >&2
    exit 2
  fi
}

require_env SPARK_K8S_MASTER
require_env SPARK_IMAGE
require_env HDFS_NAMENODE
require_env ICEBERG_WAREHOUSE

case "$JOB_TYPE" in
  weather)
    APP_NAME="WeatherHistory_Streaming"
    JOB_FILE="local:///opt/spark-jobs/weather_streaming.py"
    KAFKA_TOPIC="weather_history"
    ICEBERG_TABLE="${ICEBERG_CATALOG}.weather.weather_history_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/weather_history/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-files)
    APP_NAME="ERA5Files_Streaming"
    JOB_FILE="local:///opt/spark-jobs/era5_files_streaming.py"
    KAFKA_TOPIC="era5-files"
    ICEBERG_TABLE="${ICEBERG_CATALOG}.weather.era5_files_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/era5_files/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq)
    APP_NAME="OpenAQHourly_Streaming"
    JOB_FILE="local:///opt/spark-jobs/openaq_hourly_streaming.py"
    KAFKA_TOPIC="openaq-hourly"
    ICEBERG_TABLE="${ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/openaq_hourly/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p)
    APP_NAME="Sentinel5PSummary_Streaming"
    JOB_FILE="local:///opt/spark-jobs/sentinel5p_summary_streaming.py"
    KAFKA_TOPIC="sentinel5p-summary"
    ICEBERG_TABLE="${ICEBERG_CATALOG}.satellite.sentinel5p_summary_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/sentinel5p_summary/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac)
    APP_NAME="MAIACSummary_Streaming"
    JOB_FILE="local:///opt/spark-jobs/maiac_summary_streaming.py"
    KAFKA_TOPIC="maiac-summary"
    ICEBERG_TABLE="${ICEBERG_CATALOG}.satellite.maiac_summary_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/maiac_summary/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-openaq-silver)
    APP_NAME="HanoiOpenAQSilver"
    JOB_FILE="local:///opt/spark-jobs/hanoi_openaq_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.air_quality.openaq_hanoi_hourly_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hanoi_openaq_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-weather-silver)
    APP_NAME="HanoiWeatherSurfaceProxySilver"
    JOB_FILE="local:///opt/spark-jobs/hanoi_weather_surface_proxy_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.weather.weather_hanoi_surface_proxy_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hanoi_weather_surface_proxy_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-surface-hanoi-silver)
    APP_NAME="ERA5SurfaceHanoiSilver"
    JOB_FILE="local:///opt/spark-jobs/era5_surface_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.weather.era5_surface_hanoi_hourly_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/era5_surface_hanoi_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-pressure-arl)
    APP_NAME="ERA5PressureLevelsToARL"
    JOB_FILE="local:///opt/spark-jobs/era5_pressure_levels_to_arl.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.weather.era5_arl_files_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/era5_pressure_arl/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hysplit-run)
    APP_NAME="HYSPLITTrajectoryRun"
    JOB_FILE="local:///opt/spark-jobs/hysplit_trajectory_run.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.trajectory.hysplit_runs_bronze"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hysplit_run/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hysplit-parse)
    APP_NAME="HYSPLITTrajectoryParseSilver"
    JOB_FILE="local:///opt/spark-jobs/hysplit_trajectory_parse_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.trajectory.hysplit_trajectories_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hysplit_parse/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hysplit-cluster)
    APP_NAME="HYSPLITTrajectoryClusterSilver"
    JOB_FILE="local:///opt/spark-jobs/hysplit_trajectory_cluster_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    if [ -n "${ANCHOR_HOURS:-}" ]; then
      JOB_ARGS+=("--anchor-hours" "$ANCHOR_HOURS")
    fi
    ICEBERG_TABLE="${ICEBERG_CATALOG}.trajectory.hysplit_trajectories_clustered_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hysplit_cluster/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p-hanoi-silver)
    APP_NAME="Sentinel5PHanoiSilver"
    JOB_FILE="local:///opt/spark-jobs/sentinel5p_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.satellite.sentinel5p_hanoi_daily_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/sentinel5p_hanoi_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq-gradient)
    APP_NAME="OpenAQSpatialGradientSilver"
    JOB_FILE="local:///opt/spark-jobs/openaq_spatial_gradient_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.features.openaq_spatial_gradient_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/openaq_spatial_gradient_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  s5p-grid-silver)
    APP_NAME="Sentinel5PGridSilver"
    JOB_FILE="local:///opt/spark-jobs/sentinel5p_grid_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.satellite.sentinel5p_grid_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/sentinel5p_grid_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  traj-path-sampling)
    APP_NAME="TrajectoryPathSamplingSilver"
    JOB_FILE="local:///opt/spark-jobs/trajectory_path_sampling_silver.py"
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
    ICEBERG_TABLE="${ICEBERG_CATALOG}.features.trajectory_path_satellite_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/trajectory_path_sampling_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  traj-hourly-features)
    APP_NAME="TrajectoryHourlyFeaturesSilver"
    JOB_FILE="local:///opt/spark-jobs/trajectory_hourly_features_silver.py"
    JOB_ARGS=("--start-date" "$START_DATE" "--end-date" "$END_DATE" "--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.features.trajectory_hourly_features_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/trajectory_hourly_features_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac-hanoi-silver)
    APP_NAME="MAIACHanoiSilver"
    JOB_FILE="local:///opt/spark-jobs/maiac_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--local-fallback-path" "$MAIAC_LOCAL_FALLBACK_PATH" "--relaxed-qa" "$MAIAC_RELAXED_QA")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.satellite.maiac_hanoi_daily_silver"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/maiac_hanoi_daily_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-master-features-gold)
    APP_NAME="HanoiPM25MasterFeaturesGold"
    JOB_FILE="local:///opt/spark-jobs/hanoi_pm25_master_features_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.features.hanoi_pm25_master_hourly_gold"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hanoi_pm25_master_features_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-training-dataset-gold)
    APP_NAME="HanoiPM25TrainingDatasetGold"
    JOB_FILE="local:///opt/spark-jobs/hanoi_pm25_training_dataset_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.features.hanoi_pm25_training_dataset_gold"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hanoi_pm25_training_dataset_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-train-baseline)
    APP_NAME="TrainHanoiPM25Baseline"
    JOB_FILE="local:///opt/ml/train_hanoi_pm25.py"
    JOB_ARGS=("--dataset-version" "${DATASET_VERSION:-hanoi_pm25_v1}" "--feature-set-name" "${FEATURE_SET_NAME:-hanoi_pm25_core_v1}" "--model-type" "${MODEL_TYPE:-lightgbm}" "--output-dir" "${MODEL_OUTPUT_DIR:-/opt/models/hanoi_pm25}")
    ICEBERG_TABLE="${ICEBERG_CATALOG}.models.hanoi_pm25_model_runs_gold"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/hanoi_train_baseline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  cassandra-weather)
    APP_NAME="IcebergToCassandra_Weather"
    JOB_FILE="local:///opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("weather")
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/iceberg_to_cassandra/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-openaq)
    APP_NAME="IcebergToCassandra_OpenAQ"
    JOB_FILE="local:///opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("openaq")
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/iceberg_to_cassandra/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  ensure-iceberg)
    APP_NAME="AIS_EnsureIcebergTables"
    JOB_FILE="local:///opt/spark-jobs/ensure_iceberg_tables.py"
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/ensure_iceberg/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maintenance-iceberg)
    APP_NAME="AIS_IcebergMaintenance"
    JOB_FILE="local:///opt/spark-jobs/iceberg_maintenance.py"
    JOB_ARGS=("--retention-hours" "${RETENTION_HOURS:-168}")
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/iceberg_maintenance/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  reconcile-serving)
    APP_NAME="AIS_ReconcileServing"
    JOB_FILE="local:///opt/spark-jobs/reconcile_iceberg_cassandra.py"
    JOB_ARGS=("--lookback-hours" "${RECONCILE_LOOKBACK_HOURS:-24}" "--tolerance" "${RECONCILE_TOLERANCE:-0.95}")
    CHECKPOINT_PATH="${CHECKPOINT_BASE_URI}/reconcile_serving/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  *)
    echo "Usage: $0 [weather|openaq|sentinel5p|maiac|era5-files|hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|traj-path-sampling|traj-hourly-features|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-train-baseline|cassandra-weather|cassandra-openaq|ensure-iceberg|maintenance-iceberg|reconcile-serving]"
    exit 1
    ;;
esac

case "$JOB_TYPE" in
  weather|openaq|sentinel5p|maiac|era5-files)
    if [ "$STOP_AFTER_BATCH" = "true" ]; then
      STREAM_ARGS+=("--stop-after-batch" "1")
    fi
    if [ -n "$PROCESSING_TIME" ]; then
      STREAM_ARGS+=("--processing-time" "$PROCESSING_TIME")
    fi
    ;;
esac

case "$JOB_TYPE" in
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold)
    if [ -n "$START_DATE" ]; then
      JOB_ARGS+=("--start-date" "$START_DATE")
    fi
    if [ -n "$END_DATE" ]; then
      JOB_ARGS+=("--end-date" "$END_DATE")
    fi
    ;;
esac

if [ "$#" -gt 0 ]; then
  JOB_ARGS+=("$@")
fi

echo "=== Submit Spark-on-K8s Job: ${APP_NAME} ==="
echo "job_type=${JOB_TYPE}"
echo "spark_master=${SPARK_K8S_MASTER}"
echo "namespace=${SPARK_K8S_NAMESPACE}"
echo "image=${SPARK_IMAGE}"
echo "job_file=${JOB_FILE}"
echo "checkpoint=${CHECKPOINT_PATH}"

/opt/spark/bin/spark-submit \
  --master "${SPARK_K8S_MASTER}" \
  --deploy-mode cluster \
  --name "${APP_NAME}" \
  --conf "spark.kubernetes.namespace=${SPARK_K8S_NAMESPACE}" \
  --conf "spark.kubernetes.container.image=${SPARK_IMAGE}" \
  --conf "spark.kubernetes.authenticate.driver.serviceAccountName=${SPARK_DRIVER_SERVICE_ACCOUNT}" \
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
  --conf "spark.executor.request.cores=${SPARK_EXECUTOR_REQUEST_CORES}" \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" \
  --conf "spark.driver.cores=${SPARK_DRIVER_CORES}" \
  --conf "spark.driver.request.cores=${SPARK_DRIVER_REQUEST_CORES}" \
  --conf "spark.jars.ivy=${SPARK_JARS_IVY}" \
  --repositories "https://repo.maven.apache.org/maven2,https://repo1.maven.org/maven2,https://repos.spark-packages.org" \
  --packages "$PACKAGES" \
  --conf "spark.sql.streaming.checkpointLocation=${CHECKPOINT_PATH}" \
  --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}" \
  --conf "spark.kubernetes.driverEnv.ICEBERG_CATALOG=${ICEBERG_CATALOG}" \
  --conf "spark.kubernetes.driverEnv.ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE}" \
  --conf "spark.kubernetes.driverEnv.HDFS_NAMENODE=${HDFS_NAMENODE}" \
  --conf "spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}" \
  --conf "spark.kubernetes.driverEnv.KAFKA_STARTING_OFFSETS=${KAFKA_STARTING_OFFSETS}" \
  --conf "spark.kubernetes.driverEnv.KAFKA_TOPIC=${KAFKA_TOPIC}" \
  --conf "spark.kubernetes.driverEnv.ICEBERG_TABLE=${ICEBERG_TABLE}" \
  --conf "spark.kubernetes.driverEnv.CHECKPOINT_PATH=${CHECKPOINT_PATH}" \
  --conf "spark.kubernetes.driverEnv.CASSANDRA_HOST=${CASSANDRA_HOST}" \
  --conf "spark.kubernetes.driverEnv.HYSPLIT_BIN=${HYSPLIT_BIN:-/opt/hysplit/exec/hyts_std}" \
  --conf "spark.kubernetes.driverEnv.HYSPLIT_OUTPUT_BASE_PATH=${HYSPLIT_OUTPUT_BASE_PATH:-${HDFS_NAMENODE%/}/raw/hysplit/trajectories}" \
  --conf "spark.executorEnv.ICEBERG_CATALOG=${ICEBERG_CATALOG}" \
  --conf "spark.executorEnv.ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE}" \
  --conf "spark.executorEnv.HDFS_NAMENODE=${HDFS_NAMENODE}" \
  --conf "spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}" \
  --conf "spark.executorEnv.CASSANDRA_HOST=${CASSANDRA_HOST}" \
  "$JOB_FILE" \
  "${JOB_ARGS[@]}" \
  "${STREAM_ARGS[@]}"
