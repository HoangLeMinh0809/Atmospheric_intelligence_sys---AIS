#!/bin/bash
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
_AIS_ENV_HDFS_CLIENT_USE_DATANODE_HOSTNAME="${HDFS_CLIENT_USE_DATANODE_HOSTNAME-}"

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
[ -n "$_AIS_ENV_HDFS_CLIENT_USE_DATANODE_HOSTNAME" ] && HDFS_CLIENT_USE_DATANODE_HOSTNAME="$_AIS_ENV_HDFS_CLIENT_USE_DATANODE_HOSTNAME"

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

KAFKA_HADOOP_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-client:3.3.4"
ICEBERG_PACKAGES="${KAFKA_HADOOP_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
CASSANDRA_PACKAGES="${ICEBERG_PACKAGES},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

APP_NAME=""
JOB_FILE=""
JOB_ARGS=()
STREAM_ARGS=()
PACKAGES="${ICEBERG_PACKAGES}"
KAFKA_TOPIC="${KAFKA_TOPIC:-}"
ICEBERG_TABLE="${ICEBERG_TABLE:-}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-}"

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
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://host.docker.internal:9000/checkpoints/weather_history/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-files)
    APP_NAME="ERA5Files_Streaming"
    JOB_FILE="/opt/spark-jobs/era5_files_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-era5-files}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.weather.era5_files_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://host.docker.internal:9000/checkpoints/era5_files/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq)
    APP_NAME="OpenAQHourly_Streaming"
    JOB_FILE="/opt/spark-jobs/openaq_hourly_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-openaq-hourly}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.air_quality.openaq_hourly_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://host.docker.internal:9000/checkpoints/openaq_hourly/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p)
    APP_NAME="Sentinel5PSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/sentinel5p_summary_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-sentinel5p-summary}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.satellite.sentinel5p_summary_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://host.docker.internal:9000/checkpoints/sentinel5p_summary/}"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac)
    APP_NAME="MAIACSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/maiac_summary_streaming.py"
    KAFKA_TOPIC="${KAFKA_TOPIC:-maiac-summary}"
    ICEBERG_TABLE="${ICEBERG_TABLE:-ais.satellite.maiac_summary_bronze}"
    CHECKPOINT_PATH="${CHECKPOINT_PATH:-hdfs://host.docker.internal:9000/checkpoints/maiac_summary/}"
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
  *)
    echo "Usage: $0 [spark-smoke|weather|openaq|sentinel5p|maiac|era5-files|hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|traj-path-sampling|traj-hourly-features|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-serving-features-gold|cassandra-weather|cassandra-openaq|ensure-iceberg|maintenance-iceberg|reconcile-serving]"
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
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|era5-pressure-arl|hysplit-run|hysplit-parse|hysplit-cluster|sentinel5p-hanoi-silver|openaq-gradient|s5p-grid-silver|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-serving-features-gold)
    if [ -n "$START_DATE" ]; then
      JOB_ARGS+=("--start-date" "$START_DATE")
    fi
    if [ -n "$END_DATE" ]; then
      JOB_ARGS+=("--end-date" "$END_DATE")
    fi
    ;;
esac

JOB_ARGS+=("${STREAM_ARGS[@]}")
JOB_ARGS+=("$@")

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] Required command not found: $1" >&2
    exit 127
  fi
}

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
WAIT_FOR_COMPLETION="${WAIT_FOR_COMPLETION:-true}"
FOLLOW_LOGS="${FOLLOW_LOGS:-true}"
DELETE_EXISTING="${DELETE_EXISTING:-false}"
SPARK_PACKAGES="${SPARK_JARS_PACKAGES:-$PACKAGES}"

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
  backoffLimit: 0
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
              value: "$(printf "%s" "${HYSPLIT_OUTPUT_BASE_PATH:-hdfs://host.docker.internal:9000/raw/hysplit/trajectories}")"
            - name: DIRECTION
              value: "$(printf "%s" "${DIRECTION:-}")"
            - name: ANCHOR_HOURS
              value: "$(printf "%s" "${ANCHOR_HOURS:-}")"
            - name: PM25_TRIGGER_THRESHOLD
              value: "$(printf "%s" "${PM25_TRIGGER_THRESHOLD:-}")"
            - name: SPARK_SMOKE_CHECK_ICEBERG
              value: "$(printf "%s" "${SPARK_SMOKE_CHECK_ICEBERG:-1}")"
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
                --conf "spark.kubernetes.driver.request.cores=\${SPARK_DRIVER_REQUEST_CORES:-500m}"
                --conf "spark.kubernetes.driver.limit.cores=\${SPARK_DRIVER_LIMIT_CORES:-1}"
                --conf "spark.kubernetes.executor.request.cores=\${SPARK_EXECUTOR_REQUEST_CORES:-500m}"
                --conf "spark.kubernetes.executor.limit.cores=\${SPARK_EXECUTOR_LIMIT_CORES:-1}"
                --conf "spark.hadoop.fs.defaultFS=\${HDFS_NAMENODE}"
                --conf "spark.hadoop.dfs.client.use.datanode.hostname=\${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
                --conf "spark.jars.ivy=\${SPARK_IVY_DIR:-/tmp/.ivy2}"
                --conf "spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS=\${KAFKA_BOOTSTRAP_SERVERS:-}"
                --conf "spark.kubernetes.driverEnv.KAFKA_TOPIC=\${KAFKA_TOPIC:-}"
                --conf "spark.kubernetes.driverEnv.KAFKA_STARTING_OFFSETS=\${KAFKA_STARTING_OFFSETS:-latest}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_TABLE=\${ICEBERG_TABLE:-}"
                --conf "spark.kubernetes.driverEnv.CHECKPOINT_PATH=\${CHECKPOINT_PATH:-}"
                --conf "spark.kubernetes.driverEnv.START_DATE=\${START_DATE:-}"
                --conf "spark.kubernetes.driverEnv.END_DATE=\${END_DATE:-}"
                --conf "spark.kubernetes.driverEnv.FULL_REFRESH=\${FULL_REFRESH:-0}"
                --conf "spark.kubernetes.driverEnv.HDFS_NAMENODE=\${HDFS_NAMENODE:-}"
                --conf "spark.kubernetes.driverEnv.HDFS_WEBHDFS_BASE=\${HDFS_WEBHDFS_BASE:-}"
                --conf "spark.kubernetes.driverEnv.HDFS_CLIENT_USE_DATANODE_HOSTNAME=\${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_CATALOG=\${ICEBERG_CATALOG:-ais}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_CATALOG_URI=\${ICEBERG_CATALOG_URI:-}"
                --conf "spark.kubernetes.driverEnv.ICEBERG_WAREHOUSE=\${ICEBERG_WAREHOUSE:-}"
                --conf "spark.kubernetes.driverEnv.CASSANDRA_HOST=\${CASSANDRA_HOST:-}"
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
                --conf "spark.kubernetes.driverEnv.DIRECTION=\${DIRECTION:-}"
                --conf "spark.kubernetes.driverEnv.ANCHOR_HOURS=\${ANCHOR_HOURS:-}"
                --conf "spark.kubernetes.driverEnv.PM25_TRIGGER_THRESHOLD=\${PM25_TRIGGER_THRESHOLD:-}"
                --conf "spark.kubernetes.driverEnv.SPARK_SMOKE_CHECK_ICEBERG=\${SPARK_SMOKE_CHECK_ICEBERG:-1}"
                --conf "spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=\${KAFKA_BOOTSTRAP_SERVERS:-}"
                --conf "spark.executorEnv.KAFKA_TOPIC=\${KAFKA_TOPIC:-}"
                --conf "spark.executorEnv.KAFKA_STARTING_OFFSETS=\${KAFKA_STARTING_OFFSETS:-latest}"
                --conf "spark.executorEnv.ICEBERG_TABLE=\${ICEBERG_TABLE:-}"
                --conf "spark.executorEnv.CHECKPOINT_PATH=\${CHECKPOINT_PATH:-}"
                --conf "spark.executorEnv.START_DATE=\${START_DATE:-}"
                --conf "spark.executorEnv.END_DATE=\${END_DATE:-}"
                --conf "spark.executorEnv.FULL_REFRESH=\${FULL_REFRESH:-0}"
                --conf "spark.executorEnv.HDFS_NAMENODE=\${HDFS_NAMENODE:-}"
                --conf "spark.executorEnv.HDFS_WEBHDFS_BASE=\${HDFS_WEBHDFS_BASE:-}"
                --conf "spark.executorEnv.HDFS_CLIENT_USE_DATANODE_HOSTNAME=\${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
                --conf "spark.executorEnv.ICEBERG_CATALOG=\${ICEBERG_CATALOG:-ais}"
                --conf "spark.executorEnv.ICEBERG_CATALOG_URI=\${ICEBERG_CATALOG_URI:-}"
                --conf "spark.executorEnv.ICEBERG_WAREHOUSE=\${ICEBERG_WAREHOUSE:-}"
                --conf "spark.executorEnv.CASSANDRA_HOST=\${CASSANDRA_HOST:-}"
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
                --conf "spark.executorEnv.DIRECTION=\${DIRECTION:-}"
                --conf "spark.executorEnv.ANCHOR_HOURS=\${ANCHOR_HOURS:-}"
                --conf "spark.executorEnv.PM25_TRIGGER_THRESHOLD=\${PM25_TRIGGER_THRESHOLD:-}"
                --conf "spark.executorEnv.SPARK_SMOKE_CHECK_ICEBERG=\${SPARK_SMOKE_CHECK_ICEBERG:-1}"
                --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                --conf "spark.sql.catalog.\${ICEBERG_CATALOG:-ais}=org.apache.iceberg.spark.SparkCatalog"
                --conf "spark.sql.catalog.\${ICEBERG_CATALOG:-ais}.type=hadoop"
                --conf "spark.sql.catalog.\${ICEBERG_CATALOG:-ais}.warehouse=\${ICEBERG_WAREHOUSE}"
              )

              if [ -n "\${SPARK_JARS_PACKAGES:-}" ]; then
                submit_args+=(--packages "\${SPARK_JARS_PACKAGES}")
              fi

              submit_args+=("local://\${JOB_FILE}")
              submit_args+=("\$@")
              exec /opt/spark/bin/spark-submit "\${submit_args[@]}"
YAML

if [ "$FOLLOW_LOGS" = "true" ]; then
  kubectl -n "$K8S_NAMESPACE" logs -f "job/${SUBMIT_JOB_NAME}" --all-containers=true || true
fi

if [ "$WAIT_FOR_COMPLETION" = "true" ]; then
  if kubectl -n "$K8S_NAMESPACE" wait --for=condition=complete "job/${SUBMIT_JOB_NAME}" --timeout="$KUBECTL_TIMEOUT"; then
    SPARK_APP_LABEL="$(sanitize_name "$SPARK_RUNTIME_APP_NAME")"
    DRIVER_PHASES="$(kubectl -n "$K8S_NAMESPACE" get pods -l "spark-role=driver,spark-app-name=${SPARK_APP_LABEL}" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}' 2>/dev/null || true)"
    if printf "%s\n" "$DRIVER_PHASES" | grep -q " Failed"; then
      echo "[ERROR] Spark driver pod failed for app ${SPARK_RUNTIME_APP_NAME}" >&2
      printf "%s\n" "$DRIVER_PHASES" >&2
      kubectl -n "$K8S_NAMESPACE" logs -l "spark-role=driver,spark-app-name=${SPARK_APP_LABEL}" --tail=200 || true
      exit 1
    fi
    echo "[OK] Spark submit job completed: ${SUBMIT_JOB_NAME}"
  else
    echo "[ERROR] Spark submit job did not complete cleanly: ${SUBMIT_JOB_NAME}" >&2
    kubectl -n "$K8S_NAMESPACE" get job "$SUBMIT_JOB_NAME" -o wide || true
    kubectl -n "$K8S_NAMESPACE" describe job "$SUBMIT_JOB_NAME" || true
    kubectl -n "$K8S_NAMESPACE" get pods -l "job-name=${SUBMIT_JOB_NAME}" -o wide || true
    exit 1
  fi
fi
