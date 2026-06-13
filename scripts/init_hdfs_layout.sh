#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
HDFS_URI="${HDFS_URI:-}"

run_hdfs() {
    if [ -n "$HDFS_URI" ]; then
        docker exec "$NAMENODE_CONTAINER" hdfs dfs -fs "$HDFS_URI" "$@"
    else
        docker exec "$NAMENODE_CONTAINER" hdfs dfs "$@"
    fi
}

run_hdfs_admin() {
    if [ -n "$HDFS_URI" ]; then
        docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -fs "$HDFS_URI" "$@"
    else
        docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin "$@"
    fi
}

echo "=== Check HDFS safemode ==="
SAFE_MODE_OUTPUT="$(run_hdfs_admin -safemode get 2>/dev/null || true)"
echo "$SAFE_MODE_OUTPUT"

if echo "$SAFE_MODE_OUTPUT" | grep -qi "ON"; then
    echo "=== Leave HDFS safemode ==="
    run_hdfs_admin -safemode leave || true
fi

echo "=== Create AIS HDFS layout ==="

DIRS=(
    "/warehouse"
    "/warehouse/iceberg"
    "/warehouse/iceberg/weather"
    "/warehouse/iceberg/air_quality"
    "/warehouse/iceberg/satellite"
    "/warehouse/iceberg/features"
    "/warehouse/iceberg/models"
    "/warehouse/iceberg/predictions"
    "/warehouse/iceberg/trajectory"
    "/warehouse/iceberg/visualization"

    "/checkpoints"
    "/checkpoints/weather_history"
    "/checkpoints/openaq_hourly"
    "/checkpoints/sentinel5p_summary"
    "/checkpoints/maiac_summary"
    "/checkpoints/era5_files"
    "/checkpoints/hanoi_openaq_silver"
    "/checkpoints/hanoi_weather_surface_proxy_silver"
    "/checkpoints/hanoi_pm25_master_features_gold"
    "/checkpoints/hanoi_pm25_serving_features_gold"
    "/checkpoints/pm25_features_cassandra"

    "/tmp"
    "/tmp/spark"

    "/logs"
    "/logs/spark"
    "/logs/ingest"

    "/raw"
    "/raw/era5"
    "/raw/sentinel5p"
    "/raw/maiac"
    "/raw/hysplit"

    "/models"
    "/visualization_cache"
)

for dir in "${DIRS[@]}"; do
    echo "[MKDIR] $dir"
    run_hdfs -mkdir -p "$dir"
done

echo "=== Set HDFS permissions for local development ==="

# Local/demo mode: để rộng quyền cho Spark/Airflow/Ingest dễ ghi.
run_hdfs -chmod -R 777 /warehouse
run_hdfs -chmod -R 777 /checkpoints
run_hdfs -chmod -R 777 /tmp/spark
run_hdfs -chmod -R 777 /logs
run_hdfs -chmod -R 777 /raw
run_hdfs -chmod -R 777 /models
run_hdfs -chmod -R 777 /visualization_cache

echo "=== Current AIS HDFS layout ==="
run_hdfs -ls / || true
run_hdfs -ls /warehouse || true
run_hdfs -ls /warehouse/iceberg || true
run_hdfs -ls /checkpoints || true
run_hdfs -ls /raw || true
run_hdfs -ls /models || true
run_hdfs -ls /visualization_cache || true
run_hdfs -ls /logs || true

echo "=== HDFS layout initialized successfully ==="
