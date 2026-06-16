from __future__ import annotations

import os

SPARK_VERSION = os.getenv("SPARK_VERSION", "3.5.3")
HADOOP_CLIENT_VERSION = os.getenv("HADOOP_CLIENT_VERSION", "3.3.4")
ICEBERG_VERSION = os.getenv("ICEBERG_VERSION", "1.6.1")
CASSANDRA_CONNECTOR_VERSION = os.getenv("CASSANDRA_CONNECTOR_VERSION", "3.5.1")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", f"{HDFS_NAMENODE}/warehouse/iceberg")

SPARK_COMMON_CONF = (
    f" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION},"
    f"org.apache.hadoop:hadoop-client:{HADOOP_CLIENT_VERSION},"
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
    f" --conf \"spark.hadoop.fs.defaultFS={HDFS_NAMENODE}\""
    " --conf \"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\""
    " --conf \"spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog\""
    " --conf \"spark.sql.catalog.ais.type=hadoop\""
    f" --conf \"spark.sql.catalog.ais.warehouse={ICEBERG_WAREHOUSE}\""
    " --conf \"spark.sql.adaptive.enabled=true\""
    " --conf \"spark.driver.memory=1g\""
    " --conf \"spark.executor.memory=1g\""
)

CASSANDRA_CONF = (
    f" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION},"
    f"org.apache.hadoop:hadoop-client:{HADOOP_CLIENT_VERSION},"
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
    f"com.datastax.spark:spark-cassandra-connector_2.12:{CASSANDRA_CONNECTOR_VERSION}"
    f" --conf \"spark.hadoop.fs.defaultFS={HDFS_NAMENODE}\""
    " --conf \"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\""
    " --conf \"spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog\""
    " --conf \"spark.sql.catalog.ais.type=hadoop\""
    f" --conf \"spark.sql.catalog.ais.warehouse={ICEBERG_WAREHOUSE}\""
    " --conf \"spark.cassandra.connection.host=cassandra\""
    " --conf \"spark.cassandra.connection.port=9042\""
    " --conf \"spark.sql.adaptive.enabled=true\""
    " --conf \"spark.driver.memory=1g\""
    " --conf \"spark.executor.memory=1g\""
)

LOOKBACK_DAYS_TEMPLATE = "{{ dag_run.conf.get('lookback_days', 7) if dag_run and dag_run.conf else 7 }}"
MAIAC_LOOKBACK_DAYS_TEMPLATE = "{{ dag_run.conf.get('maiac_lookback_days', dag_run.conf.get('lookback_days', 30)) if dag_run and dag_run.conf else 30 }}"
COMPOSE_PROJECT_NAME_TEMPLATE = "${COMPOSE_PROJECT_NAME:-atmospheric_intelligence_sys---ais}"



_STREAM_CHECKPOINT_BASE = {
    "weather_streaming.py": "weather_history",
    "openaq_hourly_streaming.py": "openaq_hourly",
    "sentinel5p_summary_streaming.py": "sentinel5p_summary",
    "maiac_summary_streaming.py": "maiac_summary",
    "era5_files_streaming.py": "era5_files",
}


def _stream_checkpoint_env(job_file: str, extra_args: str) -> str:
    """Return CHECKPOINT_PATH env for finite bootstrap/backfill streams.

    Spark Structured Streaming ignores startingOffsets once a checkpoint exists.
    Batch/bootstrap runs therefore must not share the realtime checkpoint path.
    """
    basename = job_file.rstrip("/").split("/")[-1]
    checkpoint_name = _STREAM_CHECKPOINT_BASE.get(basename)
    if not checkpoint_name:
        return ""
    finite = "--stop-after-batch" in extra_args or os.getenv("STOP_AFTER_BATCH", "").lower() in {"1", "true", "yes"}
    if not finite:
        return ""
    hdfs = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000").rstrip("/")
    return f"CHECKPOINT_PATH={hdfs}/checkpoints/{checkpoint_name}/bootstrap/${{AIRFLOW_CTX_DAG_RUN_ID:-manual}} "

def spark_submit_command(
    app_name: str,
    job_file: str,
    *,
    extra_args: str = "",
    starting_offsets: str | None = None,
    with_cassandra: bool = False,
    detached: bool = False,
) -> str:
    runtime = os.getenv("AIS_SPARK_RUNTIME", "compose").strip().lower()
    if runtime == "k8s":
        cleaned_args = extra_args.strip()
        suffix = f" {cleaned_args}" if cleaned_args else ""
        env_prefix = f"KAFKA_STARTING_OFFSETS={starting_offsets} " if starting_offsets else ""
        env_prefix += _stream_checkpoint_env(job_file, cleaned_args)
        if "--stop-after-batch" in cleaned_args:
            env_prefix += "STOP_AFTER_BATCH=true "
        job_type = _k8s_job_type_for_file(job_file, extra_args=cleaned_args, with_cassandra=with_cassandra)
        return (
            "set -euo pipefail\n"
            "cd /opt/ais\n"
            f"{env_prefix}bash ./scripts/submit_spark_k8s.sh {job_type}{suffix}"
        )

    conf = CASSANDRA_CONF if with_cassandra else SPARK_COMMON_CONF
    detach_flag = "-d " if detached else ""
    cleaned_args = extra_args.strip()
    suffix = f" {cleaned_args}" if cleaned_args else ""

    env_prefix = f"KAFKA_STARTING_OFFSETS={starting_offsets} " if starting_offsets else ""
    env_prefix += _stream_checkpoint_env(job_file, cleaned_args)
    if "--stop-after-batch" in cleaned_args:
        env_prefix += "STOP_AFTER_BATCH=true "

    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"{env_prefix}docker exec {detach_flag}spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --name \"{app_name}\"{conf} {job_file}{suffix}"
    )


def _k8s_job_type_for_file(job_file: str, *, extra_args: str = "", with_cassandra: bool = False) -> str:
    if with_cassandra:
        dataset = extra_args.strip().split()[0] if extra_args.strip() else ""
        if dataset in {"weather", "openaq"}:
            return f"cassandra-{dataset}"
        raise ValueError(f"No Spark-on-K8s Cassandra job mapping for args: {extra_args!r}")

    mapping = {
        "weather_streaming.py": "weather",
        "openaq_hourly_streaming.py": "openaq",
        "sentinel5p_summary_streaming.py": "sentinel5p",
        "maiac_summary_streaming.py": "maiac",
        "era5_files_streaming.py": "era5-files",
        "hanoi_openaq_silver.py": "hanoi-openaq-silver",
        "hanoi_weather_surface_proxy_silver.py": "hanoi-weather-silver",
        "era5_surface_hanoi_silver.py": "era5-surface-hanoi-silver",
        "era5_pressure_levels_to_arl.py": "era5-pressure-arl",
        "hysplit_trajectory_run.py": "hysplit-run",
        "hysplit_trajectory_parse_silver.py": "hysplit-parse",
        "hysplit_trajectory_cluster_silver.py": "hysplit-cluster",
        "sentinel5p_hanoi_silver.py": "sentinel5p-hanoi-silver",
        "openaq_spatial_gradient_silver.py": "openaq-gradient",
        "sentinel5p_grid_silver.py": "s5p-grid-silver",
        "trajectory_path_sampling_silver.py": "traj-path-sampling",
        "trajectory_hourly_features_silver.py": "traj-hourly-features",
        "maiac_hanoi_silver.py": "maiac-hanoi-silver",
        "hanoi_pm25_master_features_gold.py": "hanoi-master-features-gold",
        "hanoi_pm25_training_dataset_gold.py": "hanoi-training-dataset-gold",
        "hanoi_pm25_serving_features_gold.py": "hanoi-serving-features-gold",
        "pm25_serving_features_to_cassandra.py": "pm25-features-cassandra",
        "visualization_pm25_heatmap_grid_gold.py": "visualization-heatmap-grid",
        "visualization_backward_trajectory_paths_gold.py": "visualization-backward-trajectories",
        "visualization_forward_plume_probability_gold.py": "visualization-forward-plume",
        "visualization_forecast_dashboard_gold.py": "visualization-forecast-dashboard",
        "visualization_pm25_timeseries_gold.py": "visualization-pm25-timeseries",
        "visualization_source_attribution_gold.py": "visualization-source-attribution",
        "visualization_station_observations_gold.py": "visualization-station-observations",
        "export_visualization_cache.py": "visualization-export-cache",
        "visualization_quality_checks.py": "visualization-quality-checks",
        "train_hanoi_pm25.py": "hanoi-train-baseline",
        "ensure_iceberg_tables.py": "ensure-iceberg",
        "iceberg_maintenance.py": "maintenance-iceberg",
        "reconcile_iceberg_cassandra.py": "reconcile-serving",
    }
    basename = job_file.rstrip("/").split("/")[-1]
    try:
        return mapping[basename]
    except KeyError as exc:
        raise ValueError(f"No Spark-on-K8s job mapping for {job_file}") from exc


def spark_cassandra_command(dataset: str) -> str:
    return spark_submit_command(
        app_name=f"IcebergToCassandra_{dataset.capitalize()}",
        job_file="/opt/spark-jobs/iceberg_to_cassandra.py",
        extra_args=dataset,
        with_cassandra=True,
    )


def ensure_topics_command() -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        "bash ./scripts/create_topics.sh "
    )


def ensure_iceberg_tables_command() -> str:
    return spark_submit_command(
        app_name="AIS_EnsureIcebergTables",
        job_file="/opt/spark-jobs/ensure_iceberg_tables.py",
    )


def ensure_cassandra_schema_command() -> str:
    return (
        "set -euo pipefail\n"
        "docker exec cassandra cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS ais_serving WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"\n"
        "docker exec cassandra cqlsh -e \"CREATE TABLE IF NOT EXISTS ais_serving.weather_hourly_by_province_day (province text, day text, event_time timestamp, event_id text, query_date text, location_name text, lat double, lon double, temp_c double, temp_f double, humidity int, wind_kph double, wind_degree int, wind_dir text, precip_mm double, condition_text text, source text, ingest_time text, PRIMARY KEY ((province, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);\"\n"
        "docker exec cassandra cqlsh -e \"CREATE TABLE IF NOT EXISTS ais_serving.openaq_hourly_by_city_parameter_day (city text, parameter text, day text, event_time timestamp, event_id text, location_id bigint, location_name text, provider text, sensor_id bigint, unit text, value double, min double, max double, sd double, coverage_pct double, source text, ingest_time text, PRIMARY KEY ((city, parameter, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);\"\n"
        "bash /opt/ais/scripts/ensure_cassandra_online_schema.sh"
    )


def compose_ingest_command(
    service: str,
    script_name: str,
    *,
    lookback_days_template: str = LOOKBACK_DAYS_TEMPLATE,
) -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        "for i in $(seq 1 36); do\n"
        "  if docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then\n"
        "    echo 'Kafka is ready'\n"
        "    break\n"
        "  fi\n"
        "  if [ \"$i\" -eq 36 ]; then\n"
        "    echo 'Kafka is not ready after 180 seconds' >&2\n"
        "    exit 1\n"
        "  fi\n"
        "  echo \"Waiting for Kafka... attempt $i/36\"\n"
        "  sleep 5\n"
        "done\n"
        f"docker compose -p {COMPOSE_PROJECT_NAME_TEMPLATE} run --rm --no-deps -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS={lookback_days_template} -e KAFKA_CONNECT_MAX_RETRIES=36 -e KAFKA_CONNECT_RETRY_DELAY=5 {service} python -u {script_name}"
    )


def ensure_streaming_job_command(job_type: str) -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"bash scripts/airflow/ensure_stream_job.sh {job_type}"
    )


def kafka_lag_check_command(group_id: str, topic: str, max_lag: int = 50000) -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"bash scripts/airflow/check_kafka_lag.sh {group_id} {topic} {max_lag}"
    )


def operational_health_check_command() -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        "python scripts/check_operational_health.py "
        "--visualization-url ${VIS_API_BASE_URL:-http://visualization-api:8080} "
        "--forecast-url ${PM25_API_BASE_URL:-http://pm25-api:8080} "
        "--webhdfs-url ${HDFS_WEBHDFS_BASE:-http://namenode:9870/webhdfs/v1}"
    )


def reconcile_serving_command(lookback_hours: int = 24, tolerance: float = 0.95) -> str:
    return spark_submit_command(
        app_name="AIS_ReconcileServing",
        job_file="/opt/spark-jobs/reconcile_iceberg_cassandra.py",
        extra_args=f"--lookback-hours {lookback_hours} --tolerance {tolerance}",
        with_cassandra=True,
    )


def iceberg_maintenance_command(retention_hours: int = 168) -> str:
    return spark_submit_command(
        app_name="AIS_IcebergMaintenance",
        job_file="/opt/spark-jobs/iceberg_maintenance.py",
        extra_args=f"--retention-hours {retention_hours}",
    )


def visualization_spark_command(job_type: str, *, dry_run: str = "0", extra_args: str = "") -> str:
    suffix = f" {extra_args.strip()}" if extra_args.strip() else ""
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"DRY_RUN={dry_run} bash ./scripts/submit_spark_k8s.sh {job_type}{suffix}"
    )
