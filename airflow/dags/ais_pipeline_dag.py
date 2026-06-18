# Airflow DAG for historical AIS batch processing and serving refresh.
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import (
    LOOKBACK_DAYS_TEMPLATE,
    MAIAC_LOOKBACK_DAYS_TEMPLATE,
    compose_ingest_command,
    ensure_cassandra_schema_command,
    ensure_iceberg_tables_command,
    ensure_topics_command,
)

DAG_ID = "ais_batch_orchestration"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "do_xcom_push": False,
    "execution_timeout": timedelta(hours=3),
}

START_DATE_TEMPLATE = "{{ dag_run.conf.get('start_date', ds) if dag_run and dag_run.conf else ds }}"
END_DATE_TEMPLATE = "{{ dag_run.conf.get('end_date', ds) if dag_run and dag_run.conf else ds }}"
BASE_TIME_TEMPLATE = "{{ dag_run.conf.get('base_time', '') if dag_run and dag_run.conf else '' }}"
FULL_REFRESH_TEMPLATE = "{{ dag_run.conf.get('full_refresh', 1) if dag_run and dag_run.conf else 1 }}"
RUN_INGEST_TEMPLATE = "{{ dag_run.conf.get('run_ingest', false) if dag_run and dag_run.conf else false }}"
VIS_TRAJECTORY_FALLBACK_TEMPLATE = "{{ dag_run.conf.get('use_historical_trajectory_fallback', true) if dag_run and dag_run.conf else true }}"


def k8s_submit_command(job_type: str, *, extra_env: str = "", include_date_range: bool = True) -> str:
    date_env = (
        f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} "
        if include_date_range
        else ""
    )
    env_prefix = f"{date_env}FULL_REFRESH={FULL_REFRESH_TEMPLATE} {extra_env}".strip()
    env_prefix = f"{env_prefix} " if env_prefix else ""
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"{env_prefix}bash scripts/submit_spark_k8s.sh {job_type}"
    )


def optional_ingest_command(service: str, script_name: str, *, lookback_days_template: str = LOOKBACK_DAYS_TEMPLATE) -> str:
    ingest_command = compose_ingest_command(service, script_name, lookback_days_template=lookback_days_template)
    return (
        "set -euo pipefail\n"
        f"if [ \"{RUN_INGEST_TEMPLATE}\" = \"true\" ] || [ \"{RUN_INGEST_TEMPLATE}\" = \"True\" ] || [ \"{RUN_INGEST_TEMPLATE}\" = \"1\" ]; then\n"
        f"{ingest_command}\n"
        "else\n"
        f"  echo '[INFO] run_ingest=false; skipping {service} backfill ingest and using existing Kafka/Iceberg inputs.'\n"
        "fi"
    )


def bronze_source_command(source: str) -> str:
    return k8s_submit_command(
        "bronze-pipeline",
        extra_env=(
            f"PIPELINE_SOURCES={source} "
            "PIPELINE_CONTINUE_ON_ERROR=false "
            "KAFKA_STARTING_OFFSETS=earliest "
            "BRONZE_CHECKPOINT_RUN_ID=${AIRFLOW_CTX_DAG_RUN_ID:-airflow_batch}"
        ),
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 13),
    schedule=timedelta(days=7),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ais", "bootstrap", "historical", "airflow"],
    description=(
        "Batch processing orchestration aligned with run_todo4: optional ingest, "
        "K8s Spark bronze processing, PM2.5 feature build, Cassandra refresh, and visualization cache export."
    ),
) as dag:
    ensure_kafka_topics = BashOperator(
        task_id="ensure_kafka_topics",
        bash_command=ensure_topics_command(),
    )

    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    ensure_cassandra_schema = BashOperator(
        task_id="ensure_cassandra_schema",
        bash_command=ensure_cassandra_schema_command(),
    )

    run_weather_ingest = BashOperator(
        task_id="run_weather_ingest",
        bash_command=optional_ingest_command(
            "ingest",
            "ingest_weather.py",
            lookback_days_template=LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    run_openaq_ingest = BashOperator(
        task_id="run_openaq_ingest",
        bash_command=optional_ingest_command(
            "openaq-ingest",
            "openaq_ingest.py",
            lookback_days_template=LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    run_sentinel5p_ingest = BashOperator(
        task_id="run_sentinel5p_ingest",
        bash_command=optional_ingest_command(
            "sentinel5p-ingest",
            "sentinel5p_ingest.py",
            lookback_days_template=LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    run_maiac_ingest = BashOperator(
        task_id="run_maiac_ingest",
        bash_command=optional_ingest_command(
            "maiac-ingest",
            "maiac_ingest.py",
            lookback_days_template=MAIAC_LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    process_weather_to_iceberg = BashOperator(
        task_id="process_weather_to_iceberg",
        bash_command=bronze_source_command("weather"),
    )

    process_openaq_to_iceberg = BashOperator(
        task_id="process_openaq_to_iceberg",
        bash_command=bronze_source_command("openaq"),
    )

    process_sentinel5p_to_iceberg = BashOperator(
        task_id="process_sentinel5p_to_iceberg",
        bash_command=bronze_source_command("sentinel5p"),
    )

    process_maiac_to_iceberg = BashOperator(
        task_id="process_maiac_to_iceberg",
        bash_command=bronze_source_command("maiac"),
    )

    build_pm25_feature_tables = BashOperator(
        task_id="build_pm25_feature_tables",
        bash_command=k8s_submit_command("pm25-feature-pipeline", extra_env=f"ASOF_TIME={BASE_TIME_TEMPLATE} "),
    )

    refresh_visualization_cache = BashOperator(
        task_id="refresh_visualization_cache",
        bash_command=k8s_submit_command(
            "visualization-pipeline",
            extra_env=(
                f"BASE_TIME={BASE_TIME_TEMPLATE} "
                "PIPELINE_LAYERS=heatmap,backward_trajectories,forward_plume,source_attribution,stations,forecast,timeseries "
                "EXPORT_CACHE=true "
                f"VIS_TRAJECTORY_HISTORICAL_FALLBACK={VIS_TRAJECTORY_FALLBACK_TEMPLATE} "
            ),
        ),
    )

    load_weather_cassandra = BashOperator(
        task_id="load_weather_cassandra",
        bash_command=k8s_submit_command("cassandra-weather"),
    )

    load_openaq_cassandra = BashOperator(
        task_id="load_openaq_cassandra",
        bash_command=k8s_submit_command("cassandra-openaq"),
    )

    bootstrap_done = BashOperator(
        task_id="bootstrap_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'AIS batch processing complete: Iceberg, Cassandra serving, and visualization cache are refreshed.'"
        ),
    )

    ensure_kafka_topics >> [
        run_weather_ingest,
        run_openaq_ingest,
        run_sentinel5p_ingest,
        run_maiac_ingest,
    ]

    ensure_iceberg_tables >> [
        process_weather_to_iceberg,
        process_openaq_to_iceberg,
        process_sentinel5p_to_iceberg,
        process_maiac_to_iceberg,
    ]

    run_weather_ingest >> process_weather_to_iceberg
    run_openaq_ingest >> process_openaq_to_iceberg
    run_sentinel5p_ingest >> process_sentinel5p_to_iceberg
    run_maiac_ingest >> process_maiac_to_iceberg

    [
        process_weather_to_iceberg,
        process_openaq_to_iceberg,
        process_sentinel5p_to_iceberg,
        process_maiac_to_iceberg,
    ] >> build_pm25_feature_tables

    [process_weather_to_iceberg, process_openaq_to_iceberg] >> ensure_cassandra_schema
    ensure_cassandra_schema >> [load_weather_cassandra, load_openaq_cassandra]

    [
        build_pm25_feature_tables,
        load_weather_cassandra,
        load_openaq_cassandra,
    ] >> refresh_visualization_cache >> bootstrap_done
