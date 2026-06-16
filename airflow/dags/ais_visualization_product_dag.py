# File nay: DAG Airflow dieu phoi ingest, Spark, ML, visualization hoac maintenance.
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import ensure_iceberg_tables_command, visualization_spark_command


DAG_ID = "ais_visualization_product"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "do_xcom_push": False,
}

START_DATE_TEMPLATE = "{{ dag_run.conf.get('start_date', ds) if dag_run and dag_run.conf else ds }}"
END_DATE_TEMPLATE = "{{ dag_run.conf.get('end_date', ds) if dag_run and dag_run.conf else ds }}"
BASE_TIME_TEMPLATE = "{{ dag_run.conf.get('base_time', '') if dag_run and dag_run.conf else '' }}"
HORIZONS_TEMPLATE = "{{ dag_run.conf.get('horizons', '0,6,12,24') if dag_run and dag_run.conf else '0,6,12,24' }}"
PRODUCT_VERSION_TEMPLATE = "{{ dag_run.conf.get('product_version', 'windy_v1') if dag_run and dag_run.conf else 'windy_v1' }}"
DRY_RUN_TEMPLATE = "{{ dag_run.conf.get('dry_run', 0) if dag_run and dag_run.conf else 0 }}"


COMMON_ARGS = (
    f"--start-date {START_DATE_TEMPLATE} "
    f"--end-date {END_DATE_TEMPLATE} "
    f"--horizons {HORIZONS_TEMPLATE} "
    f"--product-version {PRODUCT_VERSION_TEMPLATE}"
)


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ais", "visualization", "product", "k8s"],
    description="Refresh TODO4 visualization gold tables and exported API cache on Spark-on-Kubernetes.",
) as dag:
    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    forecast_dashboard = BashOperator(
        task_id="visualization_forecast_dashboard",
        bash_command=visualization_spark_command("visualization-forecast-dashboard", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    pm25_timeseries = BashOperator(
        task_id="visualization_pm25_timeseries",
        bash_command=visualization_spark_command("visualization-pm25-timeseries", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    station_observations = BashOperator(
        task_id="visualization_station_observations",
        bash_command=visualization_spark_command("visualization-station-observations", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    backward_trajectories = BashOperator(
        task_id="visualization_backward_trajectories",
        bash_command=visualization_spark_command("visualization-backward-trajectories", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    source_attribution = BashOperator(
        task_id="visualization_source_attribution",
        bash_command=visualization_spark_command("visualization-source-attribution", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    forward_plume = BashOperator(
        task_id="visualization_forward_plume_optional",
        bash_command=visualization_spark_command("visualization-forward-plume", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    heatmap_grid = BashOperator(
        task_id="visualization_heatmap_grid",
        bash_command=visualization_spark_command(
            "visualization-heatmap-grid",
            dry_run=DRY_RUN_TEMPLATE,
            extra_args=f"{COMMON_ARGS} --base-time '{BASE_TIME_TEMPLATE}'",
        ),
    )

    export_cache = BashOperator(
        task_id="visualization_export_cache",
        bash_command=visualization_spark_command("visualization-export-cache", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    quality_checks = BashOperator(
        task_id="visualization_quality_checks",
        bash_command=visualization_spark_command("visualization-quality-checks", dry_run=DRY_RUN_TEMPLATE, extra_args=COMMON_ARGS),
    )

    api_ready_check = BashOperator(
        task_id="visualization_api_ready_check",
        bash_command=(
            "set -euo pipefail\n"
            "python /opt/ais/scripts/check_visualization_serving.py "
            "--base-url ${VIS_API_BASE_URL:-http://visualization-api.ais.svc.cluster.local}"
        ),
    )

    ensure_iceberg_tables >> [forecast_dashboard, pm25_timeseries, station_observations, backward_trajectories, source_attribution, forward_plume]
    [forecast_dashboard, pm25_timeseries, station_observations, backward_trajectories, source_attribution, forward_plume] >> heatmap_grid
    heatmap_grid >> export_cache >> quality_checks >> api_ready_check
