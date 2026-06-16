# File nay: DAG Airflow dieu phoi ingest, Spark, ML, visualization hoac maintenance.
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import ensure_iceberg_tables_command, spark_submit_command


DAG_ID = "ais_pm25_k8s_compute"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "do_xcom_push": False,
    "execution_timeout": timedelta(hours=3),
}

START_DATE_TEMPLATE = "{{ dag_run.conf.get('start_date', ds) if dag_run and dag_run.conf else ds }}"
END_DATE_TEMPLATE = "{{ dag_run.conf.get('end_date', ds) if dag_run and dag_run.conf else ds }}"
FULL_REFRESH_TEMPLATE = "{{ dag_run.conf.get('full_refresh', 0) if dag_run and dag_run.conf else 0 }}"
DATASET_VERSION_TEMPLATE = "{{ dag_run.conf.get('dataset_version', 'hanoi_pm25_v1') if dag_run and dag_run.conf else 'hanoi_pm25_v1' }}"
FEATURE_VERSION_TEMPLATE = "{{ dag_run.conf.get('feature_version', 'hanoi_pm25_core_v1') if dag_run and dag_run.conf else 'hanoi_pm25_core_v1' }}"
FEATURE_SET_NAME_TEMPLATE = "{{ dag_run.conf.get('feature_set_name', 'hanoi_pm25_core_v1') if dag_run and dag_run.conf else 'hanoi_pm25_core_v1' }}"
LOCATION_ID_TEMPLATE = "{{ dag_run.conf.get('location_id', 'hanoi') if dag_run and dag_run.conf else 'hanoi' }}"
LOCATION_NAME_TEMPLATE = "{{ dag_run.conf.get('location_name', 'Hanoi') if dag_run and dag_run.conf else 'Hanoi' }}"


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ais", "pm25", "k8s"],
    description="PM2.5 compute chain on Spark-on-Kubernetes: master gold -> serving features gold (no compose spark target).",
) as dag:
    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    # Assumes upstream pipeline produced `ais.features.hanoi_pm25_master_hourly_gold`.
    # (e.g. Tier-2 + master gold DAG). This DAG focuses on the K8s compute steps.
    hanoi_pm25_serving_features_gold = BashOperator(
        task_id="hanoi_pm25_serving_features_gold",
        bash_command=spark_submit_command(
            app_name="HanoiPM25ServingFeaturesGold",
            job_file="/opt/spark-jobs/hanoi_pm25_serving_features_gold.py",
            extra_args=(
                f"--start-date {START_DATE_TEMPLATE} "
                f"--end-date {END_DATE_TEMPLATE} "
                f"--full-refresh {FULL_REFRESH_TEMPLATE} "
                f"--feature-version {FEATURE_VERSION_TEMPLATE} "
                f"--feature-set-name {FEATURE_SET_NAME_TEMPLATE} "
                f"--dataset-version {DATASET_VERSION_TEMPLATE} "
                f"--location-id {LOCATION_ID_TEMPLATE} "
                f"--location-name {LOCATION_NAME_TEMPLATE}"
            ),
        ),
    )

    # Optional: training on Kubernetes (CPU/memory heavier than inference). This task only writes
    # model artifacts + run metadata; promotion to production registry is a separate explicit action.
    train_hanoi_pm25 = BashOperator(
        task_id="train_hanoi_pm25",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/ais\n"
            "kubectl -n ais apply -f deploy/k8s/ml/pm25-train-job.yaml\n"
            "kubectl -n ais wait --for=condition=complete --timeout=2h job/pm25-train\n"
            "kubectl -n ais logs -f job/pm25-train"
        ),
    )

    ensure_iceberg_tables >> hanoi_pm25_serving_features_gold
    hanoi_pm25_serving_features_gold >> train_hanoi_pm25
