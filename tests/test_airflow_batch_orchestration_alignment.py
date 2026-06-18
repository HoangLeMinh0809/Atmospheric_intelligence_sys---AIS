from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_batch_orchestration_keeps_dag_and_task_names_but_uses_k8s_processing_flow():
    source = (ROOT / "airflow" / "dags" / "ais_pipeline_dag.py").read_text(encoding="utf-8")

    assert 'DAG_ID = "ais_batch_orchestration"' in source
    for task_id in [
        "ensure_kafka_topics",
        "run_weather_ingest",
        "process_weather_to_iceberg",
        "load_weather_cassandra",
        "bootstrap_done",
    ]:
        assert f'task_id="{task_id}"' in source

    assert "bash scripts/submit_spark_k8s.sh {job_type}" in source
    for job_type in [
        "bronze-pipeline",
        "pm25-feature-pipeline",
        "visualization-pipeline",
        "cassandra-weather",
        "cassandra-openaq",
    ]:
        assert job_type in source


def test_batch_ingest_tasks_are_optional_by_default():
    source = (ROOT / "airflow" / "dags" / "ais_pipeline_dag.py").read_text(encoding="utf-8")

    assert "RUN_INGEST_TEMPLATE" in source
    assert "run_ingest=false; skipping" in source
    assert "dag_run.conf.get('run_ingest', false)" in source
