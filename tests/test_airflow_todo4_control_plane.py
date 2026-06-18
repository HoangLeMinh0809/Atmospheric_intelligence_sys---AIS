# TODO4 Airflow regression checks.
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_todo4_starts_airflow_control_plane_and_runtime_dags():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert '[switch]$EnableAirflow = $true' in script
    assert 'Step "21) Start Airflow control plane and enable runtime DAGs"' in script
    assert 'Invoke-DockerCompose @("up", "--build", "airflow-init")' in script
    assert 'Invoke-DockerCompose @("up", "-d", "--force-recreate", "airflow-webserver", "airflow-scheduler", "airflow-triggerer")' in script
    assert '[switch]$ForceAirflowStreamingSupervision' in script
    assert 'Trigger-AirflowDag -DagId "ais_streaming_supervision"' in script
    assert 'keeping Airflow DAG ais_streaming_supervision paused to avoid local duplicate stream restarts' in script


def test_monitoring_ui_targets_batch_backfill_dag():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "AIRFLOW_DAG_ID: ais_batch_orchestration" in compose


def test_airflow_cassandra_schema_command_does_not_end_with_template_like_sh_path():
    dag_utils = (ROOT / "airflow" / "dags" / "ais_dag_utils.py").read_text(encoding="utf-8")

    assert 'bash /opt/ais/scripts/ensure_cassandra_online_schema.sh \\n' in dag_utils
