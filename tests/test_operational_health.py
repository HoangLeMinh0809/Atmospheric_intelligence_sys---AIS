# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from datetime import datetime, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = spec_from_file_location("check_operational_health", ROOT / "scripts" / "check_operational_health.py")
MODULE = module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


# Kiem tra age minutes accepts zulu and is non negative.
def test_age_minutes_accepts_zulu_and_is_non_negative():
    now = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)
    assert MODULE.age_minutes("2026-06-15T11:30:00Z", now=now) == 30
    assert MODULE.age_minutes("2026-06-15T12:30:00Z", now=now) == 0


# Kiem tra operational check covers readiness prediction and checkpoints.
def test_operational_check_covers_readiness_prediction_and_checkpoints():
    script = (ROOT / "scripts" / "check_operational_health.py").read_text(encoding="utf-8")
    assert "visualization_ready" in script
    assert "forecast_ready" in script
    assert "prediction_freshness" in script
    assert "checkpoint_freshness" in script


# Kiem tra airflow supervision runs operational check.
def test_airflow_supervision_runs_operational_check():
    dag = (ROOT / "airflow" / "dags" / "ais_streaming_supervision_dag.py").read_text(encoding="utf-8")
    assert 'task_id="check_operational_health"' in dag
    assert "] >> check_operational_health >> supervision_done" in dag
