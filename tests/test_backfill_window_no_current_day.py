import re
from pathlib import Path

from ais_architecture_logic import build_historical_backfill_env


ROOT = Path(__file__).resolve().parents[1]


def test_backfill_env_uses_historical_end_date_not_current_day():
    env = build_historical_backfill_env("2026-05-29", "2026-05-31")

    assert env["WINDOW_END_UTC"] == "2026-05-30T23:59:59Z"
    assert env["END_DATE"] == "2026-05-30"
    assert env["REALTIME_SIMULATION_DATE"] == "2026-05-31"
    assert "2026-05-31T23:59:59Z" not in env.values()


def test_todo4_backfill_commands_do_not_use_raw_end_date_as_batch_window_end():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert "$historicalBackfillEndDate" in script
    assert "$resolvedEndDate = $historicalBackfillEndDate" in script
    dangerous = re.compile(r"WINDOW_END_UTC=.*\$EndDate.*23:59:59", re.IGNORECASE)
    assert not dangerous.search(script)
    assert "WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z'" in script


def test_training_dataset_is_submitted_with_historical_resolved_end_date():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert 'Submit-SparkK8s "hanoi-training-dataset-gold"' in script
    assert "Submit-SparkK8s" in script
    assert "END_DATE='$resolvedEndDate'" in script or '$dateEnv = "START_DATE=\'$resolvedStartDate\' END_DATE=\'$resolvedEndDate\'' in script
