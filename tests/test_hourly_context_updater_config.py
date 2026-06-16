# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


# Kiem tra hourly context updater cronjob exists and is hourly not 30 seconds.
def test_hourly_context_updater_cronjob_exists_and_is_hourly_not_30_seconds():
    manifest = (ROOT / "deploy" / "k8s" / "hourly" / "ais-hourly-context-updater-cronjob.yaml").read_text(encoding="utf-8")

    assert "kind: CronJob" in manifest
    assert "name: ais-hourly-context-updater" in manifest
    assert 'schedule: "12 * * * *"' in manifest
    assert "30 seconds" not in manifest
    assert "*/30 * * * * *" not in manifest


# Kiem tra hourly context script orchestrates required era5 hysplit steps.
def test_hourly_context_script_orchestrates_required_era5_hysplit_steps():
    script = (ROOT / "scripts" / "run_hourly_context_update.sh").read_text(encoding="utf-8")
    required = [
        "era5_ingest.py",
        "--dataset-type surface",
        "--dataset-type pressure_levels",
        "era5_files_streaming.py",
        "era5_surface_hanoi_silver.py",
        "era5_pressure_levels_to_arl.py",
        "hysplit_trajectory_run.py",
        "hysplit_trajectory_parse_silver.py",
        "hysplit_trajectory_cluster_silver.py",
        "trajectory_hourly_features_silver.py",
    ]

    for needle in required:
        assert needle in script
    assert re.search(r'START_DATE="\$\{ERA5_HOURLY_START_DATE', script)
    assert 'BASE_DATE="$(date -u +%F)"' in script
