# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


# Kiem tra near realtime openaq weather cadence is seconds level.
def test_near_realtime_openaq_weather_cadence_is_seconds_level():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert '[int]$RealtimePollSeconds = 60' in script or '[int]$RealtimePollSeconds = 30' in script
    assert '$RealtimeProcessingTime = "30 seconds"' in script
    assert "Start-RealtimeNewData" in script


# Kiem tra era5 hysplit hourly cadence is not realtime seconds loop.
def test_era5_hysplit_hourly_cadence_is_not_realtime_seconds_loop():
    manifest = (ROOT / "deploy" / "k8s" / "hourly" / "ais-hourly-context-updater-cronjob.yaml").read_text(encoding="utf-8")

    assert 'schedule: "12 * * * *"' in manifest
    assert "30 seconds" not in manifest
    assert "PROCESSING_TIME" not in manifest


# Kiem tra s5p maiac are batch daily not in realtime loop.
def test_s5p_maiac_are_batch_daily_not_in_realtime_loop():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")
    realtime_section = script[script.index("function Start-RealtimeNewData") : script.index("function Start-RealtimeBronzeStreaming")]

    assert "sentinel5p" not in realtime_section.lower()
    assert "maiac" not in realtime_section.lower()
    assert "openaq-ingest" in realtime_section
    assert '"ingest", "openaq-ingest"' in realtime_section
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert 'command: ["python", "-u", "ingest_weather.py"]' in compose
