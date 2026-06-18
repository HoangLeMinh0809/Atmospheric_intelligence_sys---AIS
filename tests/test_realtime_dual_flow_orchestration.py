# File nay: test bao ve contract du lieu, realtime flow, serving hoac orchestration.
from pathlib import Path

from ais_architecture_logic import expected_todo4_online_order


ROOT = Path(__file__).resolve().parents[1]


# Kiem tra index.
def _index(script: str, needle: str) -> int:
    pos = script.find(needle)
    assert pos >= 0, f"Missing orchestration marker: {needle}"
    return pos


# Kiem tra expected logical order contract.
def test_expected_logical_order_contract():
    assert expected_todo4_online_order() == [
        "historical backfill",
        "historical bronze",
        "historical silver/gold",
        "train model",
        "promote model",
        "start realtime ingest",
        "start streaming kafka to bronze",
        "start hourly era5/hysplit updater",
        "build online feature state",
        "run realtime prediction",
        "api/ui latest reads cassandra",
    ]


# Kiem tra todo4 realtime dual flow order is after historical training.
def test_todo4_realtime_dual_flow_order_is_after_historical_training():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert _index(script, "Step \"5) Backfill source data to Kafka\"") < _index(script, "Step \"6) Catch Kafka bronze topics into Iceberg\"")
    assert _index(script, "Step \"10) Train PM2.5 models\"") < _index(script, "Step \"11) Promote latest models to production registry\"")
    assert _index(script, "Step \"16) Start near-realtime Weather/OpenAQ new-data loops\"") < _index(script, "Step \"17) Start Spark streaming Kafka to Bronze for realtime audit\"")
    assert _index(script, "Step \"17) Start Spark streaming Kafka to Bronze for realtime audit\"") < _index(script, "Step \"20) Build online feature state and run realtime prediction\"")
    assert _index(script, "kubectl apply -f deploy/k8s/ml/online-pm25-features-cronjob.yaml") < _index(script, "            Start-OnlineFeatureBootstrapCycle")


# Kiem tra streaming to bronze and online path both exist.
def test_streaming_to_bronze_and_online_path_both_exist():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert "Start-RealtimeBronzeStreaming" in script
    assert "Test-RealtimeBronzeStreamingActive" in script
    assert "submit_spark.sh openaq" in script
    assert "submit_spark.sh weather" in script
    assert "Start-OnlineFeatureBootstrapCycle" in script
    assert "pm25-predict-job.yaml" in script
    assert "FEATURE_SOURCE = \"cassandra\"" in script


# Kiem tra resume mode khong duoc xoa pod/job luc moi vao script.
def test_resume_mode_skips_initial_runtime_cleanup():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert "$resumeMode = $ResumeFromStep -gt 1" in script
    assert "Resume mode detected; preserving existing pods/jobs and skipping initial runtime cleanup." in script
    assert _index(script, "if ($resumeMode) {") < _index(script, "elseif (-not $SkipInitialRuntimeCleanup) {")
    assert """if ($resumeMode) {
    Write-Host "[INFO] Resume mode detected; preserving existing pods/jobs and skipping initial runtime cleanup." -ForegroundColor Yellow
}
elseif (-not $SkipInitialRuntimeCleanup) {
    Stop-ExistingRuntimeWorkloads
}""" in script


# Kiem tra khong bootstrap online ngay khi streaming Bronze dang active, neu chua ep override.
def test_online_bootstrap_is_guarded_while_realtime_streaming_is_active():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert '[switch]$ForceOnlineBootstrapWhileStreaming' in script
    assert "Concurrent online bootstrap while streaming" in script
    assert "if ($realtimeStreamingActive -and -not $ForceOnlineBootstrapWhileStreaming) {" in script
    assert "skip immediate online bootstrap to avoid overloading local Docker" in script
    assert _index(script, "$realtimeStreamingActive = Test-RealtimeBronzeStreamingActive") < _index(script, "            Start-OnlineFeatureBootstrapCycle")


# Kiem tra Spark Compose khong bi bat len trong core infra mac dinh khi da chay K8s.
def test_compose_spark_is_not_core_infra_for_k8s_first_run():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")
    start_core = script[script.index("function Start-CoreInfra") : script.index("function Start-RealtimeNewData")]

    assert "Test-NeedsComposeSparkInfra" in start_core
    assert 'Invoke-DockerCompose @("up", "-d", "--build", "spark-master", "spark-worker")' not in start_core
    assert "keeping spark-master/spark-worker stopped to save local RAM" in start_core
    assert 'if ($composeSparkNeeded) {\n        Ensure-ComposeSparkInfra\n    }' in start_core


# Kiem tra promote model khong con phu thuoc spark-master Compose.
def test_model_promotion_runs_on_k8s_not_compose_spark():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")
    promote = script[script.index("function Promote-LatestModels") : script.index("function Resolve-VisualizationBaseTime")]

    assert "Promote-LatestModelK8s" in script
    assert "ais-ml-runtime:local" in script
    assert "Promote latest model horizon=$horizon on Kubernetes" in promote
    assert "docker exec spark-master" not in promote
    assert "--model-run-id" in script
    assert "latest" in script
