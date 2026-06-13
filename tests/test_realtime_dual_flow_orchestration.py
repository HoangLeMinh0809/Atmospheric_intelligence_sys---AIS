from pathlib import Path

from ais_architecture_logic import expected_todo4_online_order


ROOT = Path(__file__).resolve().parents[1]


def _index(script: str, needle: str) -> int:
    pos = script.find(needle)
    assert pos >= 0, f"Missing orchestration marker: {needle}"
    return pos


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


def test_todo4_realtime_dual_flow_order_is_after_historical_training():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert _index(script, "Step \"5) Backfill source data to Kafka\"") < _index(script, "Step \"6) Catch Kafka bronze topics into Iceberg\"")
    assert _index(script, "Step \"10) Train PM2.5 models\"") < _index(script, "Step \"11) Promote latest models to production registry\"")
    assert _index(script, "Step \"16) Start near-realtime Weather/OpenAQ new-data loops\"") < _index(script, "Step \"17) Start Spark streaming Kafka to Bronze for realtime audit\"")
    assert _index(script, "Step \"17) Start Spark streaming Kafka to Bronze for realtime audit\"") < _index(script, "Step \"20) Build online feature state and run realtime prediction\"")
    assert _index(script, 'Submit-SparkK8s "online-pm25-features"') < _index(script, "kubectl apply -f deploy/k8s/ml/online-pm25-features-cronjob.yaml")


def test_streaming_to_bronze_and_online_path_both_exist():
    script = (ROOT / "scripts" / "run_todo4_stack.ps1").read_text(encoding="utf-8")

    assert "Start-RealtimeBronzeStreaming" in script
    assert "submit_spark.sh openaq" in script
    assert "submit_spark.sh weather" in script
    assert 'Submit-SparkK8s "online-pm25-features"' in script
    assert "pm25-predict-job.yaml" in script
    assert "FEATURE_SOURCE = \"cassandra\"" in script
