# File nay: orchestrate cac Spark job theo dung thu tu bronze/silver/gold.
from __future__ import annotations

import argparse
import os

from _pipeline_shared import build_pipeline_spark, hdfs_base_uri, invoke_module_main


SOURCE_CONFIG = {
    "openaq": {
        "module": "openaq_hourly_streaming",
        "topic": "openaq-hourly",
        "checkpoint": "openaq_hourly",
    },
    "weather": {
        "module": "weather_streaming",
        "topic": "weather_history",
        "checkpoint": "weather_history",
    },
    "sentinel5p": {
        "module": "sentinel5p_summary_streaming",
        "topic": "sentinel5p-summary",
        "checkpoint": "sentinel5p_summary",
    },
    "maiac": {
        "module": "maiac_summary_streaming",
        "topic": "maiac-summary",
        "checkpoint": "maiac_summary",
    },
    "era5-files": {
        "module": "era5_files_streaming",
        "topic": "era5-files",
        "checkpoint": "era5_files",
    },
}


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run multiple Kafka-to-Iceberg bronze ingests in one Spark app")
    parser.add_argument(
        "--sources",
        default=os.getenv("PIPELINE_SOURCES", "openaq,weather,sentinel5p,maiac,era5-files"),
        help="Comma-separated sources: openaq,weather,sentinel5p,maiac,era5-files",
    )
    parser.add_argument(
        "--continue-on-error",
        default=os.getenv("PIPELINE_CONTINUE_ON_ERROR", "false"),
        help="Continue remaining sources if one source fails",
    )
    return parser.parse_args()


# Chuyen flag dang chuoi nhu 1/true/yes thanh boolean.
def as_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    selected = [item.strip() for item in args.sources.split(",") if item.strip()]
    invalid = [item for item in selected if item not in SOURCE_CONFIG]
    if invalid:
        raise ValueError(f"Unknown bronze sources: {invalid}")

    spark = build_pipeline_spark("AISBronzeIngestToIcebergPipeline")
    spark.sparkContext.setLogLevel("WARN")
    base_uri = hdfs_base_uri()
    checkpoint_run_id = os.getenv("BRONZE_CHECKPOINT_RUN_ID", "").strip()
    checkpoint_base = (
        f"{base_uri}/checkpoints/bronze_backfill_runs/{checkpoint_run_id}"
        if checkpoint_run_id
        else f"{base_uri}/checkpoints"
    )
    continue_on_error = as_bool(args.continue_on_error)
    failures: list[tuple[str, str]] = []

    try:
        for source in selected:
            config = SOURCE_CONFIG[source]
            try:
                invoke_module_main(
                    config["module"],
                    ["--stop-after-batch", "1"],
                    spark,
                    env={
                        "STOP_AFTER_BATCH": "true",
                        "KAFKA_STARTING_OFFSETS": os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
                        "KAFKA_TOPIC": config["topic"],
                        "CHECKPOINT_PATH": f"{checkpoint_base}/{config['checkpoint']}/",
                    },
                )
            except Exception as exc:
                print(f"pipeline_source_failed source={source} error={type(exc).__name__}: {exc}")
                failures.append((source, str(exc)))
                if not continue_on_error:
                    raise
        if failures:
            raise RuntimeError(f"Bronze pipeline completed with failures: {failures}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
