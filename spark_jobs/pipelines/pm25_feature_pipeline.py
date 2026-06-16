# File nay: orchestrate cac Spark job theo dung thu tu bronze/silver/gold.
from __future__ import annotations

import argparse
import os

from _pipeline_shared import build_pipeline_spark, invoke_module_main


STEP_CONFIG = {
    "openaq-station": ("hanoi_openaq_silver", []),
    "weather-proxy": ("hanoi_weather_surface_proxy_silver", []),
    "era5-surface": ("era5_surface_hanoi_silver", []),
    "sentinel5p-silver": ("sentinel5p_hanoi_silver", []),
    "maiac-silver": ("maiac_hanoi_silver", []),
    "openaq-gradient": ("openaq_spatial_gradient_silver", []),
    "s5p-grid": ("sentinel5p_grid_silver", []),
    "master-features": ("hanoi_pm25_master_features_gold", []),
    "training-dataset": ("hanoi_pm25_training_dataset_gold", []),
    "serving-features": ("hanoi_pm25_serving_features_gold", []),
}


DEFAULT_STEPS = ",".join(STEP_CONFIG.keys())
DATE_REQUIRED_STEPS = {"era5-surface", "sentinel5p-silver", "maiac-silver"}
ASOF_SUPPORTED_STEPS = {
    "openaq-station",
    "weather-proxy",
    "era5-surface",
    "openaq-gradient",
    "master-features",
    "serving-features",
}


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PM2.5 silver/gold/features in one Spark app")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--asof-time", default=os.getenv("ASOF_TIME", os.getenv("SIMULATED_NOW", os.getenv("BASE_TIME", ""))))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--steps", default=os.getenv("PIPELINE_STEPS", DEFAULT_STEPS))
    return parser.parse_args()


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    steps = [item.strip() for item in args.steps.split(",") if item.strip()]
    invalid = [item for item in steps if item not in STEP_CONFIG]
    if invalid:
        raise ValueError(f"Unknown PM2.5 pipeline steps: {invalid}")
    missing_date_steps = sorted(DATE_REQUIRED_STEPS.intersection(steps))
    if missing_date_steps and (not args.start_date.strip() or not args.end_date.strip()):
        raise ValueError(
            "START_DATE and END_DATE are required for steps: "
            f"{','.join(missing_date_steps)}; got START_DATE={args.start_date!r}, END_DATE={args.end_date!r}"
        )

    spark = build_pipeline_spark("AISPM25FeaturePipeline")
    spark.sparkContext.setLogLevel("WARN")
    try:
        for step in steps:
            module_name, extra_args = STEP_CONFIG[step]
            module_args = []
            if args.start_date.strip():
                module_args.extend(["--start-date", args.start_date.strip()])
            if args.end_date.strip():
                module_args.extend(["--end-date", args.end_date.strip()])
            if args.asof_time.strip() and step in ASOF_SUPPORTED_STEPS:
                module_args.extend(["--asof-time", args.asof_time.strip()])
            module_args.extend(["--full-refresh", args.full_refresh, *extra_args])
            invoke_module_main(
                module_name,
                module_args,
                spark,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
