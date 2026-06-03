from __future__ import annotations

import argparse
import os

from _pipeline_shared import build_pipeline_spark, invoke_module_main


LAYER_CONFIG = {
    "heatmap": "visualization_pm25_heatmap_grid_gold",
    "backward_trajectories": "visualization_backward_trajectory_paths_gold",
    "forward_plume": "visualization_forward_plume_probability_gold",
    "source_attribution": "visualization_source_attribution_gold",
    "stations": "visualization_station_observations_gold",
    "forecast": "visualization_forecast_dashboard_gold",
    "timeseries": "visualization_pm25_timeseries_gold",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run visualization gold builders and export cache in one Spark app")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--asof-time", default=os.getenv("BASE_TIME", ""))
    parser.add_argument(
        "--layers",
        default=os.getenv(
            "PIPELINE_LAYERS",
            "heatmap,backward_trajectories,forward_plume,source_attribution,stations,forecast,timeseries",
        ),
    )
    parser.add_argument("--export-cache", default=os.getenv("EXPORT_CACHE", "true"))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    args = parse_args()
    layers = [item.strip() for item in args.layers.split(",") if item.strip()]
    invalid = [item for item in layers if item not in LAYER_CONFIG]
    if invalid:
        raise ValueError(f"Unknown visualization layers: {invalid}")

    spark = build_pipeline_spark("AISVisualizationPipeline")
    spark.sparkContext.setLogLevel("WARN")
    common_env = {
        "BASE_TIME": args.asof_time,
        "START_DATE": args.start_date,
        "END_DATE": args.end_date,
        "FULL_REFRESH": args.full_refresh,
    }
    try:
        for layer in layers:
            invoke_module_main(
                LAYER_CONFIG[layer],
                [
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--base-time",
                    args.asof_time,
                    "--full-refresh",
                    args.full_refresh,
                ],
                spark,
                env=common_env,
            )
        if as_bool(args.export_cache):
            invoke_module_main(
                "export_visualization_cache",
                [
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--base-time",
                    args.asof_time,
                    "--full-refresh",
                    args.full_refresh,
                ],
                spark,
                env=common_env,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
