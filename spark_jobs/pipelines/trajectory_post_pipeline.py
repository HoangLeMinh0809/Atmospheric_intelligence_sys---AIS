# File nay: orchestrate cac Spark job theo dung thu tu bronze/silver/gold.
from __future__ import annotations

import argparse
import os

from _pipeline_shared import build_pipeline_spark, invoke_module_main


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run post-HYSPLIT trajectory Spark stages in one app")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--direction", choices=("backward", "forward", "both"), default=os.getenv("DIRECTION", "both"))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--spatial-bucket-deg", default=os.getenv("TRAJ_SPATIAL_BUCKET_DEG", "0.25"))
    parser.add_argument("--max-distance-deg", default=os.getenv("MAX_DISTANCE_DEG", "0.25"))
    return parser.parse_args()


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    spark = build_pipeline_spark("AISTrajectoryPostPipeline")
    spark.sparkContext.setLogLevel("WARN")
    try:
        for direction in ([args.direction] if args.direction != "both" else ["backward", "forward"]):
            invoke_module_main(
                "hysplit_trajectory_parse_silver",
                [
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--full-refresh",
                    args.full_refresh,
                ],
                spark,
                env={"DIRECTION": direction},
            )
        cluster_direction = "all" if args.direction == "both" else args.direction
        invoke_module_main(
            "hysplit_trajectory_cluster_silver",
            [
                "--start-date",
                args.start_date,
                "--end-date",
                args.end_date,
                "--direction",
                cluster_direction,
                "--full-refresh",
                args.full_refresh,
            ],
            spark,
        )
        invoke_module_main(
            "trajectory_path_sampling_silver",
            [
                "--start-date",
                args.start_date,
                "--end-date",
                args.end_date,
                "--full-refresh",
                args.full_refresh,
                "--spatial-bucket-deg",
                args.spatial_bucket_deg,
                "--max-distance-deg",
                args.max_distance_deg,
            ],
            spark,
        )
        invoke_module_main(
            "trajectory_hourly_features_silver",
            [
                "--start-date",
                args.start_date,
                "--end-date",
                args.end_date,
                "--full-refresh",
                args.full_refresh,
            ],
            spark,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
