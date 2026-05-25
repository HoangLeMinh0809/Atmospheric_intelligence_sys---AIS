from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

for candidate in [
    Path(__file__).resolve().parents[1] / "spark_jobs",
    Path("/opt/ais/spark_jobs"),
    Path("/opt/spark-jobs"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hanoi_config import get_table_names  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Hanoi PM2.5 forecast inference")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("DRY_RUN", "false").lower() == "true")
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    required = {
        "serving_features": tables["serving_features_gold"],
        "prediction": tables["prediction_gold"],
        "model_registry": tables["model_registry_gold"],
        "location_id": args.location_id,
        "feature_version": args.feature_version,
        "feature_set_name": args.feature_set_name,
    }
    print("pm25_predict_config " + " ".join(f"{key}={value}" for key, value in required.items()))

    if args.dry_run:
        print("pm25_predict dry_run=true status=ok")
        return

    raise SystemExit(
        "PM2.5 inference execution is not implemented in TODO3 sections 4-6. "
        "Run with --dry-run until the TODO3 inference checkpoint is implemented."
    )


if __name__ == "__main__":
    main()
