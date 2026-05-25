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
    parser = argparse.ArgumentParser(description="Promote a Hanoi PM2.5 model run to the production registry")
    parser.add_argument("--model-run-id", default=os.getenv("MODEL_RUN_ID", ""))
    parser.add_argument("--horizon-hour", type=int, choices=[6, 12, 24], default=int(os.getenv("HORIZON_HOUR", "6")))
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("DRY_RUN", "true").lower() == "true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    print(
        "pm25_promote_config "
        f"model_run_id={args.model_run_id or '<required>'} "
        f"horizon_hour={args.horizon_hour} "
        f"model_runs_table={tables['model_runs_gold']} "
        f"model_registry_table={tables['model_registry_gold']}"
    )
    if args.dry_run:
        print("pm25_promote dry_run=true status=ok")
        return
    if not args.model_run_id:
        raise SystemExit("--model-run-id is required when --dry-run is not set")
    raise SystemExit("Model promotion writes are implemented in a later TODO3 checkpoint.")


if __name__ == "__main__":
    main()
