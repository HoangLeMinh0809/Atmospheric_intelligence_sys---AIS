# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
from __future__ import annotations

import argparse
import os
import sys
from urllib.error import URLError
from urllib.request import urlopen


REQUIRED_CONFIG = [
    "SERVING_FEATURE_TABLE",
    "PREDICTION_TABLE",
    "MODEL_REGISTRY_TABLE",
    "FEATURE_FRESHNESS_MAX_MINUTES",
    "PREDICTION_FRESHNESS_MAX_MINUTES",
]


# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check PM2.5 serving runtime configuration and dependencies")
    parser.add_argument(
        "--check",
        action="append",
        choices=["config", "api-readiness"],
        default=[],
        help="Check to run. May be repeated. Defaults to config.",
    )
    return parser.parse_args()


# Kiem tra tinh dung dan cua du lieu/du doan PM2.5.
def check_config() -> None:
    missing = [name for name in REQUIRED_CONFIG if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Missing required PM2.5 serving config: {', '.join(missing)}")
    print(
        "pm25_serving_config status=ok "
        f"serving_feature_table={os.getenv('SERVING_FEATURE_TABLE')} "
        f"prediction_table={os.getenv('PREDICTION_TABLE')} "
        f"model_registry_table={os.getenv('MODEL_REGISTRY_TABLE')}"
    )


# Kiem tra tinh dung dan cua du lieu/du doan PM2.5.
def check_api_readiness() -> None:
    base_url = os.getenv("PM25_API_BASE_URL", "").rstrip("/")
    if not base_url:
        raise SystemExit("PM25_API_BASE_URL is required for api-readiness check")
    url = f"{base_url}/readyz"
    try:
        with urlopen(url, timeout=5) as response:
            status = response.getcode()
    except URLError as exc:
        raise SystemExit(f"API readiness check failed for {url}: {exc}") from exc
    if status != 200:
        raise SystemExit(f"API readiness check failed for {url}: HTTP {status}")
    print(f"pm25_api_readiness status=ok url={url}")


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    checks = args.check or ["config"]
    if "config" in checks:
        check_config()
    if "api-readiness" in checks:
        check_api_readiness()


if __name__ == "__main__":
    main()
