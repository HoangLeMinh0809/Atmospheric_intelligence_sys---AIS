# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone


# Parse timestamp ISO va dua ve UTC timezone-aware.
def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# Khai bao class age_minutes de gom state, cau hinh hoac hanh vi lien quan.
def age_minutes(value: str | None, *, now: datetime | None = None) -> float | None:
    parsed = parse_time(value)
    if parsed is None:
        return None
    current = now or datetime.now(timezone.utc)
    return max(0.0, (current - parsed).total_seconds() / 60)


# Khai bao class get_json de gom state, cau hinh hoac hanh vi lien quan.
def get_json(url: str, timeout: int) -> tuple[int, dict]:
    try:
        # Goi HTTP request truc tiep toi endpoint dich.
        with urllib.request.urlopen(url, timeout=timeout) as response:
            # Parse JSON tra ve thanh cau truc dict/list de xu ly tiep.
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            # Parse JSON tra ve thanh cau truc dict/list de xu ly tiep.
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, {"raw": body}


# Khai bao class require_ok de gom state, cau hinh hoac hanh vi lien quan.
def require_ok(name: str, url: str, timeout: int) -> dict:
    status, body = get_json(url, timeout)
    if status < 200 or status >= 300:
        raise RuntimeError(f"{name} failed status={status} body={body}")
    print(f"operational_check check={name} status=ok")
    return body


# Khai bao class checkpoint_age_minutes de gom state, cau hinh hoac hanh vi lien quan.
def checkpoint_age_minutes(webhdfs_base: str, path: str, timeout: int) -> float | None:
    encoded = urllib.parse.quote(path, safe="/")
    status, body = get_json(f"{webhdfs_base.rstrip('/')}{encoded}?op=GETFILESTATUS", timeout)
    if status == 404:
        return None
    if status < 200 or status >= 300:
        raise RuntimeError(f"checkpoint_status failed path={path} status={status} body={body}")
    modified_ms = body.get("FileStatus", {}).get("modificationTime")
    if modified_ms is None:
        return None
    modified = datetime.fromtimestamp(int(modified_ms) / 1000, tz=timezone.utc)
    return age_minutes(modified.isoformat())


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    parser = argparse.ArgumentParser(description="Check AIS readiness, prediction freshness, and stream checkpoints")
    parser.add_argument("--visualization-url", default="http://visualization-api:8080")
    parser.add_argument("--forecast-url", default="http://pm25-api:8080")
    parser.add_argument("--webhdfs-url", default="http://namenode:9870/webhdfs/v1")
    parser.add_argument("--max-prediction-age-minutes", type=int, default=180)
    parser.add_argument("--max-checkpoint-age-minutes", type=int, default=45)
    parser.add_argument("--timeout", type=int, default=10)
    args = parser.parse_args()

    require_ok("visualization_ready", f"{args.visualization_url.rstrip('/')}/readyz", args.timeout)
    require_ok("forecast_ready", f"{args.forecast_url.rstrip('/')}/readyz", args.timeout)
    forecast = require_ok(
        "forecast_latest",
        f"{args.forecast_url.rstrip('/')}/api/v1/hanoi/pm25/forecast/latest",
        args.timeout,
    )
    prediction_age = age_minutes(forecast.get("created_at") or forecast.get("generated_at") or forecast.get("base_hour"))
    if prediction_age is None or prediction_age > args.max_prediction_age_minutes:
        raise RuntimeError(
            f"prediction_stale age_minutes={prediction_age} threshold={args.max_prediction_age_minutes}"
        )
    print(f"operational_check check=prediction_freshness status=ok age_minutes={prediction_age:.1f}")

    checkpoints = (
        "/checkpoints/weather_history/valid",
        "/checkpoints/openaq_hourly/valid",
        "/checkpoints/sentinel5p_summary/valid",
        "/checkpoints/maiac_summary/valid",
    )
    for path in checkpoints:
        checkpoint_age = checkpoint_age_minutes(args.webhdfs_url, path, args.timeout)
        if checkpoint_age is None or checkpoint_age > args.max_checkpoint_age_minutes:
            raise RuntimeError(
                f"checkpoint_stale path={path} age_minutes={checkpoint_age} "
                f"threshold={args.max_checkpoint_age_minutes}"
            )
        print(f"operational_check check=checkpoint_freshness status=ok path={path} age_minutes={checkpoint_age:.1f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"operational_check status=failed error={exc}", file=sys.stderr)
        raise SystemExit(1) from exc
