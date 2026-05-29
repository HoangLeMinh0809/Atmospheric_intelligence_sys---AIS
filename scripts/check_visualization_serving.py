from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request


def get_json(url: str, timeout: int) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8")
        try:
            body = json.loads(payload)
        except json.JSONDecodeError:
            body = {"raw": payload}
        return exc.code, body


def require_ok(name: str, url: str, timeout: int) -> dict:
    status, body = get_json(url, timeout)
    if status < 200 or status >= 300:
        raise RuntimeError(f"{name} failed status={status} url={url} body={body}")
    print(f"visualization_serving_check check={name} status=ok url={url}")
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description="Check TODO4 visualization API/cache serving endpoints")
    parser.add_argument("--base-url", default="http://localhost:8082")
    parser.add_argument("--timeout", type=int, default=5)
    parser.add_argument("--location-id", default="hanoi")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    require_ok("healthz", f"{base}/healthz", args.timeout)
    require_ok("readyz", f"{base}/readyz", args.timeout)
    manifest = require_ok("manifest_latest", f"{base}/api/v1/visualization/manifest/latest", args.timeout)
    layers = manifest.get("layers", [])
    if not layers:
        raise RuntimeError("manifest_latest returned no layers")

    for horizon in [0, 6, 12, 24]:
        require_ok("heatmap", f"{base}/api/v1/visualization/pm25/heatmap/latest?horizon_h={horizon}", args.timeout)

    require_ok("forecast", f"{base}/api/v1/visualization/forecast/latest?location_id={urllib.parse.quote(args.location_id)}", args.timeout)
    require_ok("timeseries", f"{base}/api/v1/visualization/timeseries/latest?location_id={urllib.parse.quote(args.location_id)}", args.timeout)
    require_ok("backward_trajectories", f"{base}/api/v1/visualization/trajectories/backward/latest", args.timeout)
    require_ok("source_attribution", f"{base}/api/v1/visualization/source-attribution/latest?location_id={urllib.parse.quote(args.location_id)}", args.timeout)
    require_ok("stations", f"{base}/api/v1/visualization/stations/latest", args.timeout)

    plume_status, plume_body = get_json(f"{base}/api/v1/visualization/plume/forward/latest?horizon_h=6", args.timeout)
    if plume_status != 200:
        raise RuntimeError(f"forward plume endpoint failed status={plume_status} body={plume_body}")
    if plume_body.get("available") is False and not plume_body.get("reason"):
        raise RuntimeError(f"forward plume unavailable without reason body={plume_body}")
    print("visualization_serving_check check=forward_plume status=ok")
    print("visualization_serving_check status=ok")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"visualization_serving_check status=failed error={exc}", file=sys.stderr)
        raise SystemExit(1) from exc
