from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check TODO4 visualization cache/API/UI serving")
    parser.add_argument(
        "--check",
        action="append",
        choices=["cache", "api", "ui-bundle"],
        default=[],
        help="Check to run. Defaults to cache.",
    )
    return parser.parse_args()


def cache_base_uri() -> str:
    return os.getenv("VIS_CACHE_BASE_URI", os.getenv("VIS_CACHE_LOCAL_DIR", "/tmp/ais_visualization_cache")).rstrip("/")


def read_json_uri(uri: str) -> dict:
    parsed = urlparse(uri)
    if parsed.scheme == "hdfs":
        base = os.getenv("HDFS_WEBHDFS_BASE", "").rstrip("/")
        if not base:
            raise SystemExit("HDFS_WEBHDFS_BASE is required for hdfs cache checks")
        with urlopen(f"{base}{parsed.path}?op=OPEN", timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    path = Path(parsed.path if parsed.scheme == "file" else uri)
    if not path.exists():
        raise SystemExit(f"Missing cache file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def cache_exists(uri: str) -> bool:
    parsed = urlparse(uri)
    if parsed.scheme == "hdfs":
        base = os.getenv("HDFS_WEBHDFS_BASE", "").rstrip("/")
        if not base:
            raise SystemExit("HDFS_WEBHDFS_BASE is required for hdfs cache checks")
        try:
            with urlopen(f"{base}{parsed.path}?op=GETFILESTATUS", timeout=10):
                return True
        except Exception:
            return False
    return Path(parsed.path if parsed.scheme == "file" else uri).exists()


def check_cache() -> None:
    manifest_uri = f"{cache_base_uri()}/manifest/latest.json"
    manifest = read_json_uri(manifest_uri)
    layers = manifest.get("layers", [])
    required = {"station_observations", "backward_trajectories"}
    found = {layer.get("layer_name") for layer in layers}
    missing = sorted(required - found)
    if missing:
        raise SystemExit(f"Missing required visualization layers in manifest: {', '.join(missing)}")
    for layer in layers:
        cache_uri = str(layer.get("cache_uri", ""))
        if layer.get("available") and not cache_exists(cache_uri):
            raise SystemExit(f"Available layer cache file is missing: {cache_uri}")
        if layer.get("layer_name") == "forward_plume" and not layer.get("available") and not layer.get("unavailable_reason"):
            raise SystemExit("Forward plume is unavailable without unavailable_reason")
    print(f"visualization_cache_check status=ok manifest={manifest_uri} layer_count={len(layers)}")


def get_json(url: str) -> dict:
    try:
        with urlopen(url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} for {url}: {exc.read().decode('utf-8', errors='ignore')}") from exc
    except URLError as exc:
        raise SystemExit(f"API check failed for {url}: {exc}") from exc


def check_api() -> None:
    base = os.getenv("VIS_API_BASE_URL", "").rstrip("/")
    if not base:
        raise SystemExit("VIS_API_BASE_URL is required for API check")
    for path in [
        "/healthz",
        "/readyz",
        "/api/v1/visualization/manifest/latest",
        "/api/v1/visualization/stations/latest",
        "/api/v1/visualization/trajectories/backward/latest",
        "/api/v1/visualization/plume/forward/latest?horizon_h=6",
    ]:
        payload = get_json(f"{base}{path}")
        if path.endswith("stations/latest") and not payload.get("available", True):
            raise SystemExit("Stations endpoint returned unavailable")
        if "plume/forward" in path and payload.get("available") is False and not payload.get("reason"):
            raise SystemExit("Forward plume unavailable response is missing reason")
    print(f"visualization_api_check status=ok base_url={base}")


def check_ui_bundle() -> None:
    dist = Path(os.getenv("UI_DIST_DIR", "ui/dist"))
    if not dist.exists():
        raise SystemExit(f"UI dist directory not found: {dist}")
    offenders = []
    for path in dist.rglob("*"):
        if path.is_file() and path.suffix in {".js", ".html", ".css"}:
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "/mock/" in text:
                offenders.append(str(path))
    if offenders:
        raise SystemExit(f"Production UI bundle contains /mock/ references: {', '.join(offenders)}")
    print(f"visualization_ui_bundle_check status=ok dist={dist}")


def main() -> None:
    args = parse_args()
    checks = args.check or ["cache"]
    if "cache" in checks:
        check_cache()
    if "api" in checks:
        check_api()
    if "ui-bundle" in checks:
        check_ui_bundle()


if __name__ == "__main__":
    main()
