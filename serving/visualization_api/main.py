from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, Request


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def cache_base_uri() -> str:
    return os.getenv("VIS_CACHE_BASE_URI", os.getenv("VIS_CACHE_LOCAL_DIR", "/tmp/ais_visualization_cache")).rstrip("/")


def local_path_from_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme in {"", "file"}:
        return Path(parsed.path if parsed.scheme == "file" else uri)
    raise HTTPException(status_code=503, detail={"error": "unsupported_cache_uri", "cache_uri": uri})


def read_json_uri(uri: str) -> dict:
    parsed = urlparse(uri)
    try:
        if parsed.scheme == "hdfs":
            base = os.getenv("HDFS_WEBHDFS_BASE", "").rstrip("/")
            if not base:
                raise HTTPException(status_code=503, detail={"error": "missing_hdfs_webhdfs_base"})
            with urlopen(f"{base}{parsed.path}?op=OPEN", timeout=10) as response:
                return json.loads(response.read().decode("utf-8"))
        path = local_path_from_uri(uri)
        if not path.exists():
            raise HTTPException(status_code=404, detail={"error": "cache_file_missing", "cache_uri": uri})
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail={"error": "cache_json_invalid", "cache_uri": uri}) from exc


def manifest_uri() -> str:
    return f"{cache_base_uri()}/manifest/latest.json"


def load_manifest() -> dict:
    return read_json_uri(manifest_uri())


def find_layer(layer_name: str, horizon_h: int | None = None) -> dict | None:
    manifest = load_manifest()
    for layer in manifest.get("layers", []):
        if layer.get("layer_name") != layer_name:
            continue
        if horizon_h is not None and int(layer.get("horizon_h") or -1) != int(horizon_h):
            continue
        return layer
    return None


def layer_response(layer_name: str, horizon_h: int | None = None) -> dict:
    layer = find_layer(layer_name, horizon_h)
    if not layer:
        raise HTTPException(status_code=404, detail={"error": "layer_manifest_missing", "layer_name": layer_name, "horizon_h": horizon_h})
    if not layer.get("available", False):
        return {
            "available": False,
            "layer_name": layer_name,
            "horizon_h": horizon_h,
            "reason": layer.get("unavailable_reason") or "layer_unavailable",
            "generated_at": layer.get("generated_at"),
            "record_count": layer.get("record_count", 0),
        }
    payload = read_json_uri(str(layer["cache_uri"]))
    payload["available"] = True
    payload["layer_name"] = layer_name
    payload["horizon_h"] = horizon_h
    payload["generated_at"] = layer.get("generated_at")
    payload["record_count"] = layer.get("record_count", 0)
    return payload


app = FastAPI(title="AIS Visualization API", version="0.1.0")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        print(
            "visualization_api_request "
            f"path={request.url.path} method={request.method} status_code={status_code} "
            f"latency_ms={int((time.time() - start) * 1000)}"
        )


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "time_utc": utc_now()}


@app.get("/readyz")
def readyz() -> dict:
    manifest = load_manifest()
    layers = manifest.get("layers", [])
    required = [v.strip() for v in os.getenv("VIS_REQUIRED_LAYERS", "station_observations,backward_trajectories").split(",") if v.strip()]
    missing = []
    unavailable = []
    for name in required:
        layer = next((item for item in layers if item.get("layer_name") == name), None)
        if not layer:
            missing.append(name)
        elif not layer.get("available", False):
            unavailable.append({"layer_name": name, "reason": layer.get("unavailable_reason")})
    if missing or unavailable:
        raise HTTPException(status_code=503, detail={"error": "visualization_cache_not_ready", "missing": missing, "unavailable": unavailable})
    return {"status": "ready", "manifest_uri": manifest_uri(), "layer_count": len(layers)}


@app.get("/api/v1/visualization/manifest/latest")
def manifest_latest() -> dict:
    return load_manifest()


@app.get("/api/v1/visualization/stations/latest")
def stations_latest() -> dict:
    return layer_response("station_observations")


@app.get("/api/v1/visualization/trajectories/backward/latest")
def backward_trajectories_latest() -> dict:
    return layer_response("backward_trajectories")


@app.get("/api/v1/visualization/plume/forward/latest")
def forward_plume_latest(horizon_h: int = 6) -> dict:
    if horizon_h not in {6, 12, 24}:
        raise HTTPException(status_code=400, detail={"error": "invalid_horizon_h", "allowed": [6, 12, 24]})
    return layer_response("forward_plume", horizon_h=horizon_h)
