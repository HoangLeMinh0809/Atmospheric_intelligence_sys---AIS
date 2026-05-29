from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def required_env(name: str) -> str:
    value = env(name)
    if not value:
        raise ValueError(f"Missing required env: {name}")
    return value


def cache_base_uri() -> str:
    return required_env("VIS_CACHE_BASE_URI").rstrip("/")


def manifest_uri(date: str | None = None) -> str:
    if date:
        return f"{cache_base_uri()}/manifest/date={date}.json"
    return f"{cache_base_uri()}/manifest/latest.json"


def hdfs_to_webhdfs(uri: str) -> str:
    webhdfs_base = required_env("HDFS_WEBHDFS_BASE").rstrip("/")
    parsed = urllib.parse.urlparse(uri)
    path = parsed.path
    encoded = urllib.parse.quote(path)
    return f"{webhdfs_base}{encoded}?op=OPEN"


def read_uri_text(uri: str, timeout_s: int = 5) -> str:
    parsed = urllib.parse.urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"}:
        target = uri
    elif scheme == "hdfs":
        target = hdfs_to_webhdfs(uri)
    elif scheme == "file":
        return Path(parsed.path).read_text(encoding="utf-8")
    elif scheme == "":
        return Path(uri).read_text(encoding="utf-8")
    else:
        raise ValueError(f"Unsupported cache URI scheme: {scheme}")

    with urllib.request.urlopen(target, timeout=timeout_s) as response:
        return response.read().decode("utf-8")


def read_json_uri(uri: str, timeout_s: int = 5) -> dict[str, Any]:
    return json.loads(read_uri_text(uri, timeout_s=timeout_s))


def valid_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_date", "expected": "YYYY-MM-DD"}) from exc
    return value[:10]


def load_manifest(date: str | None = None) -> dict[str, Any]:
    timeout_s = int(env("VIS_READY_TIMEOUT_SECONDS", "5") or "5")
    try:
        return read_json_uri(manifest_uri(valid_date(date)), timeout_s=timeout_s)
    except OSError:
        if date:
            raise HTTPException(status_code=404, detail={"error": "manifest_date_not_found", "date": date})
        raise


def manifest_layers(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    layers = manifest.get("layers", [])
    if not isinstance(layers, list):
        raise ValueError("Manifest field 'layers' must be a list")
    return layers


def find_layer(
    manifest: dict[str, Any],
    layer_name: str,
    *,
    horizon_h: int | None = None,
    location_id: str | None = None,
) -> dict[str, Any] | None:
    candidates = []
    for layer in manifest_layers(manifest):
        if layer.get("layer_name") != layer_name:
            continue
        if horizon_h is not None and int(layer.get("horizon_h", -999)) != int(horizon_h):
            continue
        if location_id is not None and layer.get("location_id") not in {None, "", location_id}:
            continue
        candidates.append(layer)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item.get("generated_at", ""), reverse=True)[0]


def load_layer_payload(layer: dict[str, Any]) -> dict[str, Any]:
    if layer.get("available") is False:
        return {
            "available": False,
            "layer_name": layer.get("layer_name"),
            "horizon_h": layer.get("horizon_h"),
            "reason": layer.get("unavailable_reason") or "unavailable",
            "generated_at": layer.get("generated_at"),
        }
    cache_uri = layer.get("cache_uri")
    if not cache_uri:
        raise HTTPException(status_code=503, detail={"error": "cache_uri_missing", "layer_name": layer.get("layer_name")})
    timeout_s = int(env("VIS_READY_TIMEOUT_SECONDS", "5") or "5")
    payload = read_json_uri(str(cache_uri), timeout_s=timeout_s)
    payload.setdefault("available", True)
    return payload


def required_layers() -> list[str]:
    value = env(
        "VIS_REQUIRED_LAYERS",
        "pm25_heatmap,forecast_dashboard,pm25_timeseries,backward_trajectories,source_attribution,station_observations",
    )
    return [item.strip() for item in value.split(",") if item.strip()]


def optional_layers() -> list[str]:
    value = env("VIS_OPTIONAL_LAYERS", "forward_plume")
    return [item.strip() for item in value.split(",") if item.strip()]


def check_manifest_ready(manifest: dict[str, Any]) -> dict[str, Any]:
    max_age = int(env("VIS_FRESHNESS_MAX_MINUTES", "180") or "180")
    generated_at = parse_time(manifest.get("generated_at"))
    if generated_at is None:
        raise HTTPException(status_code=503, detail={"error": "manifest_generated_at_missing"})
    age_minutes = int((utc_now() - generated_at).total_seconds() / 60)
    if age_minutes > max_age:
        raise HTTPException(
            status_code=503,
            detail={"error": "manifest_stale", "age_minutes": age_minutes, "threshold_minutes": max_age},
        )

    layers = manifest_layers(manifest)
    available_required = {layer.get("layer_name") for layer in layers if layer.get("available") is not False}
    missing = [layer for layer in required_layers() if layer not in available_required]
    heatmap_horizons = {
        int(layer.get("horizon_h"))
        for layer in layers
        if layer.get("layer_name") == "pm25_heatmap" and layer.get("available") is not False and layer.get("horizon_h") is not None
    }
    missing_heatmap = [h for h in [0, 6, 12, 24] if h not in heatmap_horizons]
    if missing or missing_heatmap:
        raise HTTPException(
            status_code=503,
            detail={"error": "manifest_missing_required_layers", "missing_layers": missing, "missing_heatmap_horizons": missing_heatmap},
        )
    return {
        "status": "ready",
        "generated_at": manifest.get("generated_at"),
        "age_minutes": age_minutes,
        "required_layers": required_layers(),
        "optional_layers": optional_layers(),
    }


app = FastAPI(title="AIS Visualization API", version="0.1.0")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    status_code = 500
    error_code = ""
    layer_name = request.path_params.get("layer_name", "")
    horizon_h = request.query_params.get("horizon_h", "")
    cache_hit = "0"
    try:
        response = await call_next(request)
        status_code = response.status_code
        cache_hit = "1" if status_code < 400 and request.url.path.startswith("/api/v1/visualization") else "0"
        return response
    except HTTPException as exc:
        status_code = exc.status_code
        if isinstance(exc.detail, dict):
            error_code = str(exc.detail.get("error") or "")
        raise
    except Exception:
        error_code = "internal_error"
        raise
    finally:
        latency_ms = int((time.time() - start) * 1000)
        print(
            "visualization_api_request "
            f"path={request.url.path} method={request.method} status_code={status_code} "
            f"latency_ms={latency_ms} layer_name={layer_name} horizon_h={horizon_h} "
            f"cache_hit={cache_hit} error_code={error_code}"
        )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "time_utc": iso_z(utc_now())}


@app.get("/readyz")
def readyz() -> dict[str, Any]:
    try:
        return check_manifest_ready(load_manifest())
    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "missing_or_invalid_config", "message": str(exc)}) from exc
    except OSError as exc:
        raise HTTPException(status_code=503, detail={"error": "manifest_unreadable", "message": str(exc)}) from exc


@app.get("/api/v1/visualization/manifest/latest")
def manifest_latest(date: str | None = None) -> dict[str, Any]:
    return load_manifest(date)


@app.get("/api/v1/visualization/pm25/heatmap/latest")
def pm25_heatmap_latest(horizon_h: int = 0, date: str | None = None) -> JSONResponse:
    if horizon_h not in {0, 6, 12, 24}:
        raise HTTPException(status_code=400, detail={"error": "invalid_horizon", "allowed": [0, 6, 12, 24]})
    layer = find_layer(load_manifest(date), "pm25_heatmap", horizon_h=horizon_h)
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "pm25_heatmap", "horizon_h": horizon_h})
    return JSONResponse(load_layer_payload(layer))


@app.get("/api/v1/visualization/pm25/heatmap/tiles/{z}/{x}/{y}")
def pm25_heatmap_tile(z: int, x: int, y: int, horizon_h: int = 0, date: str | None = None) -> JSONResponse:
    payload = pm25_heatmap_latest(horizon_h=horizon_h, date=date).body
    data = json.loads(payload)
    data["tile"] = {"z": z, "x": x, "y": y, "note": "MVP tile endpoint returns the cached horizon GeoJSON for client-side clipping."}
    return JSONResponse(data)


@app.get("/api/v1/visualization/trajectories/backward/latest")
def backward_trajectories_latest(date: str | None = None) -> JSONResponse:
    layer = find_layer(load_manifest(date), "backward_trajectories")
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "backward_trajectories"})
    return JSONResponse(load_layer_payload(layer))


@app.get("/api/v1/visualization/plume/forward/latest")
def forward_plume_latest(horizon_h: int = 6, date: str | None = None) -> JSONResponse:
    if horizon_h not in {6, 12, 24}:
        raise HTTPException(status_code=400, detail={"error": "invalid_horizon", "allowed": [6, 12, 24]})
    layer = find_layer(load_manifest(date), "forward_plume", horizon_h=horizon_h)
    if layer is None:
        return JSONResponse(
            {
                "available": False,
                "layer_name": "forward_plume_probability",
                "horizon_h": horizon_h,
                "reason": "forward_plume_cache_missing",
                "generated_at": iso_z(utc_now()),
            }
        )
    return JSONResponse(load_layer_payload(layer))


@app.get("/api/v1/visualization/forecast/latest")
def forecast_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    layer = find_layer(load_manifest(date), "forecast_dashboard", location_id=location_id)
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "forecast_dashboard", "location_id": location_id})
    return JSONResponse(load_layer_payload(layer))


@app.get("/api/v1/visualization/timeseries/latest")
def timeseries_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    layer = find_layer(load_manifest(date), "pm25_timeseries", location_id=location_id)
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "pm25_timeseries", "location_id": location_id})
    return JSONResponse(load_layer_payload(layer))


@app.get("/api/v1/visualization/source-attribution/latest")
def source_attribution_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    manifest = load_manifest(date)
    layer = find_layer(manifest, "source_attribution", location_id=location_id)
    if layer is None:
        layer = find_layer(manifest, "source_attribution")
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "source_attribution", "location_id": location_id})
    return JSONResponse(load_layer_payload(layer))


@app.get("/api/v1/visualization/stations/latest")
def stations_latest(date: str | None = None) -> JSONResponse:
    layer = find_layer(load_manifest(date), "station_observations")
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "station_observations"})
    return JSONResponse(load_layer_payload(layer))
