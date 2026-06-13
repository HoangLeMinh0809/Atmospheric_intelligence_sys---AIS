from __future__ import annotations

import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse


_JSON_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


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
    ttl_s = int(env("VIS_API_CACHE_TTL_SECONDS", "60") or "60")
    now = time.time()
    if ttl_s > 0:
        cached = _JSON_CACHE.get(uri)
        if cached and now - cached[0] <= ttl_s:
            return cached[1]
    payload = json.loads(read_uri_text(uri, timeout_s=timeout_s))
    if ttl_s > 0:
        _JSON_CACHE[uri] = (now, payload)
    return payload


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
        raise HTTPException(status_code=503, detail={"error": "manifest_unreadable", "uri": manifest_uri(None)})


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


def risk_level(pm25: float | None) -> str | None:
    if pm25 is None:
        return None
    if pm25 < 35:
        return "low"
    if pm25 < 75:
        return "medium"
    if pm25 < 150:
        return "high"
    return "very_high"


def to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "tzinfo") and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    if hasattr(value, "astimezone"):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def float_or_none(value: Any) -> float | None:
    return float(value) if value is not None else None


def cassandra_forecast_enabled() -> bool:
    source = env("VIS_FORECAST_SOURCE") or env("FEATURE_SOURCE")
    return source.lower() == "cassandra"


def load_cassandra_forecast(location_id: str) -> dict[str, Any]:
    try:
        from cassandra.cluster import Cluster
    except Exception as exc:  # pragma: no cover - depends on serving image deps
        raise HTTPException(status_code=503, detail={"error": "cassandra_driver_unavailable", "message": str(exc)}) from exc

    host = env("CASSANDRA_HOST", "cassandra")
    port = int(env("CASSANDRA_PORT", "9042") or "9042")
    keyspace = env("CASSANDRA_KEYSPACE", "ais_serving")
    table = env("CASSANDRA_FORECAST_TABLE", "pm25_forecast_latest_by_location")
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    try:
        query = f"SELECT * FROM {keyspace}.{table} WHERE location_id = %s"
        row = session.execute(query, (location_id,)).one()
    finally:
        session.shutdown()
        cluster.shutdown()
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "cassandra_forecast_not_found", "location_id": location_id})

    data = row._asdict()
    pm25_6h = float_or_none(data.get("pm25_6h"))
    pm25_12h = float_or_none(data.get("pm25_12h"))
    pm25_24h = float_or_none(data.get("pm25_24h"))
    return {
        "available": True,
        "layer_name": "forecast_dashboard",
        "source": "cassandra",
        "base_hour": to_iso(data.get("base_hour")),
        "location": location_id,
        "pm25_now": float_or_none(data.get("pm25_now")),
        "forecast": {
            "6h": {"pm25": pm25_6h, "risk": data.get("risk_6h") or risk_level(pm25_6h)},
            "12h": {"pm25": pm25_12h, "risk": data.get("risk_12h") or risk_level(pm25_12h)},
            "24h": {"pm25": pm25_24h, "risk": data.get("risk_24h") or risk_level(pm25_24h)},
        },
        "source_attribution": {
            "dominant_cluster": data.get("dominant_cluster"),
            "source_lat": data.get("source_lat"),
            "source_lon": data.get("source_lon"),
            "path_no2_mean": data.get("path_no2_mean"),
            "path_aer_mean": data.get("path_aer_mean"),
            "pm25_grad_mag": data.get("pm25_grad_mag"),
        },
        "model": {
            "model_version": data.get("model_version"),
            "model_version_6h": data.get("model_version_6h"),
            "model_version_12h": data.get("model_version_12h"),
            "model_version_24h": data.get("model_version_24h"),
            "feature_version": data.get("feature_version"),
            "feature_source": data.get("feature_source"),
        },
        "prediction_id": data.get("prediction_id"),
        "generated_at": to_iso(data.get("created_at")),
        "freshness": {
            "source": "cassandra",
            "base_hour": to_iso(data.get("base_hour")),
            "generated_at": to_iso(data.get("created_at")),
        },
    }


def parse_live_bbox() -> tuple[float, float, float, float]:
    raw = env("VIS_LIVE_HEATMAP_BBOX", "105.75,20.95,105.95,21.10")
    try:
        west, south, east, north = [float(part.strip()) for part in raw.split(",", 3)]
    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "invalid_live_heatmap_bbox", "value": raw}) from exc
    if west >= east or south >= north:
        raise HTTPException(status_code=503, detail={"error": "invalid_live_heatmap_bbox", "value": raw})
    return west, south, east, north


def live_heatmap_query_bounds(date: str | None) -> tuple[datetime, datetime] | None:
    selected = valid_date(date)
    if not selected:
        return None
    start = datetime.fromisoformat(selected).replace(tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def load_cassandra_feature_state(location_id: str, date: str | None = None) -> dict[str, Any]:
    try:
        from cassandra.cluster import Cluster
    except Exception as exc:  # pragma: no cover - depends on serving image deps
        raise HTTPException(status_code=503, detail={"error": "cassandra_driver_unavailable", "message": str(exc)}) from exc

    host = env("CASSANDRA_HOST", "cassandra")
    port = int(env("CASSANDRA_PORT", "9042") or "9042")
    keyspace = env("CASSANDRA_KEYSPACE", "ais_serving")
    table = env("CASSANDRA_FEATURE_TABLE", "pm25_feature_state_by_location_hour")
    feature_version = env("FEATURE_VERSION", "hanoi_pm25_core_v1")
    bounds = live_heatmap_query_bounds(date)

    cluster = Cluster([host], port=port)
    session = cluster.connect()
    try:
        if bounds:
            query = (
                f"SELECT * FROM {keyspace}.{table} "
                "WHERE location_id = %s AND feature_version = %s AND base_hour >= %s AND base_hour < %s LIMIT 1"
            )
            row = session.execute(query, (location_id, feature_version, bounds[0], bounds[1])).one()
        else:
            query = f"SELECT * FROM {keyspace}.{table} WHERE location_id = %s AND feature_version = %s LIMIT 1"
            row = session.execute(query, (location_id, feature_version)).one()
    finally:
        session.shutdown()
        cluster.shutdown()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "cassandra_feature_state_not_found", "location_id": location_id, "feature_version": feature_version, "date": date},
        )
    return row._asdict()


def live_pm25_value(
    data: dict[str, Any],
    lon: float,
    lat: float,
    west: float,
    south: float,
    east: float,
    north: float,
    *,
    time_bucket: int,
    noise_ratio: float,
) -> float:
    base = float_or_none(data.get("pm25_mean")) or float_or_none(data.get("pm25_median")) or 0.0
    if base <= 0:
        raise HTTPException(status_code=404, detail={"error": "cassandra_feature_state_pm25_missing", "location_id": data.get("location_id")})

    dx = ((lon - west) / max(east - west, 1e-9) - 0.5) * 2.0
    dy = ((lat - south) / max(north - south, 1e-9) - 0.5) * 2.0
    grad_e = float_or_none(data.get("pm25_grad_e")) or 0.0
    grad_w = float_or_none(data.get("pm25_grad_w")) or 0.0
    grad_n = float_or_none(data.get("pm25_grad_n")) or 0.0
    grad_s = float_or_none(data.get("pm25_grad_s")) or 0.0
    spread = max(2.0, min(12.0, float_or_none(data.get("pm25_spatial_std")) or 5.0))

    source_lon = float_or_none(data.get("traj_source_lon"))
    source_lat = float_or_none(data.get("traj_source_lat"))
    source_bump = 0.0
    if source_lon is not None and source_lat is not None:
        source_bump = spread * 0.8 * pow(2.718281828, -(((lon - source_lon) ** 2) / 0.004 + ((lat - source_lat) ** 2) / 0.0025))

    base_hour = to_iso(data.get("base_hour")) or ""
    phase_seed = sum(ord(ch) for ch in base_hour[-12:])
    wave = 0.45 * spread * (
        math.exp(-((dx - 0.25) ** 2 + (dy + 0.10) ** 2) * 2.2)
        - math.exp(-((dx + 0.35) ** 2 + (dy - 0.25) ** 2) * 2.8)
    )
    ripple = 0.18 * spread * (((int((lon * 10000) + phase_seed + time_bucket) % 7) - 3) / 3.0)
    temporal = base * noise_ratio * math.sin(time_bucket * 0.73 + lon * 91.0 + lat * 77.0)

    pm25 = base + ((grad_e - grad_w) * dx + (grad_n - grad_s) * dy) * 0.35 + source_bump + wave + ripple + temporal
    return max(1.0, min(250.0, pm25))


def build_live_cassandra_heatmap(location_id: str, date: str | None = None) -> dict[str, Any]:
    data = load_cassandra_feature_state(location_id, date=date)
    west, south, east, north = parse_live_bbox()
    cols = max(8, min(48, int(env("VIS_LIVE_HEATMAP_COLS", "28") or "28")))
    rows = max(6, min(36, int(env("VIS_LIVE_HEATMAP_ROWS", "20") or "20")))
    bucket_seconds = max(5, min(300, int(env("VIS_LIVE_HEATMAP_BUCKET_SECONDS", "15") or "15")))
    time_bucket = int(time.time() // bucket_seconds)
    noise_ratio = max(0.0, min(0.20, float(env("VIS_LIVE_HEATMAP_NOISE_RATIO", "0.06") or "0.06")))
    features = []
    for row_index in range(rows):
        y1 = south + ((north - south) / rows) * row_index
        y2 = south + ((north - south) / rows) * (row_index + 1)
        for col_index in range(cols):
            x1 = west + ((east - west) / cols) * col_index
            x2 = west + ((east - west) / cols) * (col_index + 1)
            lon = (x1 + x2) / 2
            lat = (y1 + y2) / 2
            pm25 = live_pm25_value(data, lon, lat, west, south, east, north, time_bucket=time_bucket, noise_ratio=noise_ratio)
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Polygon", "coordinates": [[[x1, y1], [x2, y1], [x2, y2], [x1, y2], [x1, y1]]]},
                    "properties": {
                        "grid_id": f"cassandra-live-{row_index}-{col_index}",
                        "location_id": location_id,
                        "pm25_value": round(pm25, 1),
                        "risk": risk_level(pm25),
                        "horizon_h": 0,
                        "source": "cassandra_feature_state",
                        "base_hour": to_iso(data.get("base_hour")),
                    },
                }
            )
    return {
        "type": "FeatureCollection",
        "available": True,
        "layer_name": "pm25_heatmap",
        "source": "cassandra",
        "horizon_h": 0,
        "location_id": location_id,
        "base_hour": to_iso(data.get("base_hour")),
        "generated_at": iso_z(utc_now()),
        "source_loaded_at": to_iso(data.get("loaded_at")) or to_iso(data.get("created_at")),
        "live_bucket_seconds": bucket_seconds,
        "live_noise_ratio": noise_ratio,
        "resolution": {"cols": cols, "rows": rows, "cells": len(features)},
        "summary": {
            "pm25_mean": float_or_none(data.get("pm25_mean")),
            "pm25_median": float_or_none(data.get("pm25_median")),
            "station_count": data.get("station_count"),
            "coverage_avg": float_or_none(data.get("coverage_avg")),
            "feature_version": data.get("feature_version"),
        },
        "features": features,
    }


def required_layers() -> list[str]:
    value = env(
        "VIS_REQUIRED_LAYERS",
        "pm25_heatmap,forecast_dashboard,pm25_timeseries,source_attribution,station_observations",
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
app.add_middleware(GZipMiddleware, minimum_size=1024)


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


@app.get("/api/v1/visualization/live/pm25/heatmap/latest")
def live_pm25_heatmap_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    return JSONResponse(build_live_cassandra_heatmap(location_id, date=date))


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
    if date is None and cassandra_forecast_enabled():
        return JSONResponse(load_cassandra_forecast(location_id))
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
