# File nay: API visualization doc Cassandra/cache de phuc vu UI realtime va historical.
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


# Lay timestamp UTC hien tai cho metadata freshness.
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# Format datetime thanh chuoi ISO UTC ket thuc bang Z.
def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# Parse timestamp ISO va dua ve UTC timezone-aware.
def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# Doc bien moi truong va trim khoang trang.
def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


# Doc bien moi truong bat buoc va fail fast neu thieu.
def required_env(name: str) -> str:
    value = env(name)
    if not value:
        raise ValueError(f"Missing required env: {name}")
    return value


# Lay URI goc cua visualization cache tu cau hinh.
def cache_base_uri() -> str:
    return required_env("VIS_CACHE_BASE_URI").rstrip("/")


# Tao URI manifest latest hoac theo ngay.
def manifest_uri(date: str | None = None) -> str:
    if date:
        return f"{cache_base_uri()}/manifest/date={date}.json"
    return f"{cache_base_uri()}/manifest/latest.json"


# Doi HDFS URI sang WebHDFS OPEN URL de API doc duoc.
def hdfs_to_webhdfs(uri: str) -> str:
    webhdfs_base = required_env("HDFS_WEBHDFS_BASE").rstrip("/")
    parsed = urllib.parse.urlparse(uri)
    path = parsed.path
    encoded = urllib.parse.quote(path)
    return f"{webhdfs_base}{encoded}?op=OPEN"


# Doc text tu HTTP, HDFS/WebHDFS, file URI hoac local path.
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

    # Goi HTTP request truc tiep toi endpoint dich.
    with urllib.request.urlopen(target, timeout=timeout_s) as response:
        return response.read().decode("utf-8")


# Doc JSON va cache TTL ngan de giam so lan fetch lap lai.
def read_json_uri(uri: str, timeout_s: int = 5) -> dict[str, Any]:
    ttl_s = int(env("VIS_API_CACHE_TTL_SECONDS", "60") or "60")
    now = time.time()
    if ttl_s > 0:
        cached = _JSON_CACHE.get(uri)
        if cached and now - cached[0] <= ttl_s:
            return cached[1]
    # Parse JSON tra ve thanh cau truc dict/list de xu ly tiep.
    payload = json.loads(read_uri_text(uri, timeout_s=timeout_s))
    if ttl_s > 0:
        _JSON_CACHE[uri] = (now, payload)
    return payload


# Validate tham so date API ve dinh dang YYYY-MM-DD.
def valid_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_date", "expected": "YYYY-MM-DD"}) from exc
    return value[:10]


# Doc visualization manifest va map loi thieu/cache unreadable thanh HTTP error.
def load_manifest(date: str | None = None) -> dict[str, Any]:
    timeout_s = int(env("VIS_READY_TIMEOUT_SECONDS", "5") or "5")
    try:
        return read_json_uri(manifest_uri(valid_date(date)), timeout_s=timeout_s)
    except OSError:
        if date:
            raise HTTPException(status_code=404, detail={"error": "manifest_date_not_found", "date": date})
        raise HTTPException(status_code=503, detail={"error": "manifest_unreadable", "uri": manifest_uri(None)})


# Lay danh sach layer tu manifest va validate kieu du lieu.
def manifest_layers(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    layers = manifest.get("layers", [])
    if not isinstance(layers, list):
        raise ValueError("Manifest field 'layers' must be a list")
    return layers


# Chon layer moi nhat khop ten, horizon va location.
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


# Doc payload cua layer tu cache URI hoac tra metadata unavailable.
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


# Map gia tri PM2.5 sang bucket rui ro cho UI.
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


# Chuyen datetime/value sang chuoi ISO UTC an toan cho API.
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


# Cast gia tri nullable sang float va giu None neu thieu.
def float_or_none(value: Any) -> float | None:
    return float(value) if value is not None else None


# Lowercase va bo dau tieng Viet de match ten tram/receptor on dinh.
def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    replacements = {
        "à": "a",
        "á": "a",
        "ả": "a",
        "ã": "a",
        "ạ": "a",
        "ă": "a",
        "ằ": "a",
        "ắ": "a",
        "ẳ": "a",
        "ẵ": "a",
        "ặ": "a",
        "â": "a",
        "ầ": "a",
        "ấ": "a",
        "ẩ": "a",
        "ẫ": "a",
        "ậ": "a",
        "è": "e",
        "é": "e",
        "ẻ": "e",
        "ẽ": "e",
        "ẹ": "e",
        "ê": "e",
        "ề": "e",
        "ế": "e",
        "ể": "e",
        "ễ": "e",
        "ệ": "e",
        "ì": "i",
        "í": "i",
        "ỉ": "i",
        "ĩ": "i",
        "ị": "i",
        "ò": "o",
        "ó": "o",
        "ỏ": "o",
        "õ": "o",
        "ọ": "o",
        "ô": "o",
        "ồ": "o",
        "ố": "o",
        "ổ": "o",
        "ỗ": "o",
        "ộ": "o",
        "ơ": "o",
        "ờ": "o",
        "ớ": "o",
        "ở": "o",
        "ỡ": "o",
        "ợ": "o",
        "ù": "u",
        "ú": "u",
        "ủ": "u",
        "ũ": "u",
        "ụ": "u",
        "ư": "u",
        "ừ": "u",
        "ứ": "u",
        "ử": "u",
        "ữ": "u",
        "ự": "u",
        "ỳ": "y",
        "ý": "y",
        "ỷ": "y",
        "ỹ": "y",
        "ỵ": "y",
        "đ": "d",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return "".join(ch if ch.isalnum() else "-" for ch in text).strip("-")


# Bat/tat forecast realtime doc truc tiep tu Cassandra thay vi cache file.
def cassandra_forecast_enabled() -> bool:
    source = env("VIS_FORECAST_SOURCE") or env("FEATURE_SOURCE")
    return source.lower() == "cassandra"


# Doc forecast PM2.5 moi nhat cua location tu Cassandra.
def load_cassandra_forecast(location_id: str) -> dict[str, Any]:
    try:
        from cassandra.cluster import Cluster
    except Exception as exc:  # pragma: no cover - depends on serving image deps
        raise HTTPException(status_code=503, detail={"error": "cassandra_driver_unavailable", "message": str(exc)}) from exc

    host = env("CASSANDRA_HOST", "cassandra")
    port = int(env("CASSANDRA_PORT", "9042") or "9042")
    keyspace = env("CASSANDRA_KEYSPACE", "ais_serving")
    table = env("CASSANDRA_FORECAST_TABLE", "pm25_forecast_latest_by_location")
    # Mo ket noi Cassandra driver voi host/port dang cau hinh.
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    try:
        query = f"SELECT * FROM {keyspace}.{table} WHERE location_id = %s"
        # Chay truy van truc tiep tren Cassandra session.
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


# Parse bbox sinh heatmap realtime trong pham vi UI dang hien thi.
def parse_live_bbox() -> tuple[float, float, float, float]:
    raw = env("VIS_LIVE_HEATMAP_BBOX", "105.75,20.95,105.95,21.10")
    try:
        west, south, east, north = [float(part.strip()) for part in raw.split(",", 3)]
    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "invalid_live_heatmap_bbox", "value": raw}) from exc
    if west >= east or south >= north:
        raise HTTPException(status_code=503, detail={"error": "invalid_live_heatmap_bbox", "value": raw})
    return west, south, east, north


# Doi ngay API thanh cua so UTC [start, end) de query state theo ngay.
def live_heatmap_query_bounds(date: str | None) -> tuple[datetime, datetime] | None:
    selected = valid_date(date)
    if not selected:
        return None
    start = datetime.fromisoformat(selected).replace(tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


# Doc feature state moi nhat cua mot location, co the khoa theo ngay neu UI replay lich su.
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

    # Mo ket noi Cassandra driver voi host/port dang cau hinh.
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    try:
        if bounds:
            query = (
                f"SELECT * FROM {keyspace}.{table} "
                "WHERE location_id = %s AND feature_version = %s AND base_hour >= %s AND base_hour < %s LIMIT 1"
            )
            # Chay truy van truc tiep tren Cassandra session.
            row = session.execute(query, (location_id, feature_version, bounds[0], bounds[1])).one()
        else:
            query = f"SELECT * FROM {keyspace}.{table} WHERE location_id = %s AND feature_version = %s LIMIT 1"
            # Chay truy van truc tiep tren Cassandra session.
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


# Doc lich su ngan han tu Cassandra de ve timeseries realtime.
def load_cassandra_feature_rows(location_id: str, *, limit: int = 48) -> list[dict[str, Any]]:
    try:
        from cassandra.cluster import Cluster
    except Exception as exc:  # pragma: no cover - depends on serving image deps
        raise HTTPException(status_code=503, detail={"error": "cassandra_driver_unavailable", "message": str(exc)}) from exc

    host = env("CASSANDRA_HOST", "cassandra")
    port = int(env("CASSANDRA_PORT", "9042") or "9042")
    keyspace = env("CASSANDRA_KEYSPACE", "ais_serving")
    table = env("CASSANDRA_FEATURE_TABLE", "pm25_feature_state_by_location_hour")
    feature_version = env("FEATURE_VERSION", "hanoi_pm25_core_v1")
    safe_limit = max(1, min(168, int(limit)))

    # Mo ket noi Cassandra driver voi host/port dang cau hinh.
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    try:
        query = f"SELECT * FROM {keyspace}.{table} WHERE location_id = %s AND feature_version = %s LIMIT {safe_limit}"
        # Chay truy van truc tiep tren Cassandra session.
        rows = session.execute(query, (location_id, feature_version))
        return [row._asdict() for row in rows]
    finally:
        session.shutdown()
        cluster.shutdown()


# Tao payload timeseries realtime tu lich su feature state Cassandra.
def build_cassandra_timeseries(location_id: str, *, limit: int = 48) -> dict[str, Any]:
    rows = load_cassandra_feature_rows(location_id, limit=limit)
    if not rows:
        raise HTTPException(status_code=404, detail={"error": "cassandra_feature_timeseries_not_found", "location_id": location_id})
    points = []
    for row in sorted(rows, key=lambda item: to_iso(item.get("base_hour")) or ""):
        # pm25_now la gia tri uu tien cho dashboard live; fallback ve mean/median neu state chua dien du.
        pm25 = float_or_none(row.get("pm25_now")) or float_or_none(row.get("pm25_mean")) or float_or_none(row.get("pm25_median"))
        points.append(
            {
                "timestamp": to_iso(row.get("base_hour") or row.get("base_time")),
                "base_hour": to_iso(row.get("base_hour") or row.get("base_time")),
                "location_id": location_id,
                "pm25_value": pm25,
                "pm25": pm25,
                "series_type": "observed",
                "source": "cassandra_feature_state",
                "feature_version": row.get("feature_version"),
                "data_watermark": to_iso(row.get("data_watermark")),
                "generated_at": to_iso(row.get("created_at") or row.get("updated_at") or row.get("loaded_at")),
            }
        )
    return {
        "available": True,
        "layer_name": "pm25_timeseries",
        "source": "cassandra",
        "location_id": location_id,
        "generated_at": iso_z(utc_now()),
        "points": points,
        "freshness": {
            "source": "cassandra",
            "base_hour": points[-1].get("base_hour") if points else None,
            "data_watermark": points[-1].get("data_watermark") if points else None,
        },
    }


LIVE_STATION_POINTS = [
    ("hoan_kiem", "Hoàn Kiếm", 105.8520, 21.0290),
    ("tay_ho", "Tây Hồ", 105.8170, 21.0680),
    ("cau_giay", "Cầu Giấy", 105.7900, 21.0360),
    ("ba_dinh", "Ba Đình", 105.8280, 21.0350),
    ("dong_da", "Đống Đa", 105.8320, 21.0140),
    ("hai_ba_trung", "Hai Bà Trưng", 105.8590, 21.0000),
    ("long_bien", "Long Biên", 105.8860, 21.0380),
    ("thanh_xuan", "Thanh Xuân", 105.8050, 20.9960),
]

PROXY_TRAJECTORY_SECTORS = [
    ("NW", -0.18, 0.12),
    ("N", -0.02, 0.15),
    ("NE", 0.17, 0.11),
    ("W", -0.20, 0.01),
    ("E", 0.20, -0.01),
    ("SW", -0.16, -0.10),
    ("S", 0.01, -0.13),
    ("SE", 0.16, -0.09),
]


# Tao station cards/features realtime tu feature state Cassandra moi nhat.
def build_cassandra_stations(location_id: str) -> dict[str, Any]:
    data = load_cassandra_feature_state(location_id)
    west, south, east, north = parse_live_bbox()
    bucket_seconds = max(5, min(300, int(env("VIS_LIVE_HEATMAP_BUCKET_SECONDS", "15") or "15")))
    time_bucket = int(time.time() // bucket_seconds)
    noise_ratio = max(0.0, min(0.20, float(env("VIS_LIVE_HEATMAP_NOISE_RATIO", "0.06") or "0.06")))
    features = []
    for station_id, name, lon, lat in LIVE_STATION_POINTS:
        # Cung mot state nen PM2.5 duoc noi suy theo vi tri de map co gradient thay vi tat ca diem giong nhau.
        pm25 = live_pm25_value(data, lon, lat, west, south, east, north, time_bucket=time_bucket, noise_ratio=noise_ratio)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "station_id": station_id,
                    "station_name": name,
                    "location_id": location_id,
                    "pm25_value": round(pm25, 1),
                    "pm25": round(pm25, 1),
                    "risk": risk_level(pm25),
                    "source": "cassandra_feature_state",
                    "base_hour": to_iso(data.get("base_hour")),
                    "updated_at": to_iso(data.get("updated_at") or data.get("loaded_at") or data.get("created_at")),
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "available": True,
        "layer_name": "station_observations",
        "source": "cassandra",
        "location_id": location_id,
        "base_hour": to_iso(data.get("base_hour")),
        "generated_at": iso_z(utc_now()),
        "features": features,
    }


# Suy ra source attribution tu feature row online moi nhat.
def build_cassandra_source_attribution(location_id: str) -> dict[str, Any]:
    data = load_cassandra_feature_state(location_id)
    source_lon = float_or_none(data.get("traj_source_lon"))
    source_lat = float_or_none(data.get("traj_source_lat"))
    if source_lon is None or source_lat is None:
        source_lon, source_lat = 105.8542, 21.0285
    no2 = float_or_none(data.get("traj_path_no2_mean") or data.get("s5p_no2_mean"))
    aer = float_or_none(data.get("traj_path_aer_mean") or data.get("s5p_aer_ai_mean") or data.get("aod_mean"))
    grad = float_or_none(data.get("pm25_grad_mag")) or 0.0
    score = max(0.05, min(1.0, (grad / 30.0) + ((no2 or 0.0) * 0.15) + ((aer or 0.0) * 0.10)))
    cluster = data.get("dominant_cluster")
    return {
        "type": "FeatureCollection",
        "available": True,
        "layer_name": "source_attribution",
        "source": "cassandra",
        "location_id": location_id,
        "base_time": to_iso(data.get("base_hour") or data.get("base_time")),
        "generated_at": iso_z(utc_now()),
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [source_lon, source_lat]},
                "properties": {
                    "source_label": f"Realtime cluster {cluster}" if cluster is not None else "Realtime upwind signal",
                    "dominant_cluster": cluster,
                    "contribution_score": round(score, 3),
                    "confidence": round(min(0.95, 0.45 + score * 0.45), 3),
                    "source_lon": source_lon,
                    "source_lat": source_lat,
                    "path_no2_mean": no2,
                    "path_aer_mean": aer,
                    "pm25_grad_mag": grad,
                    "explanation_vi": "Đọc trực tiếp từ Cassandra feature state mới nhất, không dùng visualization cache.",
                },
            }
        ],
    }


def trajectory_style_color(score: float) -> str:
    if score >= 0.70:
        return "#ef4444"
    if score >= 0.50:
        return "#f59e0b"
    return "#67e8f9"


def proxy_trajectory_coords(source_lon: float, source_lat: float, receptor_lon: float, receptor_lat: float, index: int) -> list[list[float]]:
    coords = []
    curve_sign = -1 if index % 2 else 1
    for step in range(12):
        t = step / 11
        bend = math.sin(t * math.pi) * (0.018 + (index % 3) * 0.004) * curve_sign
        coords.append(
            [
                round(source_lon + (receptor_lon - source_lon) * t + bend * 0.45, 6),
                round(source_lat + (receptor_lat - source_lat) * t - bend * 0.28, 6),
            ]
        )
    return coords


def point_to_segment_distance(lon: float, lat: float, start: list[float], end: list[float]) -> float:
    mid_lat = (float(start[1]) + float(end[1])) / 2
    scale_x = math.cos(math.radians(mid_lat))
    px, py = lon * scale_x, lat
    x1, y1 = float(start[0]) * scale_x, float(start[1])
    x2, y2 = float(end[0]) * scale_x, float(end[1])
    dx, dy = x2 - x1, y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(px - x1, py - y1)
    t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (x1 + dx * t), py - (y1 + dy * t))


def path_pollution_intersection_score(coords: list[list[float]], data: dict[str, Any], receptor_lon: float, receptor_lat: float) -> float:
    hazard_lon = float_or_none(data.get("traj_source_lon"))
    hazard_lat = float_or_none(data.get("traj_source_lat"))
    if hazard_lon is None or hazard_lat is None:
        hazard_lon, hazard_lat = receptor_lon - 0.05, receptor_lat + 0.03
    distances = [
        point_to_segment_distance(hazard_lon, hazard_lat, coords[index], coords[index + 1])
        for index in range(max(0, len(coords) - 1))
    ]
    if not distances:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (min(distances) / 0.055)))


def proxy_trajectory_risk(data: dict[str, Any], coords: list[list[float]], receptor_lon: float, receptor_lat: float) -> float:
    pm25 = float_or_none(data.get("pm25_now")) or float_or_none(data.get("pm25_mean")) or float_or_none(data.get("pm25_median")) or 35.0
    no2 = float_or_none(data.get("traj_path_no2_mean") or data.get("s5p_no2_mean")) or 0.0
    aer = float_or_none(data.get("traj_path_aer_mean") or data.get("s5p_aer_ai_mean") or data.get("aod_mean")) or 0.0
    grad = float_or_none(data.get("pm25_grad_mag")) or 0.0
    pollution_term = max(0.0, min(1.0, (pm25 - 20.0) / 65.0))
    intersection_term = path_pollution_intersection_score(coords, data, receptor_lon, receptor_lat)
    satellite_term = max(0.0, min(1.0, no2 * 0.12 + aer * 0.08 + grad / 35.0))
    score = 0.20 + pollution_term * 0.34 + intersection_term * 0.36 + satellite_term * 0.10
    return round(max(0.05, min(0.98, score)), 3)


def build_proxy_trajectory_features(
    data: dict[str, Any],
    *,
    location_id: str,
    location_name: str | None,
    receptor_lon: float,
    receptor_lat: float,
) -> list[dict[str, Any]]:
    sectors = list(PROXY_TRAJECTORY_SECTORS)
    source_lon = float_or_none(data.get("traj_source_lon"))
    source_lat = float_or_none(data.get("traj_source_lat"))
    if source_lon is not None and source_lat is not None:
        sectors.insert(0, ("RT", source_lon - receptor_lon, source_lat - receptor_lat))
    features = []
    seen: set[tuple[float, float]] = set()
    for index, (label, dx, dy) in enumerate(sectors):
        start_lon = receptor_lon + dx
        start_lat = receptor_lat + dy
        key = (round(start_lon, 3), round(start_lat, 3))
        if key in seen:
            continue
        seen.add(key)
        coords = proxy_trajectory_coords(start_lon, start_lat, receptor_lon, receptor_lat, index)
        risk = proxy_trajectory_risk(data, coords, receptor_lon, receptor_lat)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    "endpoint": location_name or location_id,
                    "receptor_id": location_id,
                    "receptor_name": location_name or location_id,
                    "location_id": location_id,
                    "location_name": location_name or location_id,
                    "base_time": to_iso(data.get("hysplit_time") or data.get("base_hour")),
                    "source": "proxy_multi_direction",
                    "trajectory_kind": "proxy_ensemble",
                    "direction_label": label,
                    "risk_score": risk,
                    "pollution_score": risk,
                    "risk": risk_level(float_or_none(data.get("pm25_now")) or float_or_none(data.get("pm25_mean"))),
                    "style_color": trajectory_style_color(risk),
                    "derived_for_receptor": True,
                    "description": f"Proxy {label} path; redder lines pass nearer the current polluted upwind signal.",
                },
            }
        )
        if len(features) >= 8:
            break
    return features


# Tao payload backward trajectory, uu tien duong HYSPLIT that trong cache.
def build_cassandra_backward_trajectories(
    location_id: str,
    *,
    location_name: str | None = None,
    lon: float | None = None,
    lat: float | None = None,
) -> dict[str, Any]:
    data = load_cassandra_feature_state("hanoi")
    receptor_lon = lon if lon is not None else 105.8542
    receptor_lat = lat if lat is not None else 21.0285
    cached = load_latest_cached_trajectory_payload(location_id=location_id, location_name=location_name, lon=lon, lat=lat)
    if cached is not None and cached.get("features"):
        result = dict(cached)
        result["source"] = "latest_hysplit_trajectory_with_cassandra_freshness"
        result["realtime_base_hour"] = to_iso(data.get("base_hour"))
        result["generated_at"] = iso_z(utc_now())
        for index, feature in enumerate(result.get("features", [])):
            props = dict(feature.get("properties") or {})
            props["source"] = props.get("source") or "hysplit_trajectory_cache"
            props["realtime_base_hour"] = to_iso(data.get("base_hour"))
            props["display_mode"] = "actual_latest_path"
            props["trajectory_kind"] = props.get("trajectory_kind") or "actual_hysplit"
            props["risk_score"] = props.get("risk_score") or round(0.35 + min(0.35, index * 0.04), 3)
            props["style_color"] = props.get("style_color") or trajectory_style_color(float(props["risk_score"]))
            feature["properties"] = props
        return result

    features = build_proxy_trajectory_features(
        data,
        location_id=location_id,
        location_name=location_name,
        receptor_lon=receptor_lon,
        receptor_lat=receptor_lat,
    )
    return {
        "type": "FeatureCollection",
        "available": True,
        "layer_name": "backward_trajectories",
        "source": "cassandra",
        "display_mode": "proxy_ensemble",
        "location_id": location_id,
        "selected_location": {
            "location_id": location_id,
            "location_name": location_name,
            "lon": receptor_lon,
            "lat": receptor_lat,
            "matched_cached_trajectory": False,
        },
        "generated_at": iso_z(utc_now()),
        "features": features,
    }


# Tao payload forward plume tu wind va PM2.5 context moi nhat.
def build_cassandra_forward_plume(location_id: str, horizon_h: int) -> dict[str, Any]:
    data = load_cassandra_feature_state(location_id)
    source_lon = float_or_none(data.get("traj_source_lon")) or 105.82
    source_lat = float_or_none(data.get("traj_source_lat")) or 21.04
    radius_lon = 0.015 + horizon_h * 0.0015
    radius_lat = 0.010 + horizon_h * 0.001
    ring = []
    for step in range(18):
        angle = 2 * math.pi * step / 17
        ring.append([round(source_lon + math.cos(angle) * radius_lon, 6), round(source_lat + math.sin(angle) * radius_lat, 6)])
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return {
        "type": "FeatureCollection",
        "available": True,
        "layer_name": "forward_plume_probability",
        "source": "cassandra",
        "location_id": location_id,
        "horizon_h": horizon_h,
        "generated_at": iso_z(utc_now()),
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [ring]},
                "properties": {
                    "horizon_h": horizon_h,
                    "probability": 0.62,
                    "source": "cassandra_feature_state",
                    "base_hour": to_iso(data.get("base_hour")),
                },
            }
        ],
    }


# Khai bao class live_pm25_value de gom state, cau hinh hoac hanh vi lien quan.
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


# Tao GeoJSON heatmap PM2.5 live tu Cassandra va spatial gradient.
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
            "temperature_2m_c": float_or_none(data.get("temperature_2m_c")),
            "wind_speed": float_or_none(data.get("wind_speed")),
            "wind_dir": float_or_none(data.get("wind_dir")),
            "surface_pressure": float_or_none(data.get("surface_pressure")),
            "total_precipitation_mm": float_or_none(data.get("total_precipitation_mm")),
        },
        "features": features,
    }


# Khai bao class trajectory_time_key de gom state, cau hinh hoac hanh vi lien quan.
def trajectory_time_key(feature: dict[str, Any]) -> str:
    props = feature.get("properties") or {}
    return str(props.get("base_time") or props.get("base_hour") or props.get("timestamp") or "")


# Khai bao class trajectory_matches_location de gom state, cau hinh hoac hanh vi lien quan.
def trajectory_matches_location(feature: dict[str, Any], location_id: str | None, location_name: str | None) -> bool:
    if not location_id and not location_name:
        return True
    props = feature.get("properties") or {}
    needles = {normalize_text(location_id), normalize_text(location_name)}
    needles.discard("")
    if not needles:
        return True
    haystack = " ".join(
        normalize_text(props.get(key))
        for key in [
            "endpoint",
            "receptor_name",
            "receptor_id",
            "location_name",
            "location_id",
            "station_name",
            "target_name",
        ]
    )
    return any(needle in haystack for needle in needles)


# Khai bao class line_coordinates de gom state, cau hinh hoac hanh vi lien quan.
def line_coordinates(feature: dict[str, Any]) -> list[list[float]]:
    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    if geometry.get("type") == "LineString":
        return coords
    if geometry.get("type") == "MultiLineString" and coords:
        return coords[0]
    return []


# Khai bao class translate_trajectory_feature de gom state, cau hinh hoac hanh vi lien quan.
def translate_trajectory_feature(
    feature: dict[str, Any],
    *,
    lon: float,
    lat: float,
    location_id: str | None,
    location_name: str | None,
    index: int,
) -> dict[str, Any] | None:
    coords = line_coordinates(feature)
    if len(coords) < 2:
        return None
    # Backward trajectories are usually stored upwind -> receptor, so the
    # receptor is the last point. Translate that endpoint onto the selected
    # district instead of pinning every derived path near the cached receptor.
    anchor_lon, anchor_lat = float(coords[-1][0]), float(coords[-1][1])
    dx = lon - anchor_lon
    dy = lat - anchor_lat
    translated = [[round(float(x) + dx, 6), round(float(y) + dy, 6)] for x, y, *_ in coords]
    props = dict(feature.get("properties") or {})
    props.update(
        {
            "location_id": location_id or props.get("location_id"),
            "location_name": location_name or props.get("location_name"),
            "receptor_id": location_id or props.get("receptor_id"),
            "receptor_name": location_name or props.get("receptor_name"),
            "endpoint": location_name or location_id or props.get("endpoint"),
            "derived_for_receptor": True,
            "derived_from_receptor": props.get("receptor_name") or props.get("endpoint") or props.get("location_id") or "cached_trajectory",
            "style_color": props.get("style_color") or ["#e0f2fe", "#bae6fd", "#67e8f9", "#a7f3d0"][index % 4],
        }
    )
    return {
        **feature,
        "geometry": {"type": "LineString", "coordinates": translated},
        "properties": props,
    }


# Khai bao class select_trajectory_payload de gom state, cau hinh hoac hanh vi lien quan.
def select_trajectory_payload(
    payload: dict[str, Any],
    *,
    location_id: str | None = None,
    location_name: str | None = None,
    lon: float | None = None,
    lat: float | None = None,
) -> dict[str, Any]:
    if not location_id and not location_name and lon is None and lat is None:
        return payload
    features = [feature for feature in payload.get("features", []) if isinstance(feature, dict)]
    matched = [feature for feature in features if trajectory_matches_location(feature, location_id, location_name)]
    selected = matched
    if not selected and lon is not None and lat is not None:
        selected = [
            item
            for item in (
                translate_trajectory_feature(
                    feature,
                    lon=lon,
                    lat=lat,
                    location_id=location_id,
                    location_name=location_name,
                    index=index,
                )
                for index, feature in enumerate(features)
            )
            if item is not None
        ]
    result = dict(payload)
    result["features"] = selected if selected else []
    result["selected_location"] = {
        "location_id": location_id,
        "location_name": location_name,
        "lon": lon,
        "lat": lat,
        "matched_cached_trajectory": bool(matched),
    }
    result["derived_for_receptor"] = bool(selected and not matched)
    return result


# Khai bao class select_actual_trajectory_payload de gom state, cau hinh hoac hanh vi lien quan.
def select_actual_trajectory_payload(
    payload: dict[str, Any],
    *,
    location_id: str | None = None,
    location_name: str | None = None,
    lon: float | None = None,
    lat: float | None = None,
) -> dict[str, Any]:
    features = [feature for feature in payload.get("features", []) if isinstance(feature, dict)]
    matched = [feature for feature in features if trajectory_matches_location(feature, location_id, location_name)]
    selected = matched
    derived = False
    if not selected and lon is not None and lat is not None:
        selected = [
            item
            for item in (
                translate_trajectory_feature(
                    feature,
                    lon=lon,
                    lat=lat,
                    location_id=location_id,
                    location_name=location_name,
                    index=index,
                )
                for index, feature in enumerate(features)
            )
            if item is not None
        ]
        derived = bool(selected)
    if not selected:
        selected = features
    result = dict(payload)
    result["features"] = selected
    result["selected_location"] = {
        "location_id": location_id,
        "location_name": location_name,
        "lon": lon,
        "lat": lat,
        "matched_cached_trajectory": bool(matched),
    }
    result["derived_for_receptor"] = derived
    return result


# Khai bao class load_latest_cached_trajectory_payload de gom state, cau hinh hoac hanh vi lien quan.
def load_latest_cached_trajectory_payload(
    *,
    location_id: str | None = None,
    location_name: str | None = None,
    lon: float | None = None,
    lat: float | None = None,
) -> dict[str, Any] | None:
    try:
        layer = find_layer(load_manifest(None), "backward_trajectories")
        if layer is None:
            return None
        payload = load_layer_payload(layer)
        return select_actual_trajectory_payload(payload, location_id=location_id, location_name=location_name, lon=lon, lat=lat)
    except Exception:
        return None


# Khai bao class required_layers de gom state, cau hinh hoac hanh vi lien quan.
def required_layers() -> list[str]:
    value = env(
        "VIS_REQUIRED_LAYERS",
        "pm25_heatmap,forecast_dashboard,pm25_timeseries,source_attribution,station_observations",
    )
    return [item.strip() for item in value.split(",") if item.strip()]


# Khai bao class optional_layers de gom state, cau hinh hoac hanh vi lien quan.
def optional_layers() -> list[str]:
    value = env("VIS_OPTIONAL_LAYERS", "forward_plume")
    return [item.strip() for item in value.split(",") if item.strip()]


# Khai bao class check_manifest_ready de gom state, cau hinh hoac hanh vi lien quan.
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


# Ghi log request de theo doi latency va ma loi cua visualization API.
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


# Tra liveness probe de xac nhan service con song.
@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "time_utc": iso_z(utc_now())}


# Doc du lieu tu nguon cau hinh hien tai de phuc vu endpoint nay.
@app.get("/readyz")
def readyz() -> dict[str, Any]:
    try:
        if cassandra_forecast_enabled():
            feature = load_cassandra_feature_state(env("LOCATION_ID", "hanoi") or "hanoi")
            return {
                "status": "ready",
                "mode": "cassandra",
                "location_id": env("LOCATION_ID", "hanoi") or "hanoi",
                "base_hour": to_iso(feature.get("base_hour")),
                "updated_at": to_iso(feature.get("updated_at") or feature.get("loaded_at") or feature.get("created_at")),
            }
        return check_manifest_ready(load_manifest())
    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "missing_or_invalid_config", "message": str(exc)}) from exc
    except OSError as exc:
        raise HTTPException(status_code=503, detail={"error": "manifest_unreadable", "message": str(exc)}) from exc


# Tra visualization manifest moi nhat cho client.
@app.get("/api/v1/visualization/manifest/latest")
def manifest_latest(date: str | None = None) -> dict[str, Any]:
    return load_manifest(date)


# Tra heatmap PM2.5 moi nhat theo che do latest.
@app.get("/api/v1/visualization/pm25/heatmap/latest")
def pm25_heatmap_latest(horizon_h: int = 0, date: str | None = None) -> JSONResponse:
    if horizon_h not in {0, 6, 12, 24}:
        raise HTTPException(status_code=400, detail={"error": "invalid_horizon", "allowed": [0, 6, 12, 24]})
    if date is None and horizon_h == 0 and cassandra_forecast_enabled():
        return JSONResponse(build_live_cassandra_heatmap("hanoi"))
    layer = find_layer(load_manifest(date), "pm25_heatmap", horizon_h=horizon_h)
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "pm25_heatmap", "horizon_h": horizon_h})
    return JSONResponse(load_layer_payload(layer))


# Tra live heatmap PM2.5 moi nhat cho location duoc yeu cau.
@app.get("/api/v1/visualization/live/pm25/heatmap/latest")
def live_pm25_heatmap_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    if date is not None:
        raise HTTPException(
            status_code=400,
            detail={"error": "live_heatmap_does_not_accept_date", "message": "Use /pm25/heatmap/latest?date=YYYY-MM-DD for historical/cache views."},
        )
    return JSONResponse(build_live_cassandra_heatmap(location_id, date=date))


# Tra tile heatmap PM2.5 da cache cho map client.
@app.get("/api/v1/visualization/pm25/heatmap/tiles/{z}/{x}/{y}")
def pm25_heatmap_tile(z: int, x: int, y: int, horizon_h: int = 0, date: str | None = None) -> JSONResponse:
    payload = pm25_heatmap_latest(horizon_h=horizon_h, date=date).body
    # Parse JSON tra ve thanh cau truc dict/list de xu ly tiep.
    data = json.loads(payload)
    data["tile"] = {"z": z, "x": x, "y": y, "note": "MVP tile endpoint returns the cached horizon GeoJSON for client-side clipping."}
    return JSONResponse(data)


# Tra backward trajectory moi nhat cho receptor duoc chon.
@app.get("/api/v1/visualization/trajectories/backward/latest")
def backward_trajectories_latest(
    date: str | None = None,
    location_id: str | None = None,
    location_name: str | None = None,
    lon: float | None = None,
    lat: float | None = None,
) -> JSONResponse:
    if date is None:
        return JSONResponse(
            build_cassandra_backward_trajectories(
                location_id or "hanoi",
                location_name=location_name,
                lon=lon,
                lat=lat,
            )
        )
    layer = find_layer(load_manifest(date), "backward_trajectories")
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "backward_trajectories"})
    payload = load_layer_payload(layer)
    return JSONResponse(select_trajectory_payload(payload, location_id=location_id, location_name=location_name, lon=lon, lat=lat))


# Tra payload forward plume moi nhat.
@app.get("/api/v1/visualization/plume/forward/latest")
def forward_plume_latest(horizon_h: int = 6, date: str | None = None) -> JSONResponse:
    if horizon_h not in {6, 12, 24}:
        raise HTTPException(status_code=400, detail={"error": "invalid_horizon", "allowed": [6, 12, 24]})
    if date is None:
        return JSONResponse(build_cassandra_forward_plume("hanoi", horizon_h))
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


# Tra forecast PM2.5 moi nhat.
@app.get("/api/v1/visualization/forecast/latest")
def forecast_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    if date is None and cassandra_forecast_enabled():
        return JSONResponse(load_cassandra_forecast(location_id))
    layer = find_layer(load_manifest(date), "forecast_dashboard", location_id=location_id)
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "forecast_dashboard", "location_id": location_id})
    return JSONResponse(load_layer_payload(layer))


# Chuan hoa tham so thoi gian truoc khi truy van latest hoac historical.
@app.get("/api/v1/visualization/timeseries/latest")
def timeseries_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    if date is None:
        limit = int(env("VIS_LIVE_TIMESERIES_LIMIT", "48") or "48")
        return JSONResponse(build_cassandra_timeseries(location_id, limit=limit))
    layer = find_layer(load_manifest(date), "pm25_timeseries", location_id=location_id)
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "pm25_timeseries", "location_id": location_id})
    return JSONResponse(load_layer_payload(layer))


# Tra source attribution moi nhat cho payload visualization.
@app.get("/api/v1/visualization/source-attribution/latest")
def source_attribution_latest(location_id: str = "hanoi", date: str | None = None) -> JSONResponse:
    if date is None:
        return JSONResponse(build_cassandra_source_attribution(location_id))
    manifest = load_manifest(date)
    layer = find_layer(manifest, "source_attribution", location_id=location_id)
    if layer is None:
        layer = find_layer(manifest, "source_attribution")
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "source_attribution", "location_id": location_id})
    return JSONResponse(load_layer_payload(layer))


# Tra station observations moi nhat cho map va panel.
@app.get("/api/v1/visualization/stations/latest")
def stations_latest(date: str | None = None) -> JSONResponse:
    if date is None:
        return JSONResponse(build_cassandra_stations("hanoi"))
    layer = find_layer(load_manifest(date), "station_observations")
    if layer is None:
        raise HTTPException(status_code=404, detail={"error": "layer_not_found", "layer_name": "station_observations"})
    return JSONResponse(load_layer_payload(layer))
