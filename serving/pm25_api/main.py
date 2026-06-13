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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _risk_level(pm25: float | None) -> str | None:
    if pm25 is None:
        return None
    if pm25 < 35:
        return "low"
    if pm25 < 75:
        return "medium"
    if pm25 < 150:
        return "high"
    return "very_high"


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _required_env(name: str) -> str:
    value = _env(name)
    if not value:
        raise ValueError(f"Missing required env: {name}")
    return value


def _hdfs_to_webhdfs(uri: str) -> str:
    webhdfs_base = _required_env("HDFS_WEBHDFS_BASE").rstrip("/")
    parsed = urllib.parse.urlparse(uri)
    encoded = urllib.parse.quote(parsed.path)
    return f"{webhdfs_base}{encoded}?op=OPEN"


def _read_uri_text(uri: str, timeout_s: int = 5) -> str:
    parsed = urllib.parse.urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"}:
        target = uri
    elif scheme == "hdfs":
        target = _hdfs_to_webhdfs(uri)
    elif scheme == "file":
        return Path(parsed.path).read_text(encoding="utf-8")
    elif scheme == "":
        return Path(uri).read_text(encoding="utf-8")
    else:
        raise ValueError(f"Unsupported PM2.5 cache URI scheme: {scheme}")

    with urllib.request.urlopen(target, timeout=timeout_s) as response:
        return response.read().decode("utf-8")


def _read_json_uri(uri: str, timeout_s: int = 5) -> dict[str, Any]:
    return json.loads(_read_uri_text(uri, timeout_s=timeout_s))


def _manifest_uri() -> str | None:
    explicit = _env("PM25_MANIFEST_URI")
    if explicit:
        return explicit
    cache_base = _env("VIS_CACHE_BASE_URI")
    if cache_base:
        return f"{cache_base.rstrip('/')}/manifest/latest.json"
    return None


def _forecast_cache_uri() -> str | None:
    explicit = _env("PM25_FORECAST_CACHE_URI") or _env("PREDICTION_CACHE_URI")
    if explicit:
        return explicit

    manifest_uri = _manifest_uri()
    if not manifest_uri:
        return None
    timeout_s = int(_env("READINESS_TIMEOUT_SECONDS", "5") or "5")
    manifest = _read_json_uri(manifest_uri, timeout_s=timeout_s)
    layers = manifest.get("layers", [])
    location_id = _env("LOCATION_ID", "hanoi") or "hanoi"
    candidates = []
    for layer in layers:
        if layer.get("layer_name") != "forecast_dashboard":
            continue
        if layer.get("location_id") not in {None, "", location_id}:
            continue
        if layer.get("available") is False:
            continue
        if not layer.get("cache_uri"):
            continue
        candidates.append(layer)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item.get("generated_at", ""), reverse=True)[0]["cache_uri"]


def _load_forecast_payload() -> dict[str, Any]:
    if _cassandra_forecast_enabled():
        return _load_forecast_payload_from_cassandra()

    timeout_s = int(_env("READINESS_TIMEOUT_SECONDS", "5") or "5")
    uri = _forecast_cache_uri()
    if not uri:
        if _env("PM25_API_ENABLE_SPARK_FALLBACK", "false").lower() in {"1", "true", "yes"}:
            return _load_forecast_payload_from_spark()
        raise HTTPException(
            status_code=503,
            detail={
                "error": "forecast_cache_not_configured",
                "message": "Set PM25_FORECAST_CACHE_URI or VIS_CACHE_BASE_URI. Spark fallback is disabled by default.",
            },
        )
    try:
        return _read_json_uri(uri, timeout_s=timeout_s)
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"error": "forecast_cache_unreadable", "uri": uri, "message": str(exc)}) from exc


def _load_forecast_payload_from_spark() -> dict[str, Any]:
    """Compatibility fallback only. Do not enable for readiness or normal serving."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except Exception as exc:  # pragma: no cover - depends on runtime image
        raise HTTPException(status_code=503, detail={"error": "spark_unavailable", "message": str(exc)}) from exc

    catalog = _required_env("ICEBERG_CATALOG")
    warehouse = _required_env("ICEBERG_WAREHOUSE")
    prediction_table = _required_env("PREDICTION_TABLE")
    hdfs_namenode = _env("HDFS_NAMENODE")
    location_id = _env("LOCATION_ID", "hanoi") or "hanoi"

    builder = (
        SparkSession.builder.appName("AIS_PM25_API_COMPAT_FALLBACK")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    )
    if hdfs_namenode:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_namenode)
    spark = builder.getOrCreate()
    try:
        df = (
            spark.read.table(prediction_table)
            .filter(F.col("location_id") == F.lit(location_id))
            .filter(F.col("model_status") == F.lit("production"))
            .orderBy(F.col("base_hour").desc())
            .limit(1)
        )
        rows = df.collect()
        if not rows:
            raise HTTPException(status_code=404, detail={"error": "prediction_not_found", "location": location_id})
        row = rows[0].asDict(recursive=True)
        return _row_to_forecast_payload(row, location_id=location_id)
    finally:
        spark.stop()


def _to_iso(ts: Any) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _row_to_forecast_payload(row: dict[str, Any], *, location_id: str) -> dict[str, Any]:
    return {
        "base_hour": _to_iso(row.get("base_hour")),
        "location": location_id,
        "pm25_now": float(row.get("pm25_now")) if row.get("pm25_now") is not None else None,
        "forecast": {
            "6h": {
                "pm25": float(row.get("pm25_6h")) if row.get("pm25_6h") is not None else None,
                "risk": row.get("risk_6h") or _risk_level(row.get("pm25_6h")),
            },
            "12h": {
                "pm25": float(row.get("pm25_12h")) if row.get("pm25_12h") is not None else None,
                "risk": row.get("risk_12h") or _risk_level(row.get("pm25_12h")),
            },
            "24h": {
                "pm25": float(row.get("pm25_24h")) if row.get("pm25_24h") is not None else None,
                "risk": row.get("risk_24h") or _risk_level(row.get("pm25_24h")),
            },
        },
        "source_attribution": {
            "dominant_cluster": row.get("dominant_cluster"),
            "source_label": row.get("source_label"),
            "source_lat": row.get("source_lat"),
            "source_lon": row.get("source_lon"),
            "path_no2_mean": row.get("path_no2_mean"),
            "path_aer_mean": row.get("path_aer_mean"),
            "pm25_grad_mag": row.get("pm25_grad_mag"),
        },
        "model": {
            "model_version": row.get("model_version"),
            "model_version_6h": row.get("model_version_6h"),
            "model_version_12h": row.get("model_version_12h"),
            "model_version_24h": row.get("model_version_24h"),
            "feature_version": row.get("feature_version"),
        },
        "created_at": _to_iso(row.get("created_at") or row.get("generated_at")),
    }


def _normalize_forecast_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize visualization dashboard cache or legacy prediction row into PM2.5 API response."""
    location_id = _env("LOCATION_ID", "hanoi") or "hanoi"
    if "forecast" in payload and "base_hour" in payload:
        row = {
            "base_hour": payload.get("base_hour"),
            "pm25_now": (payload.get("forecast") or {}).get("now", {}).get("pm25"),
            "pm25_6h": (payload.get("forecast") or {}).get("6h", {}).get("pm25"),
            "risk_6h": (payload.get("forecast") or {}).get("6h", {}).get("risk"),
            "pm25_12h": (payload.get("forecast") or {}).get("12h", {}).get("pm25"),
            "risk_12h": (payload.get("forecast") or {}).get("12h", {}).get("risk"),
            "pm25_24h": (payload.get("forecast") or {}).get("24h", {}).get("pm25"),
            "risk_24h": (payload.get("forecast") or {}).get("24h", {}).get("risk"),
            "model_version": (payload.get("model") or {}).get("model_version"),
            "model_version_6h": (payload.get("model") or {}).get("model_version_6h"),
            "model_version_12h": (payload.get("model") or {}).get("model_version_12h"),
            "model_version_24h": (payload.get("model") or {}).get("model_version_24h"),
            "feature_version": (payload.get("model") or {}).get("feature_version"),
            "dominant_cluster": (payload.get("source_attribution") or {}).get("dominant_cluster"),
            "source_label": (payload.get("source_attribution") or {}).get("source_label"),
            "source_lat": (payload.get("source_attribution") or {}).get("source_lat"),
            "source_lon": (payload.get("source_attribution") or {}).get("source_lon"),
            "created_at": payload.get("generated_at"),
        }
        return _row_to_forecast_payload(row, location_id=str(payload.get("location_id") or location_id))
    return _row_to_forecast_payload(payload, location_id=location_id)


def _cassandra_forecast_enabled() -> bool:
    source = _env("VIS_FORECAST_SOURCE") or _env("FEATURE_SOURCE")
    return source.lower() == "cassandra"


def _load_forecast_payload_from_cassandra() -> dict[str, Any]:
    try:
        from cassandra.cluster import Cluster
    except Exception as exc:  # pragma: no cover - depends on runtime image
        raise HTTPException(status_code=503, detail={"error": "cassandra_driver_unavailable", "message": str(exc)}) from exc

    host = _env("CASSANDRA_HOST", "cassandra")
    port = int(_env("CASSANDRA_PORT", "9042") or "9042")
    keyspace = _env("CASSANDRA_KEYSPACE", "ais_serving")
    table = _env("CASSANDRA_FORECAST_TABLE", "pm25_forecast_latest_by_location")
    location_id = _env("LOCATION_ID", "hanoi") or "hanoi"

    cluster = Cluster([host], port=port)
    session = cluster.connect()
    try:
        row = session.execute(f"SELECT * FROM {keyspace}.{table} WHERE location_id = %s", (location_id,)).one()
    finally:
        session.shutdown()
        cluster.shutdown()
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "cassandra_forecast_not_found", "location": location_id})
    payload = _row_to_forecast_payload(row._asdict(), location_id=location_id)
    payload["source"] = "cassandra"
    return payload


app = FastAPI(title="AIS PM2.5 API", version="0.1.0")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    error_code = ""
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except HTTPException as exc:
        status_code = exc.status_code
        if isinstance(exc.detail, dict) and "error" in exc.detail:
            error_code = str(exc.detail.get("error") or "")
        raise
    except Exception:
        error_code = "internal_error"
        raise
    finally:
        latency_ms = int((time.time() - start) * 1000)
        print(
            "pm25_api_request "
            f"path={request.url.path} method={request.method} status_code={status_code} "
            f"latency_ms={latency_ms} error_code={error_code}"
        )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "time_utc": _utc_now()}


@app.get("/readyz")
def readyz() -> dict:
    try:
        # Readiness must be lightweight. It must not create SparkSession.
        if _cassandra_forecast_enabled():
            return {"status": "ready", "mode": "cassandra", "location_id": _env("LOCATION_ID", "hanoi") or "hanoi"}
        uri = _forecast_cache_uri()
        if uri:
            timeout_s = int(_env("READINESS_TIMEOUT_SECONDS", "5") or "5")
            _read_json_uri(uri, timeout_s=timeout_s)
            return {"status": "ready", "mode": "cache", "forecast_cache_uri": uri, "location_id": _env("LOCATION_ID", "hanoi") or "hanoi"}
        if _env("PM25_API_ENABLE_SPARK_FALLBACK", "false").lower() in {"1", "true", "yes"}:
            return {"status": "ready", "mode": "spark_fallback_enabled", "location_id": _env("LOCATION_ID", "hanoi") or "hanoi"}
        raise HTTPException(status_code=503, detail={"error": "forecast_cache_not_configured"})
    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "missing_config", "message": str(exc)}) from exc


@app.get("/api/v1/hanoi/pm25/forecast/latest")
def latest_forecast() -> dict:
    payload = _load_forecast_payload()
    return _normalize_forecast_payload(payload)
