from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env: {name}")
    return value


def _build_spark() -> SparkSession:
    catalog = _required_env("ICEBERG_CATALOG")
    warehouse = _required_env("ICEBERG_WAREHOUSE")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "").strip()

    builder = (
        SparkSession.builder.appName("AIS_PM25_API")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    )
    if hdfs_namenode:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_namenode)
    return builder.getOrCreate()


def _get_prediction_row(spark: SparkSession, table: str, *, location_id: str) -> dict | None:
    df = (
        spark.read.table(table)
        .filter(F.col("location_id") == F.lit(location_id))
        .filter(F.col("model_status") == F.lit("production"))
        .orderBy(F.col("base_hour").desc())
        .limit(1)
    )
    rows = df.collect()
    if not rows:
        return None
    return rows[0].asDict(recursive=True)


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
        prediction_table = _required_env("PREDICTION_TABLE")
        location_id = os.getenv("LOCATION_ID", "hanoi").strip() or "hanoi"
        timeout_s = int(os.getenv("READINESS_TIMEOUT_SECONDS", "5"))

        t0 = time.time()
        spark = _build_spark()
        try:
            # cheap connectivity probe: check table resolves and can read 1 row
            spark.read.table(prediction_table).limit(1).count()
        finally:
            spark.stop()

        elapsed = time.time() - t0
        if elapsed > timeout_s:
            raise HTTPException(status_code=503, detail={"error": "readiness_timeout", "elapsed_s": elapsed})

        return {"status": "ready", "prediction_table": prediction_table, "location_id": location_id}

    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "missing_config", "message": str(exc)}) from exc


@app.get("/api/v1/hanoi/pm25/forecast/latest")
def latest_forecast() -> dict:
    try:
        prediction_table = _required_env("PREDICTION_TABLE")
        location_id = os.getenv("LOCATION_ID", "hanoi").strip() or "hanoi"

        spark = _build_spark()
        try:
            row = _get_prediction_row(spark, prediction_table, location_id=location_id)
        finally:
            spark.stop()

        if row is None:
            raise HTTPException(status_code=404, detail={"error": "prediction_not_found", "location": location_id})

        base_hour = row.get("base_hour")
        created_at = row.get("created_at")

        # Spark timestamps are usually Python datetime in Row dict.
        def to_iso(ts):
            if ts is None:
                return None
            if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

        resp = {
            "base_hour": to_iso(base_hour),
            "location": location_id,
            "pm25_now": float(row.get("pm25_now")) if row.get("pm25_now") is not None else None,
            "forecast": {
                "6h": {
                    "pm25": float(row.get("pm25_6h")) if row.get("pm25_6h") is not None else None,
                    "risk": _risk_level(row.get("pm25_6h")),
                },
                "12h": {
                    "pm25": float(row.get("pm25_12h")) if row.get("pm25_12h") is not None else None,
                    "risk": _risk_level(row.get("pm25_12h")),
                },
                "24h": {
                    "pm25": float(row.get("pm25_24h")) if row.get("pm25_24h") is not None else None,
                    "risk": _risk_level(row.get("pm25_24h")),
                },
            },
            "source_attribution": {
                "dominant_cluster": row.get("dominant_cluster"),
                "source_lat": row.get("source_lat"),
                "source_lon": row.get("source_lon"),
                "path_no2_mean": row.get("path_no2_mean"),
                "path_aer_mean": row.get("path_aer_mean"),
                "pm25_grad_mag": row.get("pm25_grad_mag"),
            },
            "model": {
                "model_version_6h": row.get("model_version_6h"),
                "model_version_12h": row.get("model_version_12h"),
                "model_version_24h": row.get("model_version_24h"),
                "feature_version": row.get("feature_version"),
            },
            "created_at": to_iso(created_at),
        }
        return resp

    except ValueError as exc:
        raise HTTPException(status_code=503, detail={"error": "missing_config", "message": str(exc)}) from exc
