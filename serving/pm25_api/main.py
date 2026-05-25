from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class RuntimeConfig(BaseModel):
    prediction_table: str
    iceberg_catalog: str
    iceberg_warehouse: str
    location_id: str


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        prediction_table=os.getenv("PREDICTION_TABLE", ""),
        iceberg_catalog=os.getenv("ICEBERG_CATALOG", ""),
        iceberg_warehouse=os.getenv("ICEBERG_WAREHOUSE", ""),
        location_id=os.getenv("LOCATION_ID", "hanoi"),
    )


app = FastAPI(title="AIS PM2.5 API", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "time_utc": datetime.now(timezone.utc).isoformat()}


@app.get("/readyz")
def readyz() -> dict[str, str]:
    cfg = _runtime_config()
    missing = [
        name
        for name, value in {
            "PREDICTION_TABLE": cfg.prediction_table,
            "ICEBERG_CATALOG": cfg.iceberg_catalog,
            "ICEBERG_WAREHOUSE": cfg.iceberg_warehouse,
        }.items()
        if not value
    ]
    if missing:
        raise HTTPException(status_code=503, detail={"missing": missing})
    return {"status": "ready", "prediction_table": cfg.prediction_table}


@app.get("/forecast/latest")
def latest_forecast() -> dict[str, str]:
    cfg = _runtime_config()
    raise HTTPException(
        status_code=404,
        detail={
            "message": "No materialized PM2.5 prediction row is available through this TODO3 section 4-6 API stub.",
            "prediction_table": cfg.prediction_table,
            "location_id": cfg.location_id,
        },
    )
