# File nay: tao payload visualization gold/cache cho UI ban do va thong ke.
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import timedelta

from pyspark.sql import Row

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    distance_km,
    end_of_date,
    fallback_forecast_values,
    get_tables,
    grid_cells,
    latest_row_asof,
    parse_base_time,
    polygon_geojson,
    read_table_if_exists,
    risk_value,
    run_id,
    station_trend_per_6h,
    utc_now,
    visualization_runtime,
    write_product,
)


HANOI_LAT = 21.0285
HANOI_LON = 105.8542


# Noi suy PM2.5 bang inverse-distance weighting cho payload visualization.
def idw_pm25(lat: float, lon: float, stations: list[dict], fallback: float | None) -> tuple[float | None, int]:
    weighted_sum = 0.0
    weight_total = 0.0
    count = 0
    for station in stations:
        value = station.get("pm25")
        if value is None:
            continue
        dist = max(distance_km(lat, lon, float(station["latitude"]), float(station["longitude"])), 1.0)
        if dist > 180:
            continue
        weight = 1.0 / (dist * dist)
        weighted_sum += float(value) * weight
        weight_total += weight
        count += 1
    if weight_total > 0:
        return weighted_sum / weight_total, count
    return fallback, 0


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    parser = argparse.ArgumentParser(description="Build Northern Vietnam PM2.5 visualization heatmap grid")
    add_common_args(parser)
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    args = parser.parse_args()

    spark = build_spark("VisualizationPM25HeatmapGridGold")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)

    try:
        predictions = read_table_if_exists(spark, tables["prediction_gold"])
        if predictions is None:
            raise RuntimeError(f"Missing prediction table: {tables['prediction_gold']}")
        pred = latest_row_asof(
            predictions,
            "base_hour",
            asof_time,
            filters=[
                predictions.location_id == args.location_id,
                predictions.model_status == "production",
            ],
        )
        horizons = [h for h in runtime["horizons"] if h in {0, 6, 12, 24}]

        stations = []
        station_df = read_table_if_exists(spark, tables["openaq_station_silver"])
        if station_df is not None:
            station_base = station_df.filter(station_df.pm25.isNotNull())
            if asof_time is not None:
                station_base = station_base.filter(station_base.hour <= asof_time.replace(tzinfo=None))
            latest_station_time = station_base.agg({"hour": "max"}).first()[0]
            if latest_station_time is not None:
                stations = [r.asDict() for r in station_df.filter(station_df.hour == latest_station_time).collect()]
        if pred is None and not stations:
            raise RuntimeError(f"No prediction or station observation found for location_id={args.location_id}")
        base_time = pred["base_hour"] if pred else latest_station_time
        station_pm25 = [float(s["pm25"]) for s in stations if s.get("pm25") is not None]
        fallback_anchor = sum(station_pm25) / len(station_pm25) if station_pm25 else 0.0
        fallback_forecasts = fallback_forecast_values(fallback_anchor, station_trend_per_6h(station_df, base_time))

        cells = grid_cells(runtime["bbox"], runtime["grid_resolution_deg"])
        source_label = runtime["cluster_labels"].get(int(pred["dominant_cluster"]), "Unknown source cluster") if pred and pred.get("dominant_cluster") is not None else "Observation trend proxy"
        vis_run_id = run_id("pm25_heatmap_grid", base_time, runtime["product_version"])
        rows = []
        for horizon in horizons:
            if horizon == 0:
                anchor = (pred or {}).get("pm25_now") or fallback_anchor
                method = "station_idw_with_observation_anchor" if pred is None else "station_idw_with_hanoi_prediction_anchor"
            else:
                anchor = (pred or {}).get(f"pm25_{horizon}h") or fallback_forecasts[horizon]
                method = "observation_trend_spatial_proxy" if pred is None else "hanoi_forecast_spatial_decay_proxy"
            for cell in cells:
                valid_time = base_time + timedelta(hours=horizon)
                pm25, obs_count = idw_pm25(float(cell["lat"]), float(cell["lon"]), stations, anchor)
                if pm25 is None:
                    pm25 = 0.0
                if pred is None and horizon > 0:
                    pm25 = max(0.0, float(pm25) + (fallback_forecasts[horizon] - fallback_anchor))
                dist = distance_km(float(cell["lat"]), float(cell["lon"]), HANOI_LAT, HANOI_LON)
                if pm25 is not None and horizon > 0:
                    pm25 = float(pm25) * max(0.55, 1.0 - min(dist, 260.0) / 650.0)
                uncertainty = min(1.0, 0.25 + dist / 450.0 + (0.20 if obs_count == 0 else 0.0) + (0.15 if horizon > 0 else 0.0))
                rows.append(
                    Row(
                        visualization_run_id=vis_run_id,
                        product_version=runtime["product_version"],
                        schema_version=runtime["schema_version"],
                        base_time=base_time,
                        valid_time=valid_time,
                        horizon_h=int(horizon),
                        cell_id=str(cell["cell_id"]),
                        lat=float(cell["lat"]),
                        lon=float(cell["lon"]),
                        lat_min=float(cell["lat_min"]),
                        lat_max=float(cell["lat_max"]),
                        lon_min=float(cell["lon_min"]),
                        lon_max=float(cell["lon_max"]),
                        pm25_value=float(pm25),
                        risk=risk_value(pm25),
                        uncertainty=float(uncertainty),
                        source_method=method,
                        observation_count=int(obs_count),
                        satellite_product_count=0,
                        prediction_id=(pred or {}).get("prediction_id") or "station_observation_proxy",
                        model_version=((pred or {}).get(f"model_version_{horizon}h") if horizon else (pred or {}).get("model_version")) or "observation_trend_proxy",
                        feature_version=(pred or {}).get("feature_version") or "",
                        source_cluster_id=int(pred["dominant_cluster"]) if pred and pred.get("dominant_cluster") is not None else 0,
                        source_label=source_label or "",
                        geometry_geojson=polygon_geojson(float(cell["lon_min"]), float(cell["lat_min"]), float(cell["lon_max"]), float(cell["lat_max"])),
                        generated_at=generated_at,
                        data_freshness_minutes=int((generated_at.replace(tzinfo=None) - base_time).total_seconds() / 60),
                        year=int(valid_time.year),
                        month=int(valid_time.month),
                        day=int(valid_time.day),
                    )
                )

        out = spark.createDataFrame(rows)
        count = write_product(out, tables["visualization_heatmap_grid_gold"], dry_run)
        print(
            "job=visualization_pm25_heatmap_grid "
            f"base_time={base_time} horizons={','.join(map(str, horizons))} grid_cell_count={len(cells)} "
            f"station_count={len(stations)} output_count={count} dry_run={int(dry_run)} "
            f"status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
