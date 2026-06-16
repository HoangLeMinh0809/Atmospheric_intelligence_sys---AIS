# File nay: tao payload visualization gold/cache cho UI ban do va thong ke.
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import timedelta

from pyspark.sql import Row
from pyspark.sql import functions as F

from visualization_common import (
    add_common_args,
    apply_date_range,
    as_bool,
    build_spark,
    end_of_date,
    fallback_forecast_values,
    get_tables,
    latest_row_asof,
    parse_base_time,
    read_table_if_exists,
    risk_value,
    run_id,
    station_trend_per_6h,
    utc_now,
    visualization_runtime,
    write_product,
)


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    parser = argparse.ArgumentParser(description="Build PM2.5 observed/forecast visualization timeseries")
    add_common_args(parser)
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--location-name", default=os.getenv("LOCATION_NAME", "Hanoi"))
    args = parser.parse_args()

    spark = build_spark("VisualizationPM25TimeseriesGold")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)

    try:
        predictions = read_table_if_exists(spark, tables["prediction_gold"])
        pred = None
        if predictions is not None:
            pred = latest_row_asof(
                predictions,
                "base_hour",
                asof_time,
                filters=[
                    predictions.location_id == args.location_id,
                    predictions.model_status == "production",
                ],
            )
        station = read_table_if_exists(spark, tables["openaq_station_silver"])
        base_time = pred["base_hour"] if pred else None
        if base_time is None and station is not None:
            station_base = station.filter(station.pm25.isNotNull())
            if asof_time is not None:
                station_base = station_base.filter(station_base.hour <= asof_time.replace(tzinfo=None))
            base_time = station_base.agg({"hour": "max"}).first()[0]
        if base_time is None:
            raise RuntimeError(f"No prediction or station observation found for location_id={args.location_id}")
        vis_run_id = run_id("pm25_timeseries", base_time, runtime["product_version"])
        rows = []

        if station is not None:
            start_time = base_time - timedelta(hours=int(runtime["obs_history_hours"]))
            observed = (
                station.filter(station.pm25.isNotNull())
                .filter(station.hour >= start_time)
                .filter(station.hour <= base_time)
                # Bat dau gom nhom de tinh cac chi so tong hop.
                .groupBy("hour")
                .agg(
                    F.avg("pm25").alias("pm25_value"),
                    F.count("*").alias("station_count"),
                )
                .orderBy("hour")
                .collect()
            )
            for obs in observed:
                ts = obs["hour"]
                pm25 = float(obs["pm25_value"]) if obs["pm25_value"] is not None else None
                rows.append(
                    Row(
                        series_id=hashlib.sha1(f"observed:{ts}:{runtime['product_version']}".encode()).hexdigest()[:20],
                        visualization_run_id=vis_run_id,
                        product_version=runtime["product_version"],
                        schema_version=runtime["schema_version"],
                        location_id=args.location_id,
                        location_name=args.location_name,
                        base_time=base_time,
                        timestamp=ts,
                        series_type="observed",
                        horizon_h=0,
                        pm25_value=pm25,
                        risk=risk_value(pm25),
                        source_table=tables["openaq_station_silver"],
                        source_id=f"station_count={obs['station_count']}",
                        model_version="",
                        generated_at=generated_at,
                        year=int(ts.year),
                        month=int(ts.month),
                        day=int(ts.day),
                    )
                )

        fallback_pm25 = rows[-1].pm25_value if rows else 0.0
        fallback = fallback_forecast_values(fallback_pm25, station_trend_per_6h(station, base_time))
        for horizon in [6, 12, 24]:
            pm25 = (pred or {}).get(f"pm25_{horizon}h") or fallback[horizon]
            valid_time = base_time + timedelta(hours=horizon)
            rows.append(
                Row(
                    series_id=hashlib.sha1(f"forecast:{base_time}:{horizon}:{runtime['product_version']}".encode()).hexdigest()[:20],
                    visualization_run_id=vis_run_id,
                    product_version=runtime["product_version"],
                    schema_version=runtime["schema_version"],
                    location_id=args.location_id,
                    location_name=args.location_name,
                    base_time=base_time,
                    timestamp=valid_time,
                    series_type="forecast",
                    horizon_h=horizon,
                    pm25_value=float(pm25 or 0.0),
                    risk=(pred or {}).get(f"risk_{horizon}h") or risk_value(pm25),
                    source_table=tables["prediction_gold"],
                    source_id=(pred or {}).get("prediction_id") or "station_observation_proxy",
                    model_version=(pred or {}).get(f"model_version_{horizon}h") or (pred or {}).get("model_version") or "observation_trend_proxy",
                    generated_at=generated_at,
                    year=int(valid_time.year),
                    month=int(valid_time.month),
                    day=int(valid_time.day),
                )
            )

        out = spark.createDataFrame(rows)
        if args.start_date or args.end_date:
            out = apply_date_range(out, "timestamp", args.start_date, args.end_date)
        count = write_product(out, tables["visualization_pm25_timeseries_gold"], dry_run)
        print(
            "job=visualization_pm25_timeseries "
            f"base_time={base_time} output_count={count} dry_run={int(dry_run)} status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
