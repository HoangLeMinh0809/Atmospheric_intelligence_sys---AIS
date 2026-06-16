# File nay: tao payload visualization gold/cache cho UI ban do va thong ke.
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import timezone

from pyspark.sql import Row

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    end_of_date,
    fallback_forecast_values,
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


# Tinh so phut giua hai moc thoi gian cho payload visualization.
def minutes_between(now, then) -> int | None:
    if then is None:
        return None
    if getattr(then, "tzinfo", None) is None:
        then = then.replace(tzinfo=timezone.utc)
    return int((now - then.astimezone(timezone.utc)).total_seconds() / 60)


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    parser = argparse.ArgumentParser(description="Build PM2.5 visualization forecast dashboard gold table")
    add_common_args(parser)
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--location-name", default=os.getenv("LOCATION_NAME", "Hanoi"))
    args = parser.parse_args()

    spark = build_spark("VisualizationForecastDashboardGold")
    spark.sparkContext.setLogLevel("WARN")
    runtime = visualization_runtime(args)
    tables = __import__("visualization_common").get_tables()
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)

    try:
        predictions = read_table_if_exists(spark, tables["prediction_gold"])
        if predictions is None:
            raise RuntimeError(f"Missing required prediction table: {tables['prediction_gold']}")
        pred = latest_row_asof(
            predictions,
            "base_hour",
            asof_time,
            filters=[
                predictions.location_id == args.location_id,
                predictions.model_status == "production",
            ],
        )
        if pred is None:
            pred = latest_row_asof(
                predictions,
                "base_hour",
                asof_time,
                filters=[predictions.model_status == "production"],
            )
            if pred is not None:
                print(
                    "job=visualization_forecast_dashboard "
                    f"warning=prediction_location_fallback requested_location={args.location_id} "
                    f"fallback_location={pred.get('location_id')}"
                )
        if pred is None:
            print(f"job=visualization_forecast_dashboard warning=no_production_prediction location_id={args.location_id}")

        station = read_table_if_exists(spark, tables["openaq_station_silver"])
        latest_obs = None
        if station is not None:
            latest_obs = latest_row_asof(station.filter(station.pm25.isNotNull()), "hour", asof_time)
            if latest_obs is None:
                # Fallback to the latest available observation globally when the requested window is empty.
                latest_obs = latest_row_asof(station.filter(station.pm25.isNotNull()), "hour", None)
        if pred is None and latest_obs is None:
            raise RuntimeError(f"No prediction or station observation found for location_id={args.location_id}")

        base_hour = pred.get("base_hour") if pred else latest_obs.get("hour")
        trend = station_trend_per_6h(station, base_hour)
        fallback = fallback_forecast_values((latest_obs or {}).get("pm25") or (pred or {}).get("pm25_now"), trend)
        cluster_labels = runtime["cluster_labels"]
        cluster_id = pred.get("dominant_cluster") if pred else 0
        source_label = cluster_labels.get(int(cluster_id), "Unknown source cluster") if cluster_id is not None else None
        dashboard_id = hashlib.sha1(f"{args.location_id}:{base_hour}:{runtime['product_version']}".encode()).hexdigest()[:20]
        vis_run_id = run_id("forecast_dashboard", base_hour, runtime["product_version"])
        obs_time = latest_obs.get("hour") if latest_obs else None

        row = Row(
            dashboard_id=dashboard_id,
            visualization_run_id=vis_run_id,
            product_version=runtime["product_version"],
            schema_version=runtime["schema_version"],
            base_hour=base_hour,
            location_id=args.location_id,
            location_name=args.location_name,
            latest_observed_time=obs_time or base_hour,
            pm25_latest_observed=float(latest_obs.get("pm25")) if latest_obs and latest_obs.get("pm25") is not None else float(pred.get("pm25_now") or 0.0),
            pm25_now=float((pred or {}).get("pm25_now") or (latest_obs or {}).get("pm25") or 0.0),
            pm25_6h=float((pred or {}).get("pm25_6h") or fallback[6]),
            risk_6h=(pred or {}).get("risk_6h") or risk_value((pred or {}).get("pm25_6h") or fallback[6]),
            pm25_12h=float((pred or {}).get("pm25_12h") or fallback[12]),
            risk_12h=(pred or {}).get("risk_12h") or risk_value((pred or {}).get("pm25_12h") or fallback[12]),
            pm25_24h=float((pred or {}).get("pm25_24h") or fallback[24]),
            risk_24h=(pred or {}).get("risk_24h") or risk_value((pred or {}).get("pm25_24h") or fallback[24]),
            dominant_cluster=int(cluster_id) if cluster_id is not None else -1,
            source_lat=float((pred or {}).get("source_lat") or 21.0285),
            source_lon=float((pred or {}).get("source_lon") or 105.8542),
            source_label=source_label or "",
            path_no2_mean=float((pred or {}).get("path_no2_mean") or 0.0),
            path_aer_mean=float((pred or {}).get("path_aer_mean") or 0.0),
            pm25_grad_mag=float((pred or {}).get("pm25_grad_mag") or 0.0),
            model_version=(pred or {}).get("model_version") or "observation_trend_proxy",
            model_version_6h=(pred or {}).get("model_version_6h") or "observation_trend_proxy",
            model_version_12h=(pred or {}).get("model_version_12h") or "observation_trend_proxy",
            model_version_24h=(pred or {}).get("model_version_24h") or "observation_trend_proxy",
            model_status=(pred or {}).get("model_status") or "unavailable",
            feature_version=(pred or {}).get("feature_version") or "",
            feature_schema_hash=(pred or {}).get("feature_schema_hash") or "",
            prediction_id=(pred or {}).get("prediction_id") or "station_observation_proxy",
            prediction_created_at=(pred or {}).get("created_at") or base_hour,
            generated_at=generated_at,
            prediction_freshness_minutes=minutes_between(generated_at, (pred or {}).get("created_at") or base_hour) or 0,
            observation_freshness_minutes=minutes_between(generated_at, obs_time or base_hour) or 0,
            year=int(base_hour.year),
            month=int(base_hour.month),
            day=int(base_hour.day),
        )
        out = spark.createDataFrame([row])
        count = write_product(out, tables["visualization_forecast_dashboard_gold"], dry_run)
        print(
            "job=visualization_forecast_dashboard "
            f"base_hour={base_hour} output_count={count} dry_run={int(dry_run)} status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
