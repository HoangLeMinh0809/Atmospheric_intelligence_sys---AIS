from __future__ import annotations

import argparse
import hashlib
import os

from pyspark.sql import Row

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    end_of_date,
    get_tables,
    parse_base_time,
    point_geojson,
    read_table_if_exists,
    risk_value,
    run_id,
    utc_now,
    visualization_runtime,
    write_product,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build visualization station observation point layer")
    add_common_args(parser)
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    args = parser.parse_args()

    spark = build_spark("VisualizationStationObservationsGold")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)

    try:
        station = read_table_if_exists(spark, tables["openaq_station_silver"])
        if station is None:
            raise RuntimeError(f"Missing station table: {tables['openaq_station_silver']}")

        station_base = station.filter(station.pm25.isNotNull())
        if asof_time is not None:
            station_base = station_base.filter(station_base.hour <= asof_time.replace(tzinfo=None))
        latest = station_base.agg({"hour": "max"}).first()[0]
        if latest is None:
            latest = generated_at.replace(tzinfo=None)

        source_rows = station.filter(station.hour == latest).filter(station.latitude.isNotNull() & station.longitude.isNotNull()).collect()
        vis_run_id = run_id("station_observations", latest, runtime["product_version"])
        rows = []
        if not source_rows:
            source_rows = [
                {
                    "sensor_id": "upstream_station_missing",
                    "location_id": 0,
                    "location_name": "Hanoi observation unavailable",
                    "city": "Hanoi",
                    "latitude": 21.0285,
                    "longitude": 105.8542,
                    "pm25": 0.0,
                    "coverage_pct": 0.0,
                    "unit": "ug/m3",
                    "provider": "unavailable",
                    "source": "upstream_station_missing",
                }
            ]

        for item in source_rows:
            station_id = str(item["sensor_id"] or item["location_id"] or item["location_name"])
            pm25 = float(item["pm25"]) if item["pm25"] is not None else None
            rows.append(
                Row(
                    observation_id=hashlib.sha1(f"{station_id}:{latest}:{runtime['product_version']}".encode()).hexdigest()[:20],
                    visualization_run_id=vis_run_id,
                    product_version=runtime["product_version"],
                    schema_version=runtime["schema_version"],
                    observation_time=latest,
                    station_id=station_id,
                    station_name=item["location_name"] or "",
                    location_id=args.location_id,
                    city=item["city"] or "",
                    lat=float(item["latitude"]),
                    lon=float(item["longitude"]),
                    pm25=pm25,
                    risk=risk_value(pm25),
                    coverage_pct=float(item["coverage_pct"]) if item["coverage_pct"] is not None else None,
                    unit=item["unit"] or "",
                    provider=item["provider"] or "",
                    source=item["source"] or "",
                    geometry_geojson=point_geojson(item["longitude"], item["latitude"]),
                    generated_at=generated_at,
                    year=int(latest.year),
                    month=int(latest.month),
                    day=int(latest.day),
                )
            )

        out = spark.createDataFrame(rows)
        count = write_product(out, tables["visualization_station_observations_gold"], dry_run)
        print(
            "job=visualization_station_observations "
            f"observation_time={latest} output_count={count} dry_run={int(dry_run)} status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
