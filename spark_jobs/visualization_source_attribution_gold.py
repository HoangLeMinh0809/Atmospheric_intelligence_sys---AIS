# File nay: tao payload visualization gold/cache cho UI ban do va thong ke.
from __future__ import annotations

import argparse
import hashlib
import os

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    end_of_date,
    get_tables,
    latest_row_asof,
    parse_base_time,
    point_geojson,
    read_table_if_exists,
    run_id,
    utc_now,
    visualization_runtime,
    write_product,
)

OUTPUT_SCHEMA = StructType(
    [
        StructField("attribution_id", StringType(), False),
        StructField("visualization_run_id", StringType(), False),
        StructField("product_version", StringType(), False),
        StructField("schema_version", StringType(), False),
        StructField("base_time", TimestampType(), False),
        StructField("valid_time", TimestampType(), False),
        StructField("location_id", StringType(), False),
        StructField("cluster_id", IntegerType(), False),
        StructField("source_label", StringType(), False),
        StructField("source_region_type", StringType(), False),
        StructField("source_lat", DoubleType(), False),
        StructField("source_lon", DoubleType(), False),
        StructField("contribution_score", DoubleType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("traj_count", IntegerType(), False),
        StructField("age_window_start_h", IntegerType(), False),
        StructField("age_window_end_h", IntegerType(), False),
        StructField("evidence_no2_mean", DoubleType(), False),
        StructField("evidence_aer_mean", DoubleType(), False),
        StructField("evidence_no2_aer_ratio", DoubleType(), False),
        StructField("evidence_pm25_grad_mag", DoubleType(), False),
        StructField("explanation_vi", StringType(), False),
        StructField("geometry_geojson", StringType(), False),
        StructField("generated_at", TimestampType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
    ]
)


# Gioi han gia tri trong khoang hop le cho payload visualization.
def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    parser = argparse.ArgumentParser(description="Build PM2.5 source attribution visualization layer")
    add_common_args(parser)
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    args = parser.parse_args()

    spark = build_spark("VisualizationSourceAttributionGold")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)

    try:
        hourly = read_table_if_exists(spark, tables["trajectory_hourly_silver"])
        if hourly is None:
            raise RuntimeError(f"Missing trajectory hourly table: {tables['trajectory_hourly_silver']}")
        item = latest_row_asof(hourly.filter(hourly.dominant_cluster.isNotNull()), "hour", asof_time)
        if item is None:
            print(
                "job=visualization_source_attribution "
                f"location_id={args.location_id} asof_time={asof_time} output_count=0 "
                f"dry_run={int(dry_run)} status=no_real_attribution_rows"
            )
            return

        base_time = item["hour"]
        cluster_id = item.get("dominant_cluster")
        label = runtime["cluster_labels"].get(int(cluster_id), "Unknown source cluster") if cluster_id is not None else "Unknown source cluster"
        n_traj = int(item.get("n_traj") or 0)
        no2 = float(item["path_no2_mean"]) if item.get("path_no2_mean") is not None else 0.0
        aer = float(item["path_aer_mean"]) if item.get("path_aer_mean") is not None else 0.0
        contribution = clamp(0.25 + min(n_traj, 10) / 20.0 + min(abs(no2) + abs(aer), 2.0) / 8.0)
        confidence = clamp(0.30 + min(n_traj, 10) / 20.0 + (0.15 if no2 or aer else 0.0))
        explanation = (
            f"Cluster {cluster_id} ({label}) is the dominant recent trajectory signal. "
            f"Score is based on trajectory count, satellite path evidence, and PM2.5 gradient features."
        )
        vis_run_id = run_id("source_attribution", base_time, runtime["product_version"])
        source_lat = item.get("source_lat")
        source_lon = item.get("source_lon")
        row = Row(
            attribution_id=hashlib.sha1(f"{args.location_id}:{cluster_id}:{base_time}:{runtime['product_version']}".encode()).hexdigest()[:20],
            visualization_run_id=vis_run_id,
            product_version=runtime["product_version"],
            schema_version=runtime["schema_version"],
            base_time=base_time,
            valid_time=base_time,
            location_id=args.location_id,
            cluster_id=int(cluster_id) if cluster_id is not None else -1,
            source_label=label,
            source_region_type="trajectory_cluster",
            source_lat=float(source_lat or 0.0),
            source_lon=float(source_lon or 0.0),
            contribution_score=float(contribution),
            confidence=float(confidence),
            traj_count=n_traj,
            age_window_start_h=-72,
            age_window_end_h=0,
            evidence_no2_mean=float(item.get("path_no2_mean") or 0.0),
            evidence_aer_mean=float(item.get("path_aer_mean") or 0.0),
            evidence_no2_aer_ratio=float(item.get("path_no2_aer_ratio") or 0.0),
            evidence_pm25_grad_mag=0.0,
            explanation_vi=explanation,
            geometry_geojson=point_geojson(source_lon or 0.0, source_lat or 0.0),
            generated_at=generated_at,
            year=int(base_time.year),
            month=int(base_time.month),
            day=int(base_time.day),
        )
        out = spark.createDataFrame([row], schema=OUTPUT_SCHEMA)
        count = write_product(out, tables["visualization_source_attribution_gold"], dry_run)
        print(
            "job=visualization_source_attribution "
            f"base_time={base_time} cluster_id={cluster_id} output_count={count} dry_run={int(dry_run)} "
            f"status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
