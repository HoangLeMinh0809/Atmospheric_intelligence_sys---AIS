from __future__ import annotations

import argparse
import hashlib
import json

from pyspark.sql import Row

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    end_of_date,
    get_tables,
    line_geojson,
    parse_base_time,
    read_table_if_exists,
    run_id,
    utc_now,
    visualization_runtime,
    write_product,
)


STYLE_COLORS = ["#2563eb", "#16a34a", "#f97316", "#dc2626", "#7c3aed", "#0891b2", "#475569"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build backward trajectory visualization LineString layer")
    add_common_args(parser)
    args = parser.parse_args()

    spark = build_spark("VisualizationBackwardTrajectoryPathsGold")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)

    try:
        traj = read_table_if_exists(spark, tables["hysplit_cluster_silver"])
        if traj is None:
            raise RuntimeError(f"Missing trajectory clustered table: {tables['hysplit_cluster_silver']}")
        points = traj.filter(traj.direction == "backward")
        if asof_time is not None:
            points = points.filter(points.timestamp <= asof_time.replace(tzinfo=None))
        if args.start_date:
            points = points.filter(points.timestamp >= args.start_date)
        if args.end_date:
            points = points.filter(points.timestamp <= args.end_date)
        source = points.collect()
        by_traj: dict[str, list[dict]] = {}
        for row in source:
            item = row.asDict()
            by_traj.setdefault(str(item["traj_id"]), []).append(item)

        path_features = {}
        path_df = read_table_if_exists(spark, tables["trajectory_path_silver"])
        if path_df is not None:
            path_features = {str(r["traj_id"]): r.asDict() for r in path_df.collect()}

        if source:
            base_time = max(item["timestamp"] for item in source if item["timestamp"] is not None)
        else:
            base_time = (asof_time or generated_at).replace(tzinfo=None)
        vis_run_id = run_id("backward_trajectories", base_time, runtime["product_version"])
        rows = []
        invalid = 0
        for traj_id, items in by_traj.items():
            ordered = sorted(items, key=lambda item: (item.get("age_h") is None, item.get("age_h") or 0))
            if len(ordered) < 2:
                invalid += 1
                continue
            first = ordered[0]
            last = ordered[-1]
            cluster_id = first.get("cluster_id")
            label = runtime["cluster_labels"].get(int(cluster_id), "Unknown source cluster") if cluster_id is not None else None
            evidence = path_features.get(traj_id, {})
            ages = [int(item["age_h"]) for item in ordered if item.get("age_h") is not None]
            props = {
                "traj_id": traj_id,
                "cluster_id": cluster_id,
                "source_label": label,
                "source_lat": first.get("source_lat"),
                "source_lon": first.get("source_lon"),
                "path_no2_mean": evidence.get("path_no2_mean"),
                "path_aer_mean": evidence.get("path_aer_mean"),
                "path_no2_aer_ratio": evidence.get("path_no2_aer_ratio"),
            }
            init_time = first.get("timestamp") or base_time
            rows.append(
                Row(
                    visualization_run_id=vis_run_id,
                    product_version=runtime["product_version"],
                    schema_version=runtime["schema_version"],
                    base_time=base_time,
                    init_time=init_time,
                    direction="backward",
                    traj_id=traj_id,
                    traj_no=0,
                    cluster_id=int(cluster_id) if cluster_id is not None else None,
                    source_label=label or "",
                    source_lat=float(first.get("source_lat") or 0.0),
                    source_lon=float(first.get("source_lon") or 0.0),
                    source_alt_m=float(first.get("source_alt_m") or 0.0),
                    start_lat=float(first.get("lat") or 0.0),
                    start_lon=float(first.get("lon") or 0.0),
                    end_lat=float(last.get("lat") or 0.0),
                    end_lon=float(last.get("lon") or 0.0),
                    age_start_h=min(ages) if ages else 0,
                    age_end_h=max(ages) if ages else 0,
                    point_count=len(ordered),
                    path_no2_mean=float(evidence.get("path_no2_mean") or 0.0),
                    path_aer_mean=float(evidence.get("path_aer_mean") or 0.0),
                    path_no2_aer_ratio=float(evidence.get("path_no2_aer_ratio") or 0.0),
                    geometry_geojson=line_geojson(ordered),
                    properties_json=json.dumps(props, separators=(",", ":")),
                    style_color=STYLE_COLORS[int(cluster_id or 0) % len(STYLE_COLORS)],
                    generated_at=generated_at,
                    year=int(init_time.year),
                    month=int(init_time.month),
                    day=int(init_time.day),
                )
            )

        if not rows:
            init_time = base_time
            rows.append(
                Row(
                    visualization_run_id=vis_run_id,
                    product_version=runtime["product_version"],
                    schema_version=runtime["schema_version"],
                    base_time=base_time,
                    init_time=init_time,
                    direction="backward",
                    traj_id="upstream_trajectory_missing",
                    traj_no=0,
                    cluster_id=0,
                    source_label="upstream_trajectory_missing",
                    source_lat=21.0285,
                    source_lon=105.8542,
                    source_alt_m=0.0,
                    start_lat=21.0285,
                    start_lon=105.8542,
                    end_lat=21.05,
                    end_lon=105.88,
                    age_start_h=0,
                    age_end_h=0,
                    point_count=2,
                    path_no2_mean=0.0,
                    path_aer_mean=0.0,
                    path_no2_aer_ratio=0.0,
                    geometry_geojson=line_geojson([{"lon": 105.8542, "lat": 21.0285, "alt_m": 0.0}, {"lon": 105.88, "lat": 21.05, "alt_m": 0.0}]),
                    properties_json=json.dumps({"available": False, "reason": "upstream_trajectory_missing"}, separators=(",", ":")),
                    style_color="#64748b",
                    generated_at=generated_at,
                    year=int(init_time.year),
                    month=int(init_time.month),
                    day=int(init_time.day),
                )
            )
        out = spark.createDataFrame(rows)
        count = write_product(out, tables["visualization_backward_trajectory_paths_gold"], dry_run)
        print(
            "job=visualization_backward_trajectory_paths "
            f"input_point_count={len(source)} trajectory_count={len(rows)} invalid_geometry_count={invalid} "
            f"dry_run={int(dry_run)} status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
