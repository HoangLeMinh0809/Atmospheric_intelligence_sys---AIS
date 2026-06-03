from __future__ import annotations

import argparse
import json

from pyspark.sql import Row
from pyspark.sql import functions as F

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


def _downsample(items: list[dict], max_points: int) -> list[dict]:
    if max_points <= 0 or len(items) <= max_points:
        return items
    if max_points == 1:
        return [items[-1]]
    step = (len(items) - 1) / float(max_points - 1)
    chosen = []
    seen = set()
    for idx in range(max_points):
        pos = int(round(idx * step))
        pos = min(max(pos, 0), len(items) - 1)
        if pos not in seen:
            chosen.append(items[pos])
            seen.add(pos)
    return chosen


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
    max_paths = max(1, int(runtime.get("max_trajectories", 150)))
    max_points = max(2, int(runtime.get("max_points_per_trajectory", 100)))

    try:
        traj = read_table_if_exists(spark, tables["hysplit_cluster_silver"])
        if traj is None:
            raise RuntimeError(f"Missing trajectory clustered table: {tables['hysplit_cluster_silver']}")

        points = (
            traj.filter(F.col("direction") == F.lit("backward"))
            .filter(F.col("traj_id").isNotNull())
            .filter(F.col("timestamp").isNotNull())
            .filter(F.col("age_h").isNotNull())
        )

        # Select trajectories by their init/source time (age_h=0), not by every point timestamp.
        # Backward points can be 72h earlier than init_time; filtering all points by date can
        # silently truncate or drop valid trajectory lines.
        init_df = (
            points.filter(F.col("age_h") == F.lit(0))
            .groupBy("traj_id")
            .agg(F.max("timestamp").alias("init_time"))
        )
        if asof_time is not None:
            init_df = init_df.filter(F.col("init_time") <= F.lit(asof_time.replace(tzinfo=None)))
        if args.start_date:
            init_df = init_df.filter(F.to_date("init_time") >= F.to_date(F.lit(args.start_date)))
        if args.end_date:
            init_df = init_df.filter(F.to_date("init_time") <= F.to_date(F.lit(args.end_date)))

        selected_ids = init_df.orderBy(F.col("init_time").desc()).limit(max_paths)
        selected_count = selected_ids.count()
        scoped = points.join(selected_ids, on="traj_id", how="inner")
        source = [r.asDict(recursive=True) for r in scoped.collect()]

        selected_id_df = selected_ids.select("traj_id")
        path_features = {}
        path_df = read_table_if_exists(spark, tables["trajectory_path_silver"])
        if path_df is not None and selected_count:
            path_rows = path_df.join(selected_id_df, on="traj_id", how="inner").collect()
            path_features = {str(r["traj_id"]): r.asDict(recursive=True) for r in path_rows}

        by_traj: dict[str, list[dict]] = {}
        for item in source:
            by_traj.setdefault(str(item["traj_id"]), []).append(item)

        if source:
            base_time = max(item.get("init_time") for item in source if item.get("init_time") is not None)
        else:
            base_time = (asof_time or generated_at).replace(tzinfo=None)
        vis_run_id = run_id("backward_trajectories", base_time, runtime["product_version"])
        rows = []
        invalid = 0
        exported_points = 0
        for traj_id, items in by_traj.items():
            ordered_full = sorted(items, key=lambda item: (item.get("age_h") is None, item.get("age_h") or 0))
            ordered = _downsample(ordered_full, max_points)
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
                "downsampled": len(ordered_full) > len(ordered),
                "original_point_count": len(ordered_full),
            }
            init_time = first.get("init_time") or first.get("timestamp") or base_time
            exported_points += len(ordered)
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
                    cluster_id=int(cluster_id) if cluster_id is not None else -1,
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
                    geometry_geojson=line_geojson([
                        {"lon": 105.8542, "lat": 21.0285, "alt_m": 0.0},
                        {"lon": 105.88, "lat": 21.05, "alt_m": 0.0},
                    ]),
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
            f"selected_trajectory_count={selected_count} input_point_count={len(source)} "
            f"trajectory_count={len(rows)} exported_point_count={exported_points} "
            f"max_trajectories={max_paths} max_points_per_trajectory={max_points} "
            f"invalid_geometry_count={invalid} dry_run={int(dry_run)} "
            f"status={'dry_run_success' if dry_run else 'written'} table_rows={count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
