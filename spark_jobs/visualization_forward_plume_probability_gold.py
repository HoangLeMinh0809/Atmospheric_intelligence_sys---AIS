from __future__ import annotations

import argparse
import collections
from datetime import timedelta

from pyspark.sql import Row

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    get_tables,
    grid_cells,
    polygon_geojson,
    read_table_if_exists,
    run_id,
    utc_now,
    visualization_runtime,
    write_product,
)


def cell_for(lat: float, lon: float, bbox: dict[str, float], resolution: float) -> dict | None:
    if lat < bbox["south"] or lat > bbox["north"] or lon < bbox["west"] or lon > bbox["east"]:
        return None
    lat_min = bbox["south"] + int((lat - bbox["south"]) / resolution) * resolution
    lon_min = bbox["west"] + int((lon - bbox["west"]) / resolution) * resolution
    return {
        "cell_id": f"{lat_min:.3f}_{lon_min:.3f}",
        "lat": round(lat_min + resolution / 2, 6),
        "lon": round(lon_min + resolution / 2, 6),
        "lat_min": round(lat_min, 6),
        "lat_max": round(min(lat_min + resolution, bbox["north"]), 6),
        "lon_min": round(lon_min, 6),
        "lon_max": round(min(lon_min + resolution, bbox["east"]), 6),
    }


def _age_hours(point: dict, base_time) -> int | None:
    raw = point.get("age_h")
    if raw is not None:
        try:
            return int(round(abs(float(raw))))
        except (TypeError, ValueError):
            pass
    ts = point.get("timestamp")
    if ts is not None and base_time is not None:
        try:
            delta_h = abs((ts - base_time).total_seconds() / 3600.0)
            return int(round(delta_h))
        except Exception:
            return None
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Build optional forward plume probability grid")
    add_common_args(parser)
    args = parser.parse_args()

    spark = build_spark("VisualizationForwardPlumeProbabilityGold")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    generated_at = utc_now()
    dry_run = as_bool(args.dry_run)

    try:
        traj = read_table_if_exists(spark, tables["hysplit_traj_silver"])
        forward = []
        if traj is not None:
            forward = [r.asDict() for r in traj.filter(traj.direction == "forward").collect()]
        if not forward and runtime["forward_plume_required"]:
            raise RuntimeError("Forward HYSPLIT plume is required but no forward rows were found")

        base_time = max((r["timestamp"] for r in forward if r.get("timestamp") is not None), default=generated_at.replace(tzinfo=None))
        vis_run_id = run_id("forward_plume", base_time, runtime["product_version"])
        rows = []
        for horizon in [h for h in runtime["horizons"] if h in {6, 12, 24}]:
            valid_time = base_time + timedelta(hours=horizon)
            aged_points = []
            for r in forward:
                age = _age_hours(r, base_time)
                if age is not None:
                    aged_points.append((r, age))

            horizon_points = [r for r, age in aged_points if age == int(horizon)]
            if not horizon_points:
                # Accept small timing drift around requested horizons.
                horizon_points = [r for r, age in aged_points if abs(age - int(horizon)) <= 1]
            if not horizon_points and aged_points:
                # Fallback to nearest available age bucket if forward trajectories exist.
                min_diff = min(abs(age - int(horizon)) for _, age in aged_points)
                horizon_points = [r for r, age in aged_points if abs(age - int(horizon)) == min_diff]

            if not horizon_points:
                first = grid_cells(runtime["bbox"], runtime["grid_resolution_deg"])[0]
                rows.append(
                    Row(
                        visualization_run_id=vis_run_id,
                        product_version=runtime["product_version"],
                        schema_version=runtime["schema_version"],
                        base_time=base_time,
                        valid_time=valid_time,
                        horizon_h=int(horizon),
                        cell_id=f"unavailable_{horizon}",
                        lat=float(first["lat"]),
                        lon=float(first["lon"]),
                        lat_min=float(first["lat_min"]),
                        lat_max=float(first["lat_max"]),
                        lon_min=float(first["lon_min"]),
                        lon_max=float(first["lon_max"]),
                        particle_count=0,
                        total_particle_count=0,
                        probability=0.0,
                        available=False,
                        unavailable_reason="forward_hysplit_missing",
                        source_run_count=0,
                        source_method="forward_hysplit_optional",
                        geometry_geojson=polygon_geojson(float(first["lon_min"]), float(first["lat_min"]), float(first["lon_max"]), float(first["lat_max"])),
                        generated_at=generated_at,
                        year=int(valid_time.year),
                        month=int(valid_time.month),
                        day=int(valid_time.day),
                    )
                )
                continue
            counts = collections.Counter()
            cell_meta = {}
            for point in horizon_points:
                cell = cell_for(float(point["lat"]), float(point["lon"]), runtime["bbox"], runtime["grid_resolution_deg"])
                if cell is None:
                    continue
                counts[cell["cell_id"]] += 1
                cell_meta[cell["cell_id"]] = cell
            total = sum(counts.values())
            for cell_id, count in counts.items():
                cell = cell_meta[cell_id]
                rows.append(
                    Row(
                        visualization_run_id=vis_run_id,
                        product_version=runtime["product_version"],
                        schema_version=runtime["schema_version"],
                        base_time=base_time,
                        valid_time=valid_time,
                        horizon_h=int(horizon),
                        cell_id=cell_id,
                        lat=float(cell["lat"]),
                        lon=float(cell["lon"]),
                        lat_min=float(cell["lat_min"]),
                        lat_max=float(cell["lat_max"]),
                        lon_min=float(cell["lon_min"]),
                        lon_max=float(cell["lon_max"]),
                        particle_count=int(count),
                        total_particle_count=int(total),
                        probability=float(count / total) if total else 0.0,
                        available=True,
                        unavailable_reason="",
                        source_run_count=len({p.get("traj_id") for p in horizon_points}),
                        source_method="forward_hysplit_particle_grid",
                        geometry_geojson=polygon_geojson(float(cell["lon_min"]), float(cell["lat_min"]), float(cell["lon_max"]), float(cell["lat_max"])),
                        generated_at=generated_at,
                        year=int(valid_time.year),
                        month=int(valid_time.month),
                        day=int(valid_time.day),
                    )
                )

        out = spark.createDataFrame(rows)
        count = write_product(out, tables["visualization_forward_plume_probability_gold"], dry_run)
        print(
            "job=visualization_forward_plume_probability "
            f"forward_point_count={len(forward)} output_count={count} dry_run={int(dry_run)} "
            f"status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
