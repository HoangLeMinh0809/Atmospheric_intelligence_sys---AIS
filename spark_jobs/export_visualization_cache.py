from __future__ import annotations

import argparse
import hashlib
import json
from datetime import timedelta

from pyspark.sql import Row

from visualization_common import (
    add_common_args,
    as_bool,
    build_spark,
    end_of_date,
    feature_collection,
    get_tables,
    hdfs_write_text,
    iso_z,
    payload_checksum,
    parse_base_time,
    read_table_if_exists,
    run_id,
    utc_now,
    visualization_runtime,
    write_product,
)


def parse_geometry(value: str | None) -> dict | None:
    if not value:
        return None
    return json.loads(value)


def geojson_payload(rows: list[dict], id_field: str | None = None) -> dict:
    features = []
    for row in rows:
        geometry = parse_geometry(row.get("geometry_geojson"))
        if geometry is None:
            continue
        props = {k: json_safe(v) for k, v in row.items() if k != "geometry_geojson"}
        feature = {"type": "Feature", "geometry": geometry, "properties": props}
        if id_field and row.get(id_field) is not None:
            feature["id"] = str(row[id_field])
        features.append(feature)
    return feature_collection(features)


def json_safe(value):
    if hasattr(value, "isoformat"):
        return iso_z(value)
    return value


def write_json(spark, uri: str, payload: dict, dry_run: bool = False) -> tuple[int, str]:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=json_safe)
    if not dry_run:
        hdfs_write_text(spark, uri, text)
    return len(text.encode("utf-8")), payload_checksum(text)



def empty_feature_payload(layer_name: str, reason: str, generated_at) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [],
        "available": False,
        "layer_name": layer_name,
        "reason": reason,
        "generated_at": iso_z(generated_at),
    }


def rows_available(layer_name: str, rows: list[dict]) -> tuple[bool, str]:
    if not rows:
        return False, f"{layer_name}_empty"
    if layer_name == "backward_trajectories" and all(str(row.get("traj_id")) == "upstream_trajectory_missing" for row in rows):
        return False, "upstream_trajectory_missing"
    return True, ""


def export_geojson_or_unavailable(
    spark,
    runtime: dict,
    layer_name: str,
    rows: list[dict],
    base_time,
    generated_at,
    uri: str,
    id_field: str,
    dry_run: bool,
    reason: str = "",
):
    available, inferred_reason = rows_available(layer_name, rows)
    reason = reason or inferred_reason
    if available:
        payload = geojson_payload(rows, id_field)
        payload.update({"available": True, "layer_name": layer_name, "base_time": iso_z(base_time), "generated_at": iso_z(generated_at)})
        fmt = "geojson"
        content_type = "application/geo+json"
        row_count = len(rows)
    else:
        payload = empty_feature_payload(layer_name, reason or f"{layer_name}_unavailable", generated_at)
        if base_time is not None:
            payload["base_time"] = iso_z(base_time)
        fmt = "geojson"
        content_type = "application/geo+json"
        row_count = 0
    size, checksum = write_json(spark, uri, payload, dry_run)
    manifest = manifest_row(
        runtime,
        layer_name,
        uri,
        size,
        checksum,
        row_count,
        base_time=base_time or generated_at.replace(tzinfo=None),
        fmt=fmt,
        content_type=content_type,
        available=available,
        unavailable_reason="" if available else (reason or f"{layer_name}_unavailable"),
        generated_at=generated_at,
    )
    summary = {
        "layer_name": layer_name,
        "cache_uri": uri,
        "available": available,
        "unavailable_reason": "" if available else (reason or f"{layer_name}_unavailable"),
        "row_count": row_count,
        "generated_at": iso_z(generated_at),
    }
    return manifest, summary

def latest_value(rows: list[dict], field: str):
    values = [row.get(field) for row in rows if row.get(field) is not None]
    return max(values) if values else None


def manifest_row(runtime, layer_name: str, uri: str, payload_bytes: int, checksum: str, row_count: int, *,
                 base_time=None, valid_time=None, horizon_h: int | None = None, location_id: str | None = None,
                 fmt: str = "geojson", content_type: str = "application/geo+json",
                 available: bool = True, unavailable_reason: str | None = None, generated_at=None):
    generated_at = generated_at or utc_now()
    bbox = runtime["bbox"]
    layer_key = f"{layer_name}:{horizon_h if horizon_h is not None else ''}:{location_id or ''}:{base_time}:{runtime['product_version']}"
    return Row(
        manifest_id=hashlib.sha1(layer_key.encode()).hexdigest()[:20],
        visualization_run_id=run_id("visualization_cache", base_time, runtime["product_version"]),
        product_version=runtime["product_version"],
        schema_version=runtime["schema_version"],
        layer_name=layer_name,
        base_time=base_time or generated_at.replace(tzinfo=None),
        valid_time=valid_time or base_time or generated_at.replace(tzinfo=None),
        horizon_h=int(horizon_h) if horizon_h is not None else -1,
        location_id=location_id or "",
        format=fmt,
        content_type=content_type,
        cache_uri=uri,
        tile_template="",
        bbox_west=float(bbox["west"]),
        bbox_south=float(bbox["south"]),
        bbox_east=float(bbox["east"]),
        bbox_north=float(bbox["north"]),
        row_count=int(row_count),
        byte_size=int(payload_bytes),
        checksum=checksum,
        available=bool(available),
        unavailable_reason=unavailable_reason or "",
        generated_at=generated_at,
        expires_at=generated_at + timedelta(minutes=int(runtime["freshness_max_minutes"])),
        year=int((base_time or generated_at).year),
        month=int((base_time or generated_at).month),
        day=int((base_time or generated_at).day),
    )


def collect_latest(df, time_col: str, filters=None, asof_time=None) -> tuple[list[dict], object]:
    if filters:
        for condition in filters:
            df = df.filter(condition)
    if asof_time is not None:
        df = df.filter(getattr(df, time_col) <= asof_time.replace(tzinfo=None))
    latest = df.agg({time_col: "max"}).first()[0]
    if latest is None:
        return [], None
    return [r.asDict(recursive=True) for r in df.filter(getattr(df, time_col) == latest).collect()], latest


def main() -> None:
    parser = argparse.ArgumentParser(description="Export visualization gold tables to API-ready cache files")
    add_common_args(parser)
    parser.add_argument("--location-id", default="hanoi")
    args = parser.parse_args()

    spark = build_spark("ExportVisualizationCache")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    runtime = visualization_runtime(args)
    dry_run = as_bool(args.dry_run)
    generated_at = utc_now()
    asof_time = parse_base_time(args.base_time) or end_of_date(args.end_date)
    date_key = asof_time.date().isoformat() if asof_time is not None else "latest"
    cache_scope = f"date={date_key}" if asof_time is not None else "latest"
    manifest = []
    manifest_summary = []

    try:
        heatmap = read_table_if_exists(spark, tables["visualization_heatmap_grid_gold"])
        if heatmap is not None:
            for horizon in [0, 6, 12, 24]:
                rows, base_time = collect_latest(heatmap, "base_time", filters=[heatmap.horizon_h == horizon], asof_time=asof_time)
                if rows:
                    uri = f"{runtime['cache_base_uri']}/pm25_heatmap/{cache_scope}/horizon={horizon}/grid.geojson"
                    payload = geojson_payload(rows, "cell_id")
                    payload.update({"layer_name": "pm25_heatmap", "horizon_h": horizon, "base_time": iso_z(base_time), "generated_at": iso_z(generated_at)})
                    size, checksum = write_json(spark, uri, payload, dry_run)
                    manifest.append(manifest_row(runtime, "pm25_heatmap", uri, size, checksum, len(rows), base_time=base_time, valid_time=latest_value(rows, "valid_time"), horizon_h=horizon, generated_at=generated_at))
                    manifest_summary.append({"layer_name": "pm25_heatmap", "horizon_h": horizon, "cache_uri": uri, "available": True, "row_count": len(rows), "generated_at": iso_z(generated_at)})

        plume = read_table_if_exists(spark, tables["visualization_forward_plume_probability_gold"])
        if plume is not None:
            for horizon in [6, 12, 24]:
                rows, base_time = collect_latest(plume, "base_time", filters=[plume.horizon_h == horizon], asof_time=asof_time)
                if rows:
                    available = any(bool(row.get("available")) for row in rows)
                    uri = f"{runtime['cache_base_uri']}/plume/forward/{cache_scope}/horizon={horizon}/grid.geojson"
                    if available:
                        payload = geojson_payload(rows, "cell_id")
                        content_type = "application/geo+json"
                        fmt = "geojson"
                    else:
                        payload = {
                            "available": False,
                            "layer_name": "forward_plume_probability",
                            "horizon_h": horizon,
                            "reason": rows[0].get("unavailable_reason") or "forward_hysplit_missing",
                            "generated_at": iso_z(generated_at),
                        }
                        content_type = "application/json"
                        fmt = "json"
                    size, checksum = write_json(spark, uri, payload, dry_run)
                    reason = "" if available else payload["reason"]
                    manifest.append(manifest_row(runtime, "forward_plume", uri, size, checksum, len(rows), base_time=base_time, valid_time=latest_value(rows, "valid_time"), horizon_h=horizon, fmt=fmt, content_type=content_type, available=available, unavailable_reason=reason, generated_at=generated_at))
                    manifest_summary.append({"layer_name": "forward_plume", "horizon_h": horizon, "cache_uri": uri, "available": available, "unavailable_reason": reason, "row_count": len(rows), "generated_at": iso_z(generated_at)})

        for layer_name, table_key, path, time_col, id_field in [
            ("backward_trajectories", "visualization_backward_trajectory_paths_gold", "trajectories/backward/latest.geojson", "base_time", "traj_id"),
            ("source_attribution", "visualization_source_attribution_gold", "source_attribution/latest.geojson", "base_time", "attribution_id"),
            ("station_observations", "visualization_station_observations_gold", "stations/latest.geojson", "observation_time", "observation_id"),
        ]:
            uri = f"{runtime['cache_base_uri']}/{path.replace('/latest', '/' + cache_scope)}"
            df = read_table_if_exists(spark, tables[table_key])
            if df is None:
                row, summary = export_geojson_or_unavailable(
                    spark, runtime, layer_name, [], asof_time or generated_at.replace(tzinfo=None), generated_at,
                    uri, id_field, dry_run, reason=f"table_missing:{tables[table_key]}"
                )
                manifest.append(row)
                manifest_summary.append(summary)
                continue
            rows, base_time = collect_latest(df, time_col, asof_time=asof_time)
            row, summary = export_geojson_or_unavailable(
                spark, runtime, layer_name, rows, base_time or asof_time or generated_at.replace(tzinfo=None), generated_at,
                uri, id_field, dry_run
            )
            manifest.append(row)
            manifest_summary.append(summary)

        dashboard = read_table_if_exists(spark, tables["visualization_forecast_dashboard_gold"])
        if dashboard is not None:
            rows, base_time = collect_latest(dashboard, "base_hour", filters=[dashboard.location_id == args.location_id], asof_time=asof_time)
            if rows:
                row = rows[0]
                payload = {
                    "location_id": row.get("location_id"),
                    "location_name": row.get("location_name"),
                    "base_hour": iso_z(row.get("base_hour")),
                    "generated_at": iso_z(row.get("generated_at")),
                    "freshness": {
                        "prediction_freshness_minutes": row.get("prediction_freshness_minutes"),
                        "observation_freshness_minutes": row.get("observation_freshness_minutes"),
                    },
                    "forecast": {
                        "now": {"pm25": row.get("pm25_now"), "risk": "unknown"},
                        "6h": {"pm25": row.get("pm25_6h"), "risk": row.get("risk_6h")},
                        "12h": {"pm25": row.get("pm25_12h"), "risk": row.get("risk_12h")},
                        "24h": {"pm25": row.get("pm25_24h"), "risk": row.get("risk_24h")},
                    },
                    "model": {
                        "model_version": row.get("model_version"),
                        "model_version_6h": row.get("model_version_6h"),
                        "model_version_12h": row.get("model_version_12h"),
                        "model_version_24h": row.get("model_version_24h"),
                        "feature_version": row.get("feature_version"),
                    },
                    "source_attribution": {
                        "dominant_cluster": row.get("dominant_cluster"),
                        "source_label": row.get("source_label"),
                        "source_lat": row.get("source_lat"),
                        "source_lon": row.get("source_lon"),
                    },
                }
                uri = f"{runtime['cache_base_uri']}/dashboard/{cache_scope}.json"
                size, checksum = write_json(spark, uri, payload, dry_run)
                manifest.append(manifest_row(runtime, "forecast_dashboard", uri, size, checksum, 1, base_time=base_time, location_id=args.location_id, fmt="json", content_type="application/json", generated_at=generated_at))
                manifest_summary.append({"layer_name": "forecast_dashboard", "location_id": args.location_id, "cache_uri": uri, "available": True, "row_count": 1, "generated_at": iso_z(generated_at)})

        timeseries = read_table_if_exists(spark, tables["visualization_pm25_timeseries_gold"])
        if timeseries is not None:
            rows, base_time = collect_latest(timeseries, "base_time", filters=[timeseries.location_id == args.location_id], asof_time=asof_time)
            if rows:
                payload = {"location_id": args.location_id, "base_time": iso_z(base_time), "generated_at": iso_z(generated_at), "points": [{k: json_safe(v) for k, v in row.items()} for row in rows]}
                uri = f"{runtime['cache_base_uri']}/timeseries/{args.location_id}/{cache_scope}.json"
                size, checksum = write_json(spark, uri, payload, dry_run)
                manifest.append(manifest_row(runtime, "pm25_timeseries", uri, size, checksum, len(rows), base_time=base_time, location_id=args.location_id, fmt="json", content_type="application/json", generated_at=generated_at))
                manifest_summary.append({"layer_name": "pm25_timeseries", "location_id": args.location_id, "cache_uri": uri, "available": True, "row_count": len(rows), "generated_at": iso_z(generated_at)})

        available_dates = sorted({iso_z(row.base_time)[:10] for row in manifest if row.base_time is not None}, reverse=True)
        manifest_payload = {
            "generated_at": iso_z(generated_at),
            "selected_date": date_key,
            "available_dates": available_dates,
            "product_version": runtime["product_version"],
            "schema_version": runtime["schema_version"],
            "layers": manifest_summary,
        }
        manifest_uri = f"{runtime['cache_base_uri']}/manifest/latest.json"
        write_json(spark, manifest_uri, manifest_payload, dry_run)
        if asof_time is not None:
            dated_manifest_uri = f"{runtime['cache_base_uri']}/manifest/date={date_key}.json"
            write_json(spark, dated_manifest_uri, manifest_payload, dry_run)

        if manifest:
            out = spark.createDataFrame(manifest)
            count = write_product(out, tables["visualization_cache_manifest_gold"], dry_run)
        else:
            count = 0
        print(
            "job=export_visualization_cache "
            f"manifest_entries={len(manifest)} manifest_uri={manifest_uri} table_rows={count} dry_run={int(dry_run)} "
            f"status={'dry_run_success' if dry_run else 'written'}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
