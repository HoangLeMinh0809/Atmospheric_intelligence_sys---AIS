from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hanoi_config import (
    HDFS_NAMENODE,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_table_names,
    get_visualization_cache_base_uri,
    get_visualization_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export visualization gold tables to API-ready GeoJSON cache")
    parser.add_argument("--dry-run", nargs="?", const="1", default=os.getenv("DRY_RUN", "0"))
    parser.add_argument("--cache-base-uri", default=os.getenv("VIS_CACHE_BASE_URI", ""))
    parser.add_argument("--manifest-table", default=os.getenv("VIS_CACHE_MANIFEST_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ExportVisualizationCache")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )


def ensure_manifest_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.visualization")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            manifest_id STRING,
            layer_name STRING,
            product_version STRING,
            schema_version STRING,
            base_time TIMESTAMP,
            horizon_h INT,
            available BOOLEAN,
            unavailable_reason STRING,
            cache_uri STRING,
            content_type STRING,
            record_count BIGINT,
            generated_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (layer_name, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def local_path_from_uri(cache_base_uri: str) -> Path:
    parsed = urlparse(cache_base_uri)
    if parsed.scheme in {"", "file"}:
        return Path(parsed.path if parsed.scheme == "file" else cache_base_uri)
    raise ValueError(f"Only local/file cache URIs are supported by this exporter: {cache_base_uri}")


def hdfs_path_from_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "hdfs":
        raise ValueError(f"Not an HDFS URI: {uri}")
    return parsed.path


def webhdfs_base() -> str:
    base = os.getenv("HDFS_WEBHDFS_BASE", "").rstrip("/")
    if not base:
        raise ValueError("HDFS_WEBHDFS_BASE is required for hdfs:// visualization cache export")
    return base


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def write_json_uri(uri: str, payload: dict) -> None:
    parsed = urlparse(uri)
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if parsed.scheme in {"", "file"}:
        write_json(Path(parsed.path if parsed.scheme == "file" else uri), payload)
        return
    if parsed.scheme == "hdfs":
        hdfs_path = hdfs_path_from_uri(uri)
        parent = str(Path(hdfs_path).parent).replace("\\", "/")
        mkdir_req = Request(f"{webhdfs_base()}{parent}?op=MKDIRS", method="PUT")
        with urlopen(mkdir_req, timeout=30):
            pass
        url = f"{webhdfs_base()}{hdfs_path}?op=CREATE&overwrite=true"
        req = Request(url, data=body, method="PUT", headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=30):
            return
    raise ValueError(f"Unsupported cache URI: {uri}")


def feature_collection(rows: list[dict]) -> dict:
    features = []
    for row in rows:
        geometry_raw = row.pop("geometry_geojson", None)
        geometry = json.loads(geometry_raw) if geometry_raw else None
        properties = dict(row)
        for key, value in list(properties.items()):
            if isinstance(value, datetime):
                properties[key] = value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})
    return {"type": "FeatureCollection", "features": features}


def latest_rows(df, time_col: str):
    latest = df.agg(F.max(time_col).alias("latest")).first()["latest"]
    if latest is None:
        return latest, df.limit(0)
    return latest, df.filter(F.col(time_col) == F.lit(latest))


def manifest_row(layer_name: str, cfg: dict, cache_uri: str, record_count: int, available: bool, reason: str | None, base_time, horizon_h=None):
    now = datetime.now(timezone.utc)
    manifest_id = f"{layer_name}_{horizon_h if horizon_h is not None else 'latest'}_{now.strftime('%Y%m%d%H%M%S')}"
    return {
        "manifest_id": manifest_id,
        "layer_name": layer_name,
        "product_version": str(cfg["product_version"]),
        "schema_version": str(cfg["schema_version"]),
        "base_time": base_time,
        "horizon_h": horizon_h,
        "available": bool(available),
        "unavailable_reason": reason,
        "cache_uri": cache_uri,
        "content_type": "application/geo+json",
        "record_count": int(record_count),
        "generated_at": now,
        "year": now.year,
        "month": now.month,
        "day": now.day,
    }


def merge_manifest(spark: SparkSession, table_name: str, rows: list[dict]) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows)
    df.createOrReplaceTempView("visualization_manifest_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING visualization_manifest_updates s
        ON t.layer_name = s.layer_name
           AND COALESCE(t.horizon_h, -1) = COALESCE(s.horizon_h, -1)
           AND t.product_version = s.product_version
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    cfg = get_visualization_config()
    tables = get_table_names()
    cache_base_uri = (args.cache_base_uri or get_visualization_cache_base_uri()).rstrip("/")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    manifest_table = args.manifest_table or tables["visualization_cache_manifest_gold"]
    ensure_manifest_table(spark, manifest_table)

    manifests = []

    station_time, station_df = latest_rows(spark.table(tables["visualization_station_observations_gold"]), "observation_time")
    station_rows = [r.asDict(recursive=True) for r in station_df.collect()]
    station_rel = "stations/latest.geojson"
    if not as_bool(args.dry_run):
        write_json_uri(f"{cache_base_uri}/{station_rel}", feature_collection(station_rows))
    manifests.append(manifest_row("station_observations", cfg, f"{cache_base_uri}/{station_rel}", len(station_rows), len(station_rows) > 0, None if station_rows else "station_observations_missing", station_time))

    traj_time, traj_df = latest_rows(spark.table(tables["visualization_backward_trajectory_paths_gold"]), "base_time")
    traj_rows = [r.asDict(recursive=True) for r in traj_df.collect()]
    traj_rel = "trajectories/backward/latest.geojson"
    if not as_bool(args.dry_run):
        write_json_uri(f"{cache_base_uri}/{traj_rel}", feature_collection(traj_rows))
    manifests.append(manifest_row("backward_trajectories", cfg, f"{cache_base_uri}/{traj_rel}", len(traj_rows), len(traj_rows) > 0, None if traj_rows else "backward_trajectories_missing", traj_time))

    plume = spark.table(tables["visualization_forward_plume_probability_gold"])
    for horizon_h in [6, 12, 24]:
        horizon_df = plume.filter(F.col("horizon_h") == F.lit(horizon_h))
        base_time, latest_df = latest_rows(horizon_df, "base_time")
        rows = [r.asDict(recursive=True) for r in latest_df.filter(F.col("available") == F.lit(True)).collect()]
        state = latest_df.select("available", "unavailable_reason").limit(1).collect()
        available = bool(state[0]["available"]) if state else False
        reason = state[0]["unavailable_reason"] if state else "forward_hysplit_missing"
        rel = f"plume/forward/latest/horizon={horizon_h}/grid.geojson"
        if not as_bool(args.dry_run):
            write_json_uri(f"{cache_base_uri}/{rel}", feature_collection(rows))
        manifests.append(manifest_row("forward_plume", cfg, f"{cache_base_uri}/{rel}", len(rows), available, None if available else reason, base_time, horizon_h))

    latest_manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "layers": manifests,
    }
    if not as_bool(args.dry_run):
        write_json_uri(f"{cache_base_uri}/manifest/latest.json", latest_manifest)
        merge_manifest(spark, manifest_table, manifests)
    for row in manifests:
        print(
            "visualization_cache "
            f"layer={row['layer_name']} horizon_h={row['horizon_h']} available={row['available']} "
            f"record_count={row['record_count']} cache_uri={row['cache_uri']}"
        )
    print("Dry run: skipped cache/manifest writes" if as_bool(args.dry_run) else f"Saved manifest: {manifest_table}")
    spark.stop()


if __name__ == "__main__":
    main()
