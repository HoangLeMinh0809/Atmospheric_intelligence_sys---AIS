from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from visualization_common import add_common_args, build_spark, get_tables, read_table_if_exists


def require_count(spark, table: str, label: str, minimum: int = 1) -> int:
    df = read_table_if_exists(spark, table)
    if df is None:
        raise RuntimeError(f"missing_table label={label} table={table}")
    count = df.count()
    if count < minimum:
        raise RuntimeError(f"empty_table label={label} table={table} count={count}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Quality checks for visualization product tables")
    add_common_args(parser)
    args = parser.parse_args()

    spark = build_spark("VisualizationQualityChecks")
    spark.sparkContext.setLogLevel("WARN")
    tables = get_tables()
    try:
        required = {
            "forecast_dashboard": tables["visualization_forecast_dashboard_gold"],
            "pm25_timeseries": tables["visualization_pm25_timeseries_gold"],
            "station_observations": tables["visualization_station_observations_gold"],
            "source_attribution": tables["visualization_source_attribution_gold"],
            "heatmap_grid": tables["visualization_heatmap_grid_gold"],
            "cache_manifest": tables["visualization_cache_manifest_gold"],
        }
        counts = {label: require_count(spark, table, label) for label, table in required.items()}

        heatmap = spark.read.table(tables["visualization_heatmap_grid_gold"])
        horizon_counts = {int(r["horizon_h"]): int(r["n"]) for r in heatmap.groupBy("horizon_h").agg(F.count("*").alias("n")).collect()}
        missing_horizons = [h for h in [0, 6, 12, 24] if horizon_counts.get(h, 0) == 0]
        if missing_horizons:
            raise RuntimeError(f"heatmap_missing_horizons missing={missing_horizons}")

        manifest = spark.read.table(tables["visualization_cache_manifest_gold"])
        required_layers = {"pm25_heatmap", "forecast_dashboard", "pm25_timeseries", "backward_trajectories", "source_attribution", "station_observations"}
        layers = {r["layer_name"] for r in manifest.filter(manifest.available == True).select("layer_name").distinct().collect()}
        missing_layers = sorted(required_layers - layers)
        if missing_layers:
            raise RuntimeError(f"manifest_missing_required_layers missing={missing_layers}")

        print(
            "job=visualization_quality_checks "
            f"counts={counts} horizon_counts={horizon_counts} status=ok dry_run={args.dry_run}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
