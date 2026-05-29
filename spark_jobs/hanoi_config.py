from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - Spark images may not have PyYAML yet.
    yaml = None


ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

# Base URI for storing model artifacts (mounted path in containers, or a remote URI in production).
# Used by training/promote scripts to build artifact URIs.
MODEL_ARTIFACT_BASE_URI = os.getenv("MODEL_ARTIFACT_BASE_URI", "/opt/models")

DEFAULT_CONFIG: dict[str, Any] = {
    "hanoi": {
        "bbox": {"west": 105.25, "east": 106.10, "south": 20.55, "north": 21.40},
        "center": {"lat": 21.0285, "lon": 105.8542},
    },
    "pm25_qc": {"min_value": 0.0, "max_value": 1000.0, "min_coverage_pct": 50.0},
    "era5": {
        "region": {"west": 95.0, "east": 115.0, "south": 5.0, "north": 35.0},
        "raw_base_path": "hdfs://namenode:9000/raw/era5",
        "surface_variables": [
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "boundary_layer_height",
            "surface_pressure",
            "2m_temperature",
            "2m_dewpoint_temperature",
            "total_precipitation",
            "mean_sea_level_pressure",
        ],
        "pressure_level_variables": [
            "u_component_of_wind",
            "v_component_of_wind",
            "vertical_velocity",
            "geopotential",
            "temperature",
            "specific_humidity",
        ],
        "pressure_levels": [1000, 925, 850, 700, 600, 500, 400],
        "pressure_levels_time_utc": ["00:00", "06:00", "12:00", "18:00"],
    },
    "sentinel5p": {
        "raw_base_path": "hdfs://namenode:9000/raw/sentinel5p",
        "products": ["NO2", "CO", "SO2", "O3", "AER_AI"],
    },
    "maiac": {
        "raw_base_path": "hdfs://namenode:9000/raw/maiac",
        "local_fallback_path": "crawler/maiac_data",
        "scale_factor": 0.001,
        "bands": ["AOD_047", "AOD_055"],
    },
    "gold": {
        "horizons_hours": [6, 12, 24],
        "lag_hours": [1, 3, 6, 12, 24],
        "rolling_hours": [3, 6, 24],
    },
    "hysplit": {
        "backward_hours": 72,
        "forward_hours": 24,
        "backward_altitudes_m": [100, 500, 1000],
        "forward_altitudes_m": [50, 200, 500],
        "init_offsets_deg": {
            "lat": [-0.2, 0.0, 0.2],
            "lon": [-0.2, 0.0, 0.2],
        },
        "run_hours_utc": [0, 6, 12, 18],
        "meteo_interval_hours": 6,
        "pm25_trigger_threshold": 75,
    },
    "trajectory": {
        "anchor_hours": [0, -6, -12, -24, -36, -48, -60, -72],
        "cluster_k_min": 3,
        "cluster_k_max": 10,
        "cluster_k_default": 6,
    },
    "sampling": {
        "path_window_start_h": -72,
        "path_window_end_h": -24,
        "max_distance_deg": 0.5,
    },
    "visualization": {
        "product_version": "windy_v1",
        "schema_version": "1",
        "region_bbox": {"west": 100.0, "east": 108.8, "south": 18.0, "north": 24.5},
        "region_center": {"lat": 21.0285, "lon": 105.8542},
        "default_zoom": 7,
        "grid_resolution_deg": 0.1,
        "horizons_hours": [0, 6, 12, 24],
        "observation_history_hours": 48,
        "freshness_max_minutes": 180,
        "forward_plume_required": False,
        "cache": {
            "base_uri": "hdfs://namenode:9000/visualization_cache",
            "format": "geojson",
        },
        "source_cluster_labels": {
            "0": "Unknown/weak trajectory signal",
            "1": "Northwest uplands transport corridor",
            "2": "Red River Delta urban-industrial corridor",
            "3": "Northeast coastal/Quang Ninh corridor",
            "4": "Southwest agricultural and biomass-burning corridor",
            "5": "Local Hanoi recirculation",
            "6": "Long-range regional transport",
        },
    },
}

TABLES = {
    "openaq_bronze": f"{ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze",
    "weather_bronze": f"{ICEBERG_CATALOG}.weather.weather_history_bronze",
    "sentinel5p_bronze": f"{ICEBERG_CATALOG}.satellite.sentinel5p_summary_bronze",
    "maiac_bronze": f"{ICEBERG_CATALOG}.satellite.maiac_summary_bronze",
    "era5_files_bronze": f"{ICEBERG_CATALOG}.weather.era5_files_bronze",
    "openaq_station_silver": f"{ICEBERG_CATALOG}.air_quality.openaq_hanoi_station_hourly_silver",
    "openaq_hourly_silver": f"{ICEBERG_CATALOG}.air_quality.openaq_hanoi_hourly_silver",
    "weather_proxy_silver": f"{ICEBERG_CATALOG}.weather.weather_hanoi_surface_proxy_silver",
    "era5_surface_silver": f"{ICEBERG_CATALOG}.weather.era5_surface_hanoi_hourly_silver",
    "sentinel5p_silver": f"{ICEBERG_CATALOG}.satellite.sentinel5p_hanoi_daily_silver",
    "maiac_silver": f"{ICEBERG_CATALOG}.satellite.maiac_hanoi_daily_silver",
    "master_gold": f"{ICEBERG_CATALOG}.features.hanoi_pm25_master_hourly_gold",
    "training_gold": f"{ICEBERG_CATALOG}.features.hanoi_pm25_training_dataset_gold",
    "serving_features_gold": f"{ICEBERG_CATALOG}.features.hanoi_pm25_serving_features_gold",
    "prediction_gold": f"{ICEBERG_CATALOG}.predictions.hanoi_pm25_forecast_gold",
    "model_runs_gold": f"{ICEBERG_CATALOG}.models.hanoi_pm25_model_runs_gold",
    "model_registry_gold": f"{ICEBERG_CATALOG}.models.hanoi_pm25_model_registry_gold",
    "era5_arl_bronze": f"{ICEBERG_CATALOG}.weather.era5_arl_files_bronze",
    "hysplit_runs_bronze": f"{ICEBERG_CATALOG}.trajectory.hysplit_runs_bronze",
    "hysplit_traj_silver": f"{ICEBERG_CATALOG}.trajectory.hysplit_trajectories_silver",
    "hysplit_cluster_silver": f"{ICEBERG_CATALOG}.trajectory.hysplit_trajectories_clustered_silver",
    "openaq_gradient_silver": f"{ICEBERG_CATALOG}.features.openaq_spatial_gradient_silver",
    "s5p_grid_silver": f"{ICEBERG_CATALOG}.satellite.sentinel5p_grid_silver",
    "trajectory_path_silver": f"{ICEBERG_CATALOG}.features.trajectory_path_satellite_silver",
    "trajectory_hourly_silver": f"{ICEBERG_CATALOG}.features.trajectory_hourly_features_silver",
    "visualization_heatmap_grid_gold": f"{ICEBERG_CATALOG}.visualization.pm25_heatmap_grid_gold",
    "visualization_backward_trajectory_paths_gold": f"{ICEBERG_CATALOG}.visualization.backward_trajectory_paths_gold",
    "visualization_forward_plume_probability_gold": f"{ICEBERG_CATALOG}.visualization.forward_plume_probability_gold",
    "visualization_forecast_dashboard_gold": f"{ICEBERG_CATALOG}.visualization.pm25_forecast_dashboard_gold",
    "visualization_pm25_timeseries_gold": f"{ICEBERG_CATALOG}.visualization.pm25_timeseries_gold",
    "visualization_source_attribution_gold": f"{ICEBERG_CATALOG}.visualization.source_attribution_gold",
    "visualization_station_observations_gold": f"{ICEBERG_CATALOG}.visualization.station_observations_gold",
    "visualization_cache_manifest_gold": f"{ICEBERG_CATALOG}.visualization.visualization_cache_manifest_gold",
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _candidate_config_paths() -> list[Path]:
    explicit = os.getenv("HANOI_PIPELINE_CONFIG", "").strip()
    paths = []
    if explicit:
        paths.append(Path(explicit))
    paths.extend(
        [
            Path("config/hanoi_pipeline.yaml"),
            Path("/opt/config/hanoi_pipeline.yaml"),
            Path("/opt/ais/config/hanoi_pipeline.yaml"),
            Path(__file__).resolve().parents[1] / "config" / "hanoi_pipeline.yaml",
        ]
    )
    return paths


def load_config() -> dict[str, Any]:
    cfg = DEFAULT_CONFIG
    if yaml is None:
        return _apply_env_overrides(cfg)

    for path in _candidate_config_paths():
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            if not isinstance(loaded, dict):
                raise ValueError(f"Invalid YAML root in {path}")
            cfg = _deep_merge(cfg, loaded)
            break
    return _apply_env_overrides(cfg)


def _apply_env_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = deepcopy(cfg)
    bbox = cfg["hanoi"]["bbox"]
    center = cfg["hanoi"]["center"]

    bbox["west"] = float(os.getenv("HANOI_BBOX_WEST", bbox["west"]))
    bbox["east"] = float(os.getenv("HANOI_BBOX_EAST", bbox["east"]))
    bbox["south"] = float(os.getenv("HANOI_BBOX_SOUTH", bbox["south"]))
    bbox["north"] = float(os.getenv("HANOI_BBOX_NORTH", bbox["north"]))
    center["lat"] = float(os.getenv("HANOI_CENTER_LAT", center["lat"]))
    center["lon"] = float(os.getenv("HANOI_CENTER_LON", center["lon"]))

    vis = cfg.setdefault("visualization", {})
    vis["product_version"] = _env_str("VIS_PRODUCT_VERSION", str(vis.get("product_version", "windy_v1")))
    vis["schema_version"] = _env_str("VIS_SCHEMA_VERSION", str(vis.get("schema_version", "1")))
    vis_bbox = vis.setdefault("region_bbox", {})
    vis_bbox["west"] = _env_float("VIS_REGION_BBOX_WEST", vis_bbox.get("west", 100.0))
    vis_bbox["east"] = _env_float("VIS_REGION_BBOX_EAST", vis_bbox.get("east", 108.8))
    vis_bbox["south"] = _env_float("VIS_REGION_BBOX_SOUTH", vis_bbox.get("south", 18.0))
    vis_bbox["north"] = _env_float("VIS_REGION_BBOX_NORTH", vis_bbox.get("north", 24.5))
    vis["grid_resolution_deg"] = _env_float("VIS_GRID_RESOLUTION_DEG", vis.get("grid_resolution_deg", 0.1))
    vis["observation_history_hours"] = _env_int("VIS_OBS_HISTORY_HOURS", vis.get("observation_history_hours", 48))
    vis["freshness_max_minutes"] = _env_int("VIS_FRESHNESS_MAX_MINUTES", vis.get("freshness_max_minutes", 180))
    vis["forward_plume_required"] = str(
        os.getenv("VIS_FORWARD_PLUME_REQUIRED", vis.get("forward_plume_required", False))
    ).lower() in {"1", "true", "yes", "y"}
    horizons = os.getenv("VIS_HORIZONS", "")
    if horizons:
        vis["horizons_hours"] = [int(v.strip()) for v in horizons.split(",") if v.strip()]
    cache = vis.setdefault("cache", {})
    cache["base_uri"] = _env_str("VIS_CACHE_BASE_URI", str(cache.get("base_uri", "hdfs://namenode:9000/visualization_cache")))
    cache["format"] = _env_str("VIS_CACHE_FORMAT", str(cache.get("format", "geojson")))
    return cfg


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


def _env_float(name: str, default: Any) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else float(default)


def _env_int(name: str, default: Any) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else int(default)


def get_hanoi_bbox() -> dict[str, float]:
    bbox = load_config()["hanoi"]["bbox"]
    return {k: float(v) for k, v in bbox.items()}


def get_hanoi_center() -> dict[str, float]:
    center = load_config()["hanoi"]["center"]
    return {k: float(v) for k, v in center.items()}


def get_pm25_qc() -> dict[str, float]:
    qc = load_config()["pm25_qc"]
    return {k: float(v) for k, v in qc.items()}


def get_era5_region() -> dict[str, float]:
    region = load_config()["era5"]["region"]
    return {k: float(v) for k, v in region.items()}


def get_era5_raw_base_path() -> str:
    return str(load_config()["era5"]["raw_base_path"]).rstrip("/")


def get_era5_surface_variables() -> list[str]:
    return [str(v) for v in load_config()["era5"]["surface_variables"]]


def get_sentinel5p_raw_base_path() -> str:
    return str(load_config()["sentinel5p"]["raw_base_path"]).rstrip("/")


def get_sentinel5p_products() -> list[str]:
    return [str(v) for v in load_config()["sentinel5p"]["products"]]


def get_maiac_raw_base_path() -> str:
    return str(load_config()["maiac"]["raw_base_path"]).rstrip("/")


def get_maiac_local_fallback_path() -> str:
    return str(load_config()["maiac"]["local_fallback_path"])


def get_maiac_scale_factor() -> float:
    return float(load_config()["maiac"]["scale_factor"])


def get_gold_horizons_hours() -> list[int]:
    return [int(v) for v in load_config()["gold"]["horizons_hours"]]


def get_gold_lag_hours() -> list[int]:
    return [int(v) for v in load_config()["gold"]["lag_hours"]]


def get_gold_rolling_hours() -> list[int]:
    return [int(v) for v in load_config()["gold"]["rolling_hours"]]


def get_hysplit_config() -> dict[str, Any]:
    return deepcopy(load_config().get("hysplit", {}))


def get_trajectory_config() -> dict[str, Any]:
    return deepcopy(load_config().get("trajectory", {}))


def get_sampling_config() -> dict[str, Any]:
    return deepcopy(load_config().get("sampling", {}))


def get_visualization_config() -> dict[str, Any]:
    return deepcopy(load_config().get("visualization", {}))


def get_visualization_region_bbox() -> dict[str, float]:
    bbox = get_visualization_config().get("region_bbox", {})
    return {
        "west": float(bbox.get("west", 100.0)),
        "east": float(bbox.get("east", 108.8)),
        "south": float(bbox.get("south", 18.0)),
        "north": float(bbox.get("north", 24.5)),
    }


def get_visualization_horizons() -> list[int]:
    return [int(v) for v in get_visualization_config().get("horizons_hours", [0, 6, 12, 24])]


def get_visualization_cache_base_uri() -> str:
    cfg = get_visualization_config()
    cache = cfg.get("cache", {})
    return _env_str("VIS_CACHE_BASE_URI", str(cache.get("base_uri", "hdfs://namenode:9000/visualization_cache"))).rstrip("/")


def get_visualization_cluster_labels() -> dict[int, str]:
    labels = get_visualization_config().get("source_cluster_labels", {})
    return {int(k): str(v) for k, v in labels.items()}


def get_era5_pressure_levels() -> list[int]:
    return [int(v) for v in load_config()["era5"].get("pressure_levels", [])]


def get_era5_pressure_level_variables() -> list[str]:
    return [str(v) for v in load_config()["era5"].get("pressure_level_variables", [])]


def get_era5_pressure_level_times() -> list[str]:
    return [str(v) for v in load_config()["era5"].get("pressure_levels_time_utc", [])]


def get_table_names() -> dict[str, str]:
    return TABLES.copy()


def filter_hanoi_bbox(df: DataFrame, lat_col: str, lon_col: str) -> DataFrame:
    bbox = get_hanoi_bbox()
    return df.filter(
        F.col(lat_col).between(bbox["south"], bbox["north"])
        & F.col(lon_col).between(bbox["west"], bbox["east"])
    )
