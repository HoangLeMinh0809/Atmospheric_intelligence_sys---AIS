from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_visualization_api_fallback_builds_multi_direction_proxy_trajectories():
    source = read("serving/visualization_api/main.py")

    assert "PROXY_TRAJECTORY_SECTORS" in source
    assert source.count('"proxy_ensemble"') >= 2
    assert '"direction_label": label' in source
    assert '"risk_score": risk' in source
    assert "path_pollution_intersection_score" in source


def test_map_canvas_keeps_latest_trajectory_ensemble_instead_of_single_endpoint_group():
    source = read("ui/src/components/map/MapCanvas.jsx")

    assert "MAX_TRAJECTORY_RENDER" in source
    assert "endpointGroups" not in source
    assert "slice(0, 12)" not in source
    assert "translateTrajectoryToReceptor(feature, receptor, index)" in source
    assert "trajectoryRiskColor(feature, index)" in source
    assert "trajectory-pulse" in source
    assert "withMinimumTrajectoryEnsemble" in source
    assert "client_proxy_ensemble" in source


def test_backward_trajectory_gold_falls_back_to_latest_historical_iceberg_rows():
    source = read("spark_jobs/visualization_backward_trajectory_paths_gold.py")

    assert "base_init_df" in source
    assert '"latest_historical_iceberg"' in source
    assert '"historical_fallback": selection_mode == "latest_historical_iceberg"' in source
    assert "VIS_MIN_TRAJECTORY_PATHS" in source
    assert "requested_window_threshold_relaxed" in source


def test_visualization_api_does_not_cut_cached_trajectories_to_latest_eight():
    source = read("serving/visualization_api/main.py")

    assert "latest_trajectory_features" not in source
    assert "selected[:8]" not in source
    assert "for index, feature in enumerate(features)" in source


def test_todo4_exports_visualization_cache_after_backward_trajectory_refresh():
    script = read("scripts/run_todo4_stack.ps1")

    core = "PIPELINE_LAYERS='heatmap,source_attribution,stations,forecast,timeseries' EXPORT_CACHE=false"
    backward = "PIPELINE_LAYERS='backward_trajectories' EXPORT_CACHE=false"
    export = 'Submit-SparkK8s "visualization-export-cache"'
    assert core in script
    assert backward in script
    assert export in script
    assert script.index(core) < script.index(backward) < script.index(export)


def test_todo4_defaults_historical_trajectory_fallback_and_raw_log():
    script = read("scripts/run_todo4_stack.ps1")

    assert "[bool]$UseHistoricalTrajectoryFallback = $true" in script
    assert "[string]$RawRunLogPath = \"logs/run_todo4_stack.raw.log\"" in script
    assert "Start-Transcript -Path $candidateRawRunLogPath -Append" in script
    assert "$rawRunLogFallbackPath" in script
    assert "Continuing without raw TODO4 transcript" in script
    assert "VIS_TRAJECTORY_HISTORICAL_FALLBACK='$trajectoryFallbackFlag'" in script


def test_todo4_log_parser_tracks_steps_and_spark_jobs():
    source = read("scripts/parse_run_todo4_log.py")

    assert "STEP_RE" in source
    assert "SPARK_SUBMIT_RE" in source
    assert "submit_spark_k8s\\.sh" in source
    assert "failed_spark_jobs" in source


def test_pm25_history_chart_uses_recent_48_points_and_threshold_bands():
    source = read("ui/src/components/charts/PM25ForecastChart.jsx")

    assert ".slice(-48)" in source
    assert "PM2.5 history" in source
    assert "locationName" in source
    assert "latest hourly points" in source
    assert "chart-band high" in source
    assert "chart-band moderate" in source


def test_air_map_uses_forecast_heatmap_horizons_and_removes_duplicate_bottom_timeline():
    source = read("ui/src/pages/AirQualityMapDashboard.jsx")

    assert "getHeatmapLatest" in source
    assert "horizonH === 0 ? getLiveHeatmapLatest(\"hanoi\") : getHeatmapLatest(horizonH)" in source
    assert "windy-bottom-timeline" not in source
    assert "Latest observed/nowcast" not in source


def test_air_map_forecast_panel_is_scoped_to_selected_receptor():
    source = read("ui/src/pages/AirQualityMapDashboard.jsx")

    assert "mergePointForecast" in source
    assert "nearestPm25At(heatmapsByHorizon?.[horizonKey], receptor)" in source
    assert "locationName={selectedReceptor.name}" in source
    assert "local_now_plus_model_delta" in source
    assert "timeseriesForReceptor" in source
    assert "SourceAttributionPanel" not in source


def test_heatmap_latest_requests_are_cache_busted_for_realtime_horizon_switching():
    source = read("ui/src/services/visualizationApi.js")

    assert "return requestJson(date ? path : withCacheBust(path));" in source
    assert "pm25/heatmap/latest?horizon_h=" in source


def test_forecast_heatmap_rendering_does_not_mix_realtime_station_samples():
    source = read("ui/src/components/map/MapCanvas.jsx")

    assert "function heatmapHorizonOf" in source
    assert "const interpolationStations = useMemo(() => (heatmapHorizon === 0 ? stations : [])" in source
    assert "buildPixelGrid(rawHeatmap, interpolationStations" in source


def test_station_markers_follow_selected_forecast_horizon():
    source = read("ui/src/components/map/MapCanvas.jsx")

    assert "function stationsForHorizon" in source
    assert 'marker_source: "forecast_heatmap_nearest_cell"' in source
    assert "const displayStations = useMemo(" in source
    assert "displayStations.map" in source


def test_heatmap_polygon_cells_use_planar_center_not_spherical_antipode():
    source = read("ui/src/components/map/MapCanvas.jsx")

    assert "function planarGeometryCenter" in source
    assert "planarGeometryCenter(geometry) || geoCentroid" in source
    assert "const usablePoints = uniquePoints.length ? uniquePoints : points" in source


def test_heatmap_forecast_grid_applies_model_delta_to_station_idw_cells():
    source = read("spark_jobs/visualization_pm25_heatmap_grid_gold.py")

    assert 'now_anchor = (pred or {}).get("pm25_now") or fallback_anchor' in source
    assert "forecast_delta = float(anchor) - float(now_anchor)" in source
    assert "if horizon > 0 and obs_count > 0:" in source
    assert "pm25 = max(0.0, float(pm25) + forecast_delta)" in source
